import logging
import os
import re
from datetime import datetime, timezone

import gspread
import numpy as np
import pandas as pd
from google.oauth2 import service_account
from pdnd_google_utils import Sheet
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------- CONFIGURAZIONE BASE ----------------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"

SHEET_ID_INPUT = "1jMw4w29sYJtB-itTI_uxwVCy6yMR6Gw2k4VkdrGh1lU"
SHEET_TAB_INPUT = "input"  # tab che contiene i senderpaid

# Sheet di controllo (log, meta, senderpaid usati, esiti upload)
SHEET_ID_CONTROLLO = "1KPOccUQm9k_-JLeWAX_GHmnMzMT4Ru7j-CpHX7EPJjk"

# Tab di controllo
TAB_META = "Data aggiornamento"
TAB_INPUT_EFFETTIVO = "Senderpaid usati"
TAB_ESITO_EXPORT = "Esito export"

# Sheet recapitisti
SHEET_ID_RECAPITISTI = {
    "Fulmine": "1KRGX0nDM-8kyac692n4Bf9FS4XfN7WfQQGo3GfbyV-4",
    "POST & SERVICE": "1BnBy9wJiql1BV4iUJlqySr43F2yAjeuyq4i97BH6zbc",
    "Poste": "13l2eNxY2_j-Rwa9nc_abILaLs8IiuDpxQWMpFHTxOY8",
    "RTI Sailpost-Snem": "1Bmy5gal5taPUogC2ZqdwM7nV-G_N5XIBC0qDE-YIS7s",
}

RECAPITISTI = ["Poste", "Fulmine", "POST & SERVICE", "RTI Sailpost-Snem"]

# Tab fissi di output per ciascun recapitista
TAB_RECAPITISTI = {
    "Fulmine": "wi7_Fulmine",
    "POST & SERVICE": "wi7_PoSe",
    "Poste": "wi7_Poste",
    "RTI Sailpost-Snem": "wi7_Sailpost",
}

# Tabella esistente su send_dev (2 colonne string): requestid, fornitore
TARGET_TABLE = "send_dev.weekly_postalizzazioni_critiche"

# Upload chunk
SHEETS_CHUNK_SIZE = 15000

# Limite celle Google Sheets (per spreadsheet)
SHEETS_CELL_LIMIT = 10_000_000
COLLAPSE_DELIMITER = ","

# Bonifica griglia: prima del primo upload (clear_on_open=True) riduciamo le righe allocate
# per evitare eredità di griglie enormi da run precedenti.
INITIAL_WS_ROWS = 2

# Pattern strutturale atteso: 8-4-4-4-12 esadecimale
SENDERPAID_PATTERN = re.compile(
    r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$"
)

# Senderpaid SEMPRE trattati come 'Critico' e rimossi dall'input settimanale.
SEMPRE_CRITICI_SENDERPAID = {"53b40136-65f2-424b-acfb-7fae17e35c60"}

# Scope (minimo ragionevole per Sheets; Drive non è necessario per il flusso)
GSHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]


# ---------------- ERRORI DEDICATI ----------------
class CapacityLimitError(RuntimeError):
    """Errore fatale: l'export non è rappresentabile in Google Sheets entro i limiti strutturali."""


# ---------------- FUNZIONI DI SUPPORTO TEMPI ----------------
def now_utc_dt() -> datetime:
    """Ritorna datetime UTC timezone-aware."""
    return datetime.now(timezone.utc)


def now_utc_str() -> str:
    """Ritorna timestamp UTC in formato stringa uniforme."""
    return now_utc_dt().strftime("%Y-%m-%d %H:%M:%S")


def elapsed_minutes_str(start_dt: datetime, end_dt: datetime | None = None) -> str:
    """
    Ritorna il tempo trascorso in minuti con due decimali.
    Esempio: 12.35
    """
    if end_dt is None:
        end_dt = now_utc_dt()
    elapsed_sec = (end_dt - start_dt).total_seconds()
    elapsed_min = elapsed_sec / 60
    return f"{elapsed_min:.2f}"


def build_meta_df(
    max_date_str: str,
    start_run_str: str,
    end_run_str: str = "-",
    elapsed_min_str: str = "IN CORSO",
) -> pd.DataFrame:
    """
    Costruisce il dataframe per il tab 'Data aggiornamento'.
    """
    return (
        pd.DataFrame(
            {
                "Ultimo aggiornamento dati (MAX requesttimestamp)": [max_date_str],
                "Start run time (UTC)": [start_run_str],
                "End run time (UTC)": [end_run_str],
                "Tempo impiegato (min)": [elapsed_min_str],
            }
        )
        .replace([np.inf, -np.inf], np.nan)
        .fillna("-")
        .astype(str)
    )


def write_tab_meta(
    creds: dict,
    max_date_str: str,
    start_run_str: str,
    end_run_str: str = "-",
    elapsed_min_str: str = "IN CORSO",
):
    """
    Scrive il tab di meta controllo.
    """
    update_df = build_meta_df(
        max_date_str=max_date_str,
        start_run_str=start_run_str,
        end_run_str=end_run_str,
        elapsed_min_str=elapsed_min_str,
    )

    export_to_sheets_chunked(
        update_df,
        creds,
        SHEET_ID_CONTROLLO,
        TAB_META,
        chunk_size=SHEETS_CHUNK_SIZE,
        expected_written_cols=len(update_df.columns),
    )


# ---------------- FUNZIONI GENERALI ----------------
def load_google_credentials(secret_path: str) -> dict:
    """Legge i file di credenziali e li restituisce come dizionario."""
    creds = {
        name: open(os.path.join(secret_path, name)).read().strip()
        for name in os.listdir(secret_path)
        if os.path.isfile(os.path.join(secret_path, name))
    }
    logging.info(f"Credenziali caricate: {list(creds.keys())}")
    return creds


def spark_to_df_per_gsheet(df_spark, missing: str = "-") -> pd.DataFrame:
    """
    Converte un DataFrame Spark in un DataFrame Pandas pronto per Google Sheets:
      - materializza sul driver (toPandas)
      - normalizza inf/-inf → NaN
      - sostituisce NaN con placeholder
      - converte tutto a stringa.
    """
    if df_spark is None:
        return pd.DataFrame()

    df = df_spark.toPandas()
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.fillna(missing)
    df = df.astype(str)
    return df


def _resize_worksheet_grid_best_effort(
    creds: dict,
    sheet_id: str,
    worksheet_name: str,
    target_rows: int | None,
    target_cols: int | None,
):
    """
    Best-effort resize della griglia del worksheet (righe e/o colonne).
    - Se target_rows/target_cols è None, mantiene il valore corrente.
    - Fail-fast solo se la griglia risultante (rows * cols) arriverebbe/supererebbe il limite.
    - Errori API/permessi: warning e si prosegue.
    """
    # Normalizzazione input
    if target_rows is not None and target_rows < 1:
        target_rows = 1
    if target_cols is not None and target_cols < 1:
        target_cols = 1

    try:
        credentials = service_account.Credentials.from_service_account_info(
            creds, scopes=GSHEETS_SCOPES
        )
        gc = gspread.authorize(credentials)
        sh = gc.open_by_key(sheet_id)
        ws = sh.worksheet(worksheet_name)

        current_rows = int(ws.row_count)
        current_cols = int(ws.col_count)

        final_rows = int(target_rows) if target_rows is not None else current_rows
        final_cols = int(target_cols) if target_cols is not None else current_cols

        # Fail-fast: griglia del worksheet non rappresentabile entro il limite
        would_allocate = final_rows * final_cols
        if would_allocate >= SHEETS_CELL_LIMIT:
            raise CapacityLimitError(
                f"Resize worksheet='{worksheet_name}' su spreadsheet={sheet_id} porterebbe la griglia a "
                f"{would_allocate} celle (rows={final_rows} * cols={final_cols}) >= {SHEETS_CELL_LIMIT}. "
                "Export non rappresentabile: ridurre righe/colonne o ridimensionare la griglia del tab prima di rieseguire."
            )

        if final_rows == current_rows and final_cols == current_cols:
            logging.info(
                f"Resize griglia worksheet '{worksheet_name}' non necessario: rows={current_rows}, cols={current_cols}"
            )
            return

        logging.info(
            f"Resize GRIGLIA worksheet '{worksheet_name}' su spreadsheet={sheet_id}: "
            f"rows {current_rows}->{final_rows}, cols {current_cols}->{final_cols}"
        )
        ws.resize(rows=final_rows, cols=final_cols)

    except CapacityLimitError:
        raise
    except Exception as e:
        logging.warning(
            f"Resize griglia fallito per worksheet='{worksheet_name}' (sheet_id={sheet_id}). "
            f"Continuo senza resize. Dettaglio: {e}"
        )


def export_to_sheets_chunked(
    df: pd.DataFrame,
    creds: dict,
    sheet_id: str,
    sheet_name: str,
    chunk_size: int = SHEETS_CHUNK_SIZE,
    expected_written_cols: int | None = None,
):
    """
    Upload su Google Sheet a chunk:
    - primo chunk: clear + header
    - chunk successivi: append tramite ul_cell
    Se un chunk fallisce il job fallisce.
    """
    logging.info(f"Scrittura su Google Sheet (chunked): {sheet_name}")

    if df is None:
        df = pd.DataFrame()

    total_rows = len(df)
    logging.info(f"Totale righe da caricare: {total_rows} (chunk_size={chunk_size})")

    # Fail-fast: stima minima della scrittura (righe del DF * colonne attese).
    if expected_written_cols is not None:
        would_write = int(total_rows) * int(expected_written_cols)
        if would_write >= SHEETS_CELL_LIMIT:
            raise CapacityLimitError(
                f"Export '{sheet_name}' su sheet_id={sheet_id}: scrittura prevista {would_write} celle "
                f"(rows={total_rows} * cols={expected_written_cols}) >= {SHEETS_CELL_LIMIT}. "
                "Termino prima della scrittura: ridurre perimetro o cambiare destinazione."
            )

        # Bonifica preventiva griglia: riduciamo le righe allocate a un valore minimo
        # e impostiamo le colonne necessarie, così non ereditiamo "row_count" enormi da run precedenti.
        _resize_worksheet_grid_best_effort(
            creds=creds,
            sheet_id=sheet_id,
            worksheet_name=sheet_name,
            target_rows=INITIAL_WS_ROWS,
            target_cols=expected_written_cols,
        )

    sheet = Sheet(sheet_id=sheet_id, service_credentials=creds, id_mode="key")

    # Caso DF vuoto: assicuriamo comunque la pulizia del foglio
    if total_rows == 0:
        logging.info(f"Nessuna riga da caricare per '{sheet_name}'. Pulizia foglio.")
        sheet.upload(
            worksheet_name=sheet_name,
            panda_df=df.astype(str),
            ul_cell="A1",
            header=True,
            clear_on_open=True,
        )
        logging.info("Scrittura completata (chunked, empty).")
        return

    for i, start in enumerate(range(0, total_rows, chunk_size)):
        end = min(start + chunk_size, total_rows)
        chunk = df.iloc[start:end]

        is_first = i == 0
        header = is_first
        clear_on_open = is_first

        ul_cell = "A1" if is_first else f"A{start + 2}"  # riga 1 header, dati da riga 2

        logging.info(
            f"Upload righe {start}-{end} su '{sheet_name}' @ {ul_cell} "
            f"(header={header}, clear={clear_on_open})"
        )

        sheet.upload(
            worksheet_name=sheet_name,
            panda_df=chunk.astype(str),
            ul_cell=ul_cell,
            header=header,
            clear_on_open=clear_on_open,
        )

    logging.info("Scrittura completata (chunked).")


def get_last_update_date(spark: SparkSession) -> str:
    """Estrae la data più recente (MAX(requesttimestamp)) dalla gold."""
    logging.info("Estrazione data ultimo aggiornamento...")
    max_date_df = spark.sql(
        "SELECT MAX(requesttimestamp) AS max_ts FROM send.gold_postalizzazione_analytics"
    )
    max_date = max_date_df.collect()[0]["max_ts"]
    if isinstance(max_date, datetime):
        return max_date.strftime("%Y-%m-%d %H:%M:%S")
    return str(max_date)


def _costruisci_df_base(spark: SparkSession):
    """
    Costruisce il DataFrame base WI7, portandosi dietro solo le colonne utili ai report.

    Persist + materializzazione per evitare ricalcoli tra:
      - scrittura tabella send_dev
      - 4 estrazioni recapitista
    """
    logging.info("Costruzione DataFrame base WI7...")

    query_sql = """
    WITH
    dati_gold_filtrati AS (
        SELECT
            g.senderpaid,
            g.iun,
            g.requestid,
            g.requesttimestamp,
            g.prodotto,
            g.geokey,
            g.region_name,
            g.province_name,
            g.recapitista_unificato,
            g.lotto,
            g.codice_oggetto,
            g.affido_consolidatore_data,
            g.stampa_imbustamento_CON080_data,
            g.affido_recapitista_CON016_data,
            g.accettazione_recapitista_CON018_data,
            g.affido_conservato_CON020_data,
            g.materialita_pronta_CON09A_data,
            g.scarto_consolidatore_stato,
            g.scarto_consolidatore_data,
            g.tentativo_recapito_stato,
            g.tentativo_recapito_data,
            g.tentativo_recapito_data_rendicontazione,
            g.messaingiacenza_recapito_stato,
            g.messaingiacenza_recapito_data,
            g.messaingiacenza_recapito_data_rendicontazione,
            g.certificazione_recapito_stato,
            g.certificazione_recapito_dettagli,
            g.certificazione_recapito_data,
            g.certificazione_recapito_data_rendicontazione,
            g.demat_23l_ar_stato,
            g.demat_23l_ar_data_rendicontazione,
            g.demat_plico_stato,
            g.demat_plico_data_rendicontazione,
            g.demat_arcad_stato,
            g.demat_arcad_data_rendicontazione,
            g.fine_recapito_stato,
            g.fine_recapito_data,
            g.fine_recapito_data_rendicontazione,
            g.accettazione_23L_RECAG012_data,
            g.accettazione_23L_RECAG012_data_rendicontazione,
            g.rend_23L_stato,
            g.rend_23L_data,
            g.rend_23L_data_rendicontazione,
            g.perfezionamento_data,
            g.perfezionamento_tipo,
            g.perfezionamento_notificationdate,
            g.perfezionamento_stato,
            g.perfezionamento_stato_dettagli,
            g.flag_wi7_report_postalizzazioni_incomplete,
            g.wi7_cluster,
            g.statusrequest,

            CASE
                WHEN g.codice_oggetto LIKE 'R14%' THEN 'Fulmine'
                WHEN g.codice_oggetto LIKE '777%' OR g.codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
                WHEN g.codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
                WHEN g.codice_oggetto LIKE '69%' OR g.codice_oggetto LIKE '381%' OR g.codice_oggetto LIKE 'RB1%' THEN 'Poste'
                ELSE g.recapitista_unificato
            END AS recapitista_unif,

            COALESCE(
                LEAST(g.accettazione_recapitista_con018_data, g.affido_recapitista_con016_data),
                g.accettazione_recapitista_con018_data,
                g.affido_recapitista_con016_data,
                g.requesttimestamp
            ) AS affido_accettazione_rec_data

        FROM send.gold_postalizzazione_analytics g
        WHERE
            g.flag_wi7_report_postalizzazioni_incomplete = 1
            AND g.statusrequest NOT IN ('PN999', 'PN998')
    ),

    finale AS (
        SELECT
            d.*,
            CAST(d.affido_accettazione_rec_data AS STRING) AS acc_str
        FROM dati_gold_filtrati d
    ),

    poste_da_escludere_dedup AS (
        SELECT DISTINCT
            requestid
        FROM send_dev.wi7_poste_da_escludere
    ),

    base_con_criticita AS (
        SELECT
            f.senderpaid,
            f.iun,
            f.requestid,
            f.requesttimestamp AS requestDateTime,
            f.prodotto,
            CONCAT("'", f.geokey) AS geokey,
            f.region_name AS regione,
            f.province_name AS provincia,
            f.recapitista_unif,
            f.lotto,
            f.codice_oggetto AS codiceOggetto,
            f.affido_consolidatore_data,
            f.stampa_imbustamento_CON080_data,
            f.affido_recapitista_CON016_data,
            f.accettazione_recapitista_CON018_data,
            SUBSTRING(f.acc_str, 1, 7) AS mese_anno_accettazione,
            SUBSTRING(f.acc_str, 1, 4) AS anno_accettazione,
            f.affido_conservato_CON020_data,
            f.materialita_pronta_CON09A_data,
            f.scarto_consolidatore_stato,
            f.scarto_consolidatore_data,
            f.tentativo_recapito_stato,
            f.tentativo_recapito_data,
            f.tentativo_recapito_data_rendicontazione,
            f.messaingiacenza_recapito_stato,
            f.messaingiacenza_recapito_data,
            f.messaingiacenza_recapito_data_rendicontazione,
            f.certificazione_recapito_stato,
            f.certificazione_recapito_dettagli,
            f.certificazione_recapito_data,
            f.certificazione_recapito_data_rendicontazione,
            f.demat_23l_ar_stato,
            f.demat_23l_ar_data_rendicontazione,
            f.demat_plico_stato,
            f.demat_plico_data_rendicontazione,
            f.demat_arcad_stato,
            f.demat_arcad_data_rendicontazione,
            f.fine_recapito_stato,
            f.fine_recapito_data,
            f.fine_recapito_data_rendicontazione,
            f.accettazione_23L_RECAG012_data,
            f.accettazione_23L_RECAG012_data_rendicontazione,
            f.rend_23L_stato,
            f.rend_23L_data,
            f.rend_23L_data_rendicontazione,
            f.perfezionamento_data,
            f.perfezionamento_tipo,
            f.perfezionamento_notificationdate,
            f.perfezionamento_stato,
            f.perfezionamento_stato_dettagli,
            f.flag_wi7_report_postalizzazioni_incomplete,
            f.wi7_cluster,

            CASE
                WHEN inp.senderpaid IS NOT NULL
                    AND f.perfezionamento_data IS NULL
                    AND e.requestid IS NULL
                THEN 'Critico'

                WHEN sem.senderpaid IS NOT NULL
                    AND e.requestid IS NULL
                THEN 'Critico'

                ELSE NULL
            END AS criticita,

            CASE
                WHEN sem.senderpaid IS NOT NULL THEN 'SI'
                ELSE 'NO'
            END AS ente_attenzionato

        FROM finale f

        LEFT JOIN input_senderpaid inp
            ON f.senderpaid = inp.senderpaid

        LEFT JOIN senderpaid_sempre_critici sem
            ON f.senderpaid = sem.senderpaid

        LEFT JOIN poste_da_escludere_dedup e
            ON f.requestid = e.requestid
    ),

    base_dedup AS (
        SELECT
            senderpaid,
            iun,
            requestid,
            requestDateTime,
            prodotto,
            geokey,
            regione,
            provincia,
            recapitista_unif,
            lotto,
            codiceOggetto,
            affido_consolidatore_data,
            stampa_imbustamento_CON080_data,
            affido_recapitista_CON016_data,
            accettazione_recapitista_CON018_data,
            mese_anno_accettazione,
            anno_accettazione,
            affido_conservato_CON020_data,
            materialita_pronta_CON09A_data,
            scarto_consolidatore_stato,
            scarto_consolidatore_data,
            tentativo_recapito_stato,
            tentativo_recapito_data,
            tentativo_recapito_data_rendicontazione,
            messaingiacenza_recapito_stato,
            messaingiacenza_recapito_data,
            messaingiacenza_recapito_data_rendicontazione,
            certificazione_recapito_stato,
            certificazione_recapito_dettagli,
            certificazione_recapito_data,
            certificazione_recapito_data_rendicontazione,
            demat_23l_ar_stato,
            demat_23l_ar_data_rendicontazione,
            demat_plico_stato,
            demat_plico_data_rendicontazione,
            demat_arcad_stato,
            demat_arcad_data_rendicontazione,
            fine_recapito_stato,
            fine_recapito_data,
            fine_recapito_data_rendicontazione,
            accettazione_23L_RECAG012_data,
            accettazione_23L_RECAG012_data_rendicontazione,
            rend_23L_stato,
            rend_23L_data,
            rend_23L_data_rendicontazione,
            perfezionamento_data,
            perfezionamento_tipo,
            perfezionamento_notificationdate,
            perfezionamento_stato,
            perfezionamento_stato_dettagli,
            flag_wi7_report_postalizzazioni_incomplete,
            wi7_cluster,
            criticita,
            ente_attenzionato
        FROM (
            SELECT
                b.*,
                ROW_NUMBER() OVER (
                    PARTITION BY b.recapitista_unif, b.requestid, b.requestDateTime
                    ORDER BY b.requestDateTime DESC
                ) AS rn
            FROM base_con_criticita b
        ) t
        WHERE t.rn = 1
    )

    SELECT
        senderpaid,
        iun,
        requestid,
        requestDateTime,
        prodotto,
        geokey,
        regione,
        provincia,
        recapitista_unif,
        lotto,
        codiceOggetto,
        affido_consolidatore_data,
        stampa_imbustamento_CON080_data,
        affido_recapitista_CON016_data,
        accettazione_recapitista_CON018_data,
        mese_anno_accettazione,
        anno_accettazione,
        affido_conservato_CON020_data,
        materialita_pronta_CON09A_data,
        scarto_consolidatore_stato,
        scarto_consolidatore_data,
        tentativo_recapito_stato,
        tentativo_recapito_data,
        tentativo_recapito_data_rendicontazione,
        messaingiacenza_recapito_stato,
        messaingiacenza_recapito_data,
        messaingiacenza_recapito_data_rendicontazione,
        certificazione_recapito_stato,
        certificazione_recapito_dettagli,
        certificazione_recapito_data,
        certificazione_recapito_data_rendicontazione,
        demat_23l_ar_stato,
        demat_23l_ar_data_rendicontazione,
        demat_plico_stato,
        demat_plico_data_rendicontazione,
        demat_arcad_stato,
        demat_arcad_data_rendicontazione,
        fine_recapito_stato,
        fine_recapito_data,
        fine_recapito_data_rendicontazione,
        accettazione_23L_RECAG012_data,
        accettazione_23L_RECAG012_data_rendicontazione,
        rend_23L_stato,
        rend_23L_data,
        rend_23L_data_rendicontazione,
        perfezionamento_data,
        perfezionamento_tipo,
        perfezionamento_notificationdate,
        perfezionamento_stato,
        perfezionamento_stato_dettagli,
        flag_wi7_report_postalizzazioni_incomplete,
        wi7_cluster,
        criticita,
        ente_attenzionato
    FROM base_dedup
    ;
    """

    df_base = spark.sql(query_sql)

    # Cache di tipo MEMORY_AND_DISK perché evita crash se non entra in RAM.
    df_base = df_base.persist(StorageLevel.MEMORY_AND_DISK)

    logging.info("Materializzazione cache df_wi7_base...")
    _ = df_base.count()
    logging.info("Cache materializzata.")

    df_base.createOrReplaceTempView("temp_wi7_base")
    return df_base


def _scrivi_critici_su_send_dev(df_base):
    """Scrive nella tabella Iceberg i soli requestid critici."""
    logging.info(f"Scrittura in tabella {TARGET_TABLE} con createOrReplace()...")

    df_critici = (
        df_base.where("criticita = 'Critico'")
        .selectExpr("requestid", "recapitista_unif AS fornitore")
        .dropDuplicates(["requestid", "fornitore"])
    )

    n = df_critici.count()
    logging.info(f"Righe critiche da scrivere in {TARGET_TABLE}: {n}")

    (
        df_critici.writeTo(TARGET_TABLE)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("engine.hive.enabled", "true")
        .createOrReplace()
    )

    logging.info("createOrReplace() completato.")


def _collassa_in_una_colonna(df_spark, delimiter: str, missing: str = "-"):
    """
    Collassa il DF in un'unica colonna, concatenando i valori riga per riga con un delimitatore.
    Normalizzazione difensiva: sostituisce \r e \n con spazio.
    """
    cols = []
    for c in df_spark.columns:
        col_str = F.coalesce(F.col(c).cast("string"), F.lit(missing))
        col_str = F.regexp_replace(col_str, r"[\r\n]+", " ")
        cols.append(col_str)

    return df_spark.select(F.concat_ws(delimiter, *cols).alias("dati"))


def _estrai_e_prepara_recapitista(df_base, recapitista: str):
    """
    Estrae il dataset WI7 per singolo recapitista a partire dal df_base cached, ed applica
    la logica di fallback se supera il limite di celle consentite da Google Sheets.
    """
    logging.info(f"Estrazione dataset recapitista: {recapitista}")

    df_spark = df_base.where(F.col("recapitista_unif") == F.lit(recapitista))

    if recapitista != "Fulmine" and "codiceOggetto" in df_spark.columns:
        df_spark = df_spark.withColumn(
            "codiceOggetto",
            F.when(F.col("codiceOggetto").isNull(), F.lit(None)).otherwise(
                F.concat(F.lit("'"), F.col("codiceOggetto").cast("string"))
            ),
        )

    logging.info(f"Calcolo righe per recapitista={recapitista} (count)...")
    n_rows = df_spark.count()
    n_cols = len(df_spark.columns)
    n_cells = n_rows * n_cols

    logging.info(
        f"Dimensioni recapitista={recapitista}: righe={n_rows}, colonne={n_cols}, celle={n_cells} "
        f"(limite={SHEETS_CELL_LIMIT})"
    )

    # Fail-fast: se anche collassando a 1 colonna non rientriamo (righe >= limite), l'export è impossibile.
    if n_rows >= SHEETS_CELL_LIMIT:
        raise CapacityLimitError(
            f"recapitista={recapitista}: righe={n_rows} >= {SHEETS_CELL_LIMIT}. "
            "Anche collassando a 1 colonna si raggiungerebbe/supererebbe il limite celle; export impossibile."
        )

    collapse_applied = False
    delimiter = ""
    collapse_reason = ""

    if n_cells >= SHEETS_CELL_LIMIT:
        collapse_applied = True
        delimiter = COLLAPSE_DELIMITER
        collapse_reason = "LIMITE_CELLE"
        logging.warning(
            f"recapitista={recapitista}: superato limite celle (celle={n_cells} >= {SHEETS_CELL_LIMIT}). "
            f"Applico collasso in 1 colonna con delimitatore='{delimiter}'."
        )

    if collapse_applied:
        header_str = delimiter.join(df_spark.columns)
        df_spark_out = _collassa_in_una_colonna(
            df_spark, delimiter=delimiter, missing="-"
        )
        out_cols = 1
        out_cells = n_rows * out_cols
    else:
        header_str = None
        df_spark_out = df_spark
        out_cols = n_cols
        out_cells = n_cells

    logging.info(f"toPandas recapitista={recapitista} (collapse={collapse_applied})...")
    df_pd = spark_to_df_per_gsheet(df_spark_out, missing="-")

    if collapse_applied:
        if df_pd.shape[1] != 1:
            raise RuntimeError(
                f"Collasso attivo ma dataframe pandas non è a 1 colonna (colonne={df_pd.shape[1]})."
            )
        df_pd.columns = [header_str]

    return df_pd, {
        "righe_output": int(n_rows),
        "colonne_output": int(out_cols),
        "celle_output": int(out_cells),
        "collapse_applied": "SI" if collapse_applied else "NO",
        "collapse_reason": collapse_reason if collapse_applied else "-",
        "delimiter": delimiter if collapse_applied else "-",
        "chunk_size_used": (
            int(SHEETS_CHUNK_SIZE * 2) if collapse_applied else int(SHEETS_CHUNK_SIZE)
        ),
        "expected_written_cols": int(out_cols),
    }


def run_query(spark: SparkSession):
    """Orchestrazione: costruisce df_base cached, scrive i critici in send_dev, ritorna df_base."""
    logging.info(
        "Esecuzione run_query: WI7 recapitisti + alimentazione tabella critici..."
    )
    df_base = _costruisci_df_base(spark)
    _scrivi_critici_su_send_dev(df_base)
    return df_base


def _write_esito_controllo_safely(creds: dict, export_rows: list[dict]):
    """
    Tenta sempre di scrivere TAB_ESITO_EXPORT sullo sheet di controllo; se fallisce, logga e basta.
    Questa funzione non deve mai propagare eccezioni, perché viene chiamata anche in percorsi di errore.
    """
    df_esito = (
        pd.DataFrame(export_rows)
        .replace([np.inf, -np.inf], np.nan)
        .fillna("-")
        .astype(str)
    )
    try:
        export_to_sheets_chunked(
            df_esito,
            creds,
            SHEET_ID_CONTROLLO,
            TAB_ESITO_EXPORT,
            chunk_size=SHEETS_CHUNK_SIZE,
            expected_written_cols=len(df_esito.columns),
        )
    except Exception as e:
        logging.error(
            f"Errore scrittura tab di controllo '{TAB_ESITO_EXPORT}': {e}",
            exc_info=True,
        )


# ---------------- MAIN SCRIPT ----------------
def main():
    logging.info("Inizializzazione SparkSession...")
    spark = SparkSession.builder.appName("SSDA-WI7_Recapitisti_Weekly").getOrCreate()

    creds = load_google_credentials(GOOGLE_SECRET_PATH)

    export_rows = []
    job_start_dt = now_utc_dt()
    job_start_str = job_start_dt.strftime("%Y-%m-%d %H:%M:%S")
    run_date = job_start_str
    df_base = None
    max_date_str = "-"

    try:
        # Lettura senderpaid da Sheet di INPUT
        logging.info("Lettura elenco senderpaid da Google Sheets...")
        sheet_in = Sheet(
            sheet_id=SHEET_ID_INPUT, service_credentials=creds, id_mode="key"
        )
        df_input = sheet_in.download(SHEET_TAB_INPUT)

        lista_senderpaid_completa = []
        sempre_critici_presenti = []
        lista_senderpaid_pulita = []

        if not df_input.empty:
            first_col = df_input.columns[0]
            raw_values = df_input[first_col].dropna().astype(str).tolist()

            cleaned_values = []
            valori_scartati = []

            for raw in raw_values:
                val = raw.strip().strip(",").strip("'").strip('"')
                if SENDERPAID_PATTERN.match(val):
                    cleaned_values.append(val)
                else:
                    valori_scartati.append(raw)

            seen = set()
            lista_senderpaid_completa = []
            for v in cleaned_values:
                if v not in seen:
                    seen.add(v)
                    lista_senderpaid_completa.append(v)

            if valori_scartati:
                logging.warning(
                    f"Trovati {len(valori_scartati)} senderpaid non conformi al pattern e scartati."
                )
                logging.warning(f"Esempi valori scartati: {valori_scartati[:5]}")

            sempre_critici_presenti = [
                x for x in lista_senderpaid_completa if x in SEMPRE_CRITICI_SENDERPAID
            ]
            lista_senderpaid_pulita = [
                x
                for x in lista_senderpaid_completa
                if x not in SEMPRE_CRITICI_SENDERPAID
            ]

            logging.info(
                f"{len(lista_senderpaid_completa)} senderpaid validi; "
                f"{len(sempre_critici_presenti)} senderpaid critici a priori."
            )

            spark.createDataFrame(
                [(x,) for x in lista_senderpaid_pulita], ["senderpaid"]
            ).createOrReplaceTempView("input_senderpaid")
        else:
            logging.warning(
                "Sheet senderpaid input vuoto: nessuna criticità standard verrà marcata."
            )
            spark.createDataFrame([], "senderpaid string").createOrReplaceTempView(
                "input_senderpaid"
            )

        spark.createDataFrame(
            [(x,) for x in sorted(sempre_critici_presenti)], ["senderpaid"]
        ).createOrReplaceTempView("senderpaid_sempre_critici")

        # Esecuzione query + scrittura tabella send_dev + estrazioni
        df_base = run_query(spark)

        # ------------- Export: controllo (meta + senderpaid) + 4 sheet recapitista -------------

        # 1) Tab "Data aggiornamento"
        max_date_str = get_last_update_date(spark)

        # prima scrittura: job in corso
        write_tab_meta(
            creds=creds,
            max_date_str=max_date_str,
            start_run_str=job_start_str,
            end_run_str="-",
            elapsed_min_str="IN CORSO",
        )

        # 2) Tab "Senderpaid usati"
        n = len(lista_senderpaid_completa)
        col_totali = lista_senderpaid_completa
        col_critici_a_priori = sempre_critici_presenti + [""] * (
            n - len(sempre_critici_presenti)
        )

        df_senderpaid_used = (
            pd.DataFrame(
                {
                    "senderpaid_totali_usati": col_totali,
                    "senderpaid_ente_attenzionato": col_critici_a_priori,
                }
            )
            .replace([np.inf, -np.inf], np.nan)
            .fillna("-")
            .astype(str)
        )

        export_to_sheets_chunked(
            df_senderpaid_used,
            creds,
            SHEET_ID_CONTROLLO,
            TAB_INPUT_EFFETTIVO,
            chunk_size=SHEETS_CHUNK_SIZE,
            expected_written_cols=len(df_senderpaid_used.columns),
        )

        # 3) Export recapitisti + raccolta esiti
        fatal_error = None

        for rec in RECAPITISTI:
            rec_start_dt = now_utc_dt()
            rec_start_str = rec_start_dt.strftime("%Y-%m-%d %H:%M:%S")
            rec_end_str = "-"
            rec_elapsed_min = "-"

            sheet_id_target = SHEET_ID_RECAPITISTI.get(rec)
            tab_target = TAB_RECAPITISTI.get(rec)

            status = "OK"
            errore = ""
            righe = 0
            colonne = 0
            celle = 0
            collapse_applied = "NO"
            collapse_reason = "-"
            delimiter = "-"
            chunk_size_used = SHEETS_CHUNK_SIZE
            expected_written_cols = None

            try:
                if sheet_id_target is None or tab_target is None:
                    raise ValueError(
                        f"Mapping mancante per recapitista='{rec}': sheet_id_target/tab_target non valorizzati"
                    )

                logging.info(
                    f"Preparazione export recapitista={rec} verso sheet_id={sheet_id_target}, tab={tab_target}..."
                )

                df_pd, meta = _estrai_e_prepara_recapitista(df_base, rec)

                righe = int(meta["righe_output"])
                colonne = int(meta["colonne_output"])
                celle = int(meta["celle_output"])
                collapse_applied = meta["collapse_applied"]
                collapse_reason = meta.get("collapse_reason", "-")
                delimiter = meta["delimiter"]
                chunk_size_used = int(meta["chunk_size_used"])
                expected_written_cols = int(meta["expected_written_cols"])

                logging.info(
                    f"Export recapitista={rec}: righe={righe}, colonne={colonne}, celle={celle}, "
                    f"collapse={collapse_applied}, reason={collapse_reason}, delimiter={delimiter}, chunk_size={chunk_size_used}"
                )

                export_to_sheets_chunked(
                    df=df_pd,
                    creds=creds,
                    sheet_id=sheet_id_target,
                    sheet_name=tab_target,
                    chunk_size=chunk_size_used,
                    expected_written_cols=expected_written_cols,
                )

                rec_end_dt = now_utc_dt()
                rec_end_str = rec_end_dt.strftime("%Y-%m-%d %H:%M:%S")
                rec_elapsed_min = elapsed_minutes_str(rec_start_dt, rec_end_dt)

            except CapacityLimitError as e:
                status = "KO"
                errore = str(e)
                fatal_error = e

                rec_end_dt = now_utc_dt()
                rec_end_str = rec_end_dt.strftime("%Y-%m-%d %H:%M:%S")
                rec_elapsed_min = elapsed_minutes_str(rec_start_dt, rec_end_dt)

                logging.error(
                    f"Errore fatale (capacity) export recapitista={rec}: {e}",
                    exc_info=True,
                )

            except Exception as e:
                status = "KO"
                errore = str(e)

                rec_end_dt = now_utc_dt()
                rec_end_str = rec_end_dt.strftime("%Y-%m-%d %H:%M:%S")
                rec_elapsed_min = elapsed_minutes_str(rec_start_dt, rec_end_dt)

                logging.error(f"Errore export recapitista={rec}: {e}", exc_info=True)

            export_rows.append(
                {
                    "status": status,
                    "errore": errore,
                    "run_time_utc": run_date,
                    "start_run_time_utc": rec_start_str,
                    "end_run_time_utc": rec_end_str,
                    "tempo_impiegato_min": rec_elapsed_min,
                    "recapitista": rec,
                    "righe_scritte": righe,
                    "colonne_scritte": colonne,
                    "celle_scritte": celle,
                    "collapse_applied": collapse_applied,
                    "collapse_reason": collapse_reason,
                    "delimiter": delimiter,
                    "chunk_size_used": chunk_size_used,
                    "sheet_id_target": sheet_id_target if sheet_id_target else "",
                    "tab_target": tab_target if tab_target else "",
                }
            )

            if fatal_error is not None:
                break

        # Scrittura tab di esito
        _write_esito_controllo_safely(creds=creds, export_rows=export_rows)

        # Aggiornamento finale tab meta
        job_end_dt = now_utc_dt()
        job_end_str = job_end_dt.strftime("%Y-%m-%d %H:%M:%S")
        job_elapsed_min = elapsed_minutes_str(job_start_dt, job_end_dt)

        write_tab_meta(
            creds=creds,
            max_date_str=max_date_str,
            start_run_str=job_start_str,
            end_run_str=job_end_str,
            elapsed_min_str=job_elapsed_min,
        )

        if fatal_error is not None:
            raise fatal_error

        if any(r.get("status") == "KO" for r in export_rows):
            raise RuntimeError(
                "Export recapitisti incompleto: verificare tab 'Esito export' sullo Sheet di controllo."
            )

        logging.info("Processo completato con successo.")

    except Exception as e:
        if not export_rows:
            job_error_end_dt = now_utc_dt()
            job_error_end_str = job_error_end_dt.strftime("%Y-%m-%d %H:%M:%S")
            job_error_elapsed_min = elapsed_minutes_str(job_start_dt, job_error_end_dt)

            export_rows.append(
                {
                    "status": "KO",
                    "errore": str(e),
                    "run_time_utc": run_date,
                    "start_run_time_utc": job_start_str,
                    "end_run_time_utc": job_error_end_str,
                    "tempo_impiegato_min": job_error_elapsed_min,
                    "recapitista": "JOB",
                    "righe_scritte": 0,
                    "colonne_scritte": 0,
                    "celle_scritte": 0,
                    "collapse_applied": "-",
                    "collapse_reason": "-",
                    "delimiter": "-",
                    "chunk_size_used": "-",
                    "sheet_id_target": "",
                    "tab_target": "",
                }
            )
            _write_esito_controllo_safely(creds=creds, export_rows=export_rows)

        try:
            job_error_end_dt = now_utc_dt()
            job_error_end_str = job_error_end_dt.strftime("%Y-%m-%d %H:%M:%S")
            job_error_elapsed_min = elapsed_minutes_str(job_start_dt, job_error_end_dt)

            write_tab_meta(
                creds=creds,
                max_date_str=max_date_str,
                start_run_str=job_start_str,
                end_run_str=job_error_end_str,
                elapsed_min_str=job_error_elapsed_min,
            )
        except Exception:
            logging.warning("Aggiornamento finale tab meta fallito in gestione errore.")

        logging.error(
            "Esecuzione terminata con errore; dettagli nel log e, se possibile, nel tab di controllo.",
            exc_info=True,
        )
        raise

    finally:
        try:
            if df_base is not None:
                df_base.unpersist(blocking=False)
        except Exception:
            logging.warning("unpersist df_base fallito; continuo in chiusura.")
        try:
            spark.stop()
        except Exception:
            logging.warning("spark.stop() fallito; continuo in chiusura.")


if __name__ == "__main__":
    main()
