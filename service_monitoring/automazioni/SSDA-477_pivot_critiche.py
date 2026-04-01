import logging
import os
from datetime import datetime, timezone

import gspread
import numpy as np
import pandas as pd
from google.oauth2 import service_account
from pdnd_google_utils import Sheet
from pyspark import StorageLevel
from pyspark.sql import SparkSession

# ---------------- CONFIGURAZIONE BASE ----------------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"

# INPUT: tabella con requestid critici
INPUT_TABLE = "send_dev.weekly_postalizzazioni_critiche"

# OUTPUT: due spreadsheet distinti (uno per pivot)
SHEET_ID_OUTPUT_PIVOT_COMPLETA = "16skxJfvewNxfEEBkZFnucI2JE-L5ZtfpN50ikQlQVTM"
SHEET_ID_OUTPUT_PIVOT_SENZA_CHIUSE = "1cz_PSi1PWZq6ArBlSAuuQQd_C3ks4wRVw6ySwuwhGww"

# CONTROLLO: unico spreadsheet per meta + esito export
SHEET_ID_CONTROLLO = "1wYwt3kH92wxX-lWw0vUzCSwJJZwgJKgkhooXeCVFOP4"

# Nomi tab
TAB_PIVOT = "pivot"
TAB_META = "Data aggiornamento"
TAB_ESITO_EXPORT = "Esito export"

# Upload chunk
SHEETS_CHUNK_SIZE = 15000
SHEETS_CELL_LIMIT = 10_000_000
INITIAL_WS_ROWS = 2

GSHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]


# ---------------- ERRORI DEDICATI ----------------
class CapacityLimitError(RuntimeError):
    """Errore fatale: l'export non è rappresentabile in Google Sheets entro i limiti strutturali."""


# ---------------- FUNZIONI ----------------
def load_google_credentials(secret_path: str) -> dict:
    """Legge i file di credenziali e li restituisce come dizionario."""
    creds = {
        name: open(os.path.join(secret_path, name)).read().strip()
        for name in os.listdir(secret_path)
        if os.path.isfile(os.path.join(secret_path, name))
    }
    logging.info(f"Credenziali caricate: {list(creds.keys())}")
    return creds


def sanitize_for_sheets(df: pd.DataFrame, missing: str = "-") -> pd.DataFrame:
    """Normalizzazione difensiva per upload in Sheets."""
    if df is None:
        return pd.DataFrame()
    df = df.copy()
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df = df.fillna(missing)
    df = df.astype(object).where(pd.notna(df), missing)
    return df


def _resize_worksheet_grid_best_effort(
    creds: dict,
    sheet_id: str,
    worksheet_name: str,
    target_rows: int | None,
    target_cols: int | None,
):
    """Best-effort resize della griglia del worksheet (righe e/o colonne), con fail-fast sul limite celle."""
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

        would_allocate = final_rows * final_cols
        if would_allocate >= SHEETS_CELL_LIMIT:
            raise CapacityLimitError(
                f"Resize worksheet='{worksheet_name}' su spreadsheet={sheet_id} porterebbe la griglia a "
                f"{would_allocate} celle (rows={final_rows} * cols={final_cols}) >= {SHEETS_CELL_LIMIT}."
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
    """Upload su Google Sheet a chunk (header sempre in riga 1, clear solo sul primo chunk)."""
    logging.info(f"Scrittura su Google Sheet (chunked): {sheet_name}")

    if df is None:
        df = pd.DataFrame()

    df = df.astype(str)
    total_rows = len(df)
    logging.info(f"Totale righe da caricare: {total_rows} (chunk_size={chunk_size})")

    if expected_written_cols is not None:
        would_write = int(total_rows) * int(expected_written_cols)
        if would_write >= SHEETS_CELL_LIMIT:
            raise CapacityLimitError(
                f"Export '{sheet_name}' su sheet_id={sheet_id}: scrittura prevista {would_write} celle "
                f"(rows={total_rows} * cols={expected_written_cols}) >= {SHEETS_CELL_LIMIT}."
            )

        _resize_worksheet_grid_best_effort(
            creds=creds,
            sheet_id=sheet_id,
            worksheet_name=sheet_name,
            target_rows=INITIAL_WS_ROWS,
            target_cols=expected_written_cols,
        )

    sheet = Sheet(sheet_id=sheet_id, service_credentials=creds, id_mode="key")

    if total_rows == 0:
        logging.info(f"Nessuna riga da caricare per '{sheet_name}'. Pulizia foglio.")
        sheet.upload(
            worksheet_name=sheet_name,
            panda_df=df,
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
        ul_cell = "A1" if is_first else f"A{start + 2}"

        logging.info(
            f"Upload righe {start}-{end} su '{sheet_name}' @ {ul_cell} "
            f"(header={is_first}, clear={is_first})"
        )

        sheet.upload(
            worksheet_name=sheet_name,
            panda_df=chunk,
            ul_cell=ul_cell,
            header=is_first,
            clear_on_open=is_first,
        )

    logging.info("Scrittura completata (chunked).")


def _write_esito_controllo_safely(creds: dict, export_rows: list[dict]):
    """Best-effort: scrive l'esito export sullo sheet di controllo; non propaga eccezioni."""
    df_esito = sanitize_for_sheets(pd.DataFrame(export_rows), "-").astype(str)
    try:
        export_to_sheets_chunked(
            df=df_esito,
            creds=creds,
            sheet_id=SHEET_ID_CONTROLLO,
            sheet_name=TAB_ESITO_EXPORT,
            chunk_size=SHEETS_CHUNK_SIZE,
            expected_written_cols=len(df_esito.columns),
        )
    except Exception as e:
        logging.error(
            f"Errore scrittura tab controllo '{TAB_ESITO_EXPORT}': {e}", exc_info=True
        )


def get_last_update_date(spark: SparkSession) -> str:
    """Estrae la data più recente (MAX(requesttimestamp)) dalla gold."""
    logging.info(
        "Estrazione data ultimo aggiornamento (gold_postalizzazione_analytics)..."
    )
    max_date_df = spark.sql(
        "SELECT MAX(requesttimestamp) AS max_ts FROM send.gold_postalizzazione_analytics"
    )
    max_date = max_date_df.collect()[0]["max_ts"]
    if isinstance(max_date, datetime):
        return max_date.strftime("%Y-%m-%d %H:%M:%S")
    return str(max_date)


def build_aggregati_persisted(spark: SparkSession):
    """
    Calcola UNA SOLA VOLTA l'aggregazione per senderpaid e recapitista_unif, includendo:
    - cnt_tot: conteggio totale
    - cnt_incomplete: conteggio dei record con flag_incomplete=1 (GOLD)

    Da questo DF persistito deriviamo poi:
    - pivot completa (usa cnt_tot)
    - pivot senza chiuse (usa cnt_incomplete)
    """
    logging.info(
        "Costruzione aggregati persistiti (unica aggregazione per entrambe le pivot)..."
    )

    query_sql = f"""
    WITH base AS (
        SELECT
            g.senderpaid,
            CASE
                WHEN g.codice_oggetto LIKE 'R14%' THEN 'Fulmine'
                WHEN g.codice_oggetto LIKE '777%' OR g.codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
                WHEN g.codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
                WHEN g.codice_oggetto LIKE '69%'  OR g.codice_oggetto LIKE '381%' OR g.codice_oggetto LIKE 'RB1%' THEN 'Poste'
                ELSE g.recapitista_unificato
            END AS recapitista_unif,
            g.flag_wi7_report_postalizzazioni_incomplete AS flag_incomplete
        FROM send.gold_postalizzazione_analytics g
        INNER JOIN {INPUT_TABLE} i
            ON g.requestid = i.requestid
    )
    SELECT
        senderpaid,
        recapitista_unif,
        COUNT(*) AS cnt_tot,
        SUM(CASE WHEN flag_incomplete = 1 THEN 1 ELSE 0 END) AS cnt_incomplete
    FROM base
    GROUP BY senderpaid, recapitista_unif
    """

    df_aggr = spark.sql(query_sql)
    df_aggr = df_aggr.persist(StorageLevel.MEMORY_AND_DISK)

    logging.info("Materializzazione cache df_aggr (count)...")
    _ = df_aggr.count()
    logging.info("Cache df_aggr materializzata.")

    return df_aggr


def pivot_from_aggregati(df_aggr, use_incomplete: bool) -> pd.DataFrame:
    """
    Produce una pivot 'wide' (Fulmine/Poste/POST & SERVICE/RTI Sailpost-Snem + totale),
    partendo dagli aggregati persistiti.

    - use_incomplete=False -> usa cnt_tot
    - use_incomplete=True  -> usa cnt_incomplete
    """
    measure = "cnt_incomplete" if use_incomplete else "cnt_tot"
    label = "SENZA CHIUSE" if use_incomplete else "COMPLETA"
    logging.info(f"Costruzione pivot {label} da df_aggr (misura={measure})...")

    df_aggr.createOrReplaceTempView("temp_aggr")

    query_pivot = f"""
    WITH pivot_senderpaid AS (
        SELECT
            senderpaid,
            SUM(CASE WHEN recapitista_unif = 'Fulmine' THEN {measure} ELSE 0 END) AS conteggio_fulmine,
            SUM(CASE WHEN recapitista_unif = 'Poste' THEN {measure} ELSE 0 END) AS conteggio_poste,
            SUM(CASE WHEN recapitista_unif = 'POST & SERVICE' THEN {measure} ELSE 0 END) AS conteggio_po_se,
            SUM(CASE WHEN recapitista_unif = 'RTI Sailpost-Snem' THEN {measure} ELSE 0 END) AS conteggio_sailpost,
            SUM({measure}) AS totale
        FROM temp_aggr
        GROUP BY senderpaid
    ),
    totale AS (
        SELECT
            'TOTALE' AS senderpaid,
            SUM(conteggio_fulmine) AS conteggio_fulmine,
            SUM(conteggio_poste) AS conteggio_poste,
            SUM(conteggio_po_se) AS conteggio_po_se,
            SUM(conteggio_sailpost) AS conteggio_sailpost,
            SUM(totale) AS totale
        FROM pivot_senderpaid
    )
    SELECT *
    FROM (
        SELECT * FROM pivot_senderpaid
        UNION ALL
        SELECT * FROM totale
    ) t
    ORDER BY
        CASE WHEN t.senderpaid = 'TOTALE' THEN 1 ELSE 0 END,
        t.senderpaid ASC
    """

    df_out_spark = df_aggr.sparkSession.sql(query_pivot)
    df_out_pd = sanitize_for_sheets(df_out_spark.toPandas(), "-")
    return df_out_pd


# ---------------- MAIN SCRIPT ----------------
def main():
    logging.info("Inizializzazione SparkSession...")
    spark = SparkSession.builder.appName(
        "SSDA-477 Estrazione Criticità (aggr persist)"
    ).getOrCreate()

    creds = load_google_credentials(GOOGLE_SECRET_PATH)

    run_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    export_rows: list[dict] = []
    df_aggr = None

    try:
        # 1) Aggregazione persistita comune
        df_aggr = build_aggregati_persisted(spark)

        # 2) Pivot COMPLETA (da df_aggr)
        status = "OK"
        errore = ""
        righe = colonne = celle = 0
        try:
            df_pivot_completa = pivot_from_aggregati(
                df_aggr, use_incomplete=False
            ).astype(str)
            righe = int(len(df_pivot_completa))
            colonne = int(df_pivot_completa.shape[1])
            celle = int(righe * colonne)

            export_to_sheets_chunked(
                df=df_pivot_completa,
                creds=creds,
                sheet_id=SHEET_ID_OUTPUT_PIVOT_COMPLETA,
                sheet_name=TAB_PIVOT,
                chunk_size=SHEETS_CHUNK_SIZE,
                expected_written_cols=colonne,
            )
        except Exception as e:
            status = "KO"
            errore = str(e)
            logging.error(f"Errore pivot COMPLETA: {e}", exc_info=True)

        export_rows.append(
            {
                "run_time_utc": run_time_utc,
                "output_type": "PIVOT_COMPLETA",
                "sheet_id_target": SHEET_ID_OUTPUT_PIVOT_COMPLETA,
                "tab_target": TAB_PIVOT,
                "righe_scritte": righe,
                "colonne_scritte": colonne,
                "celle_scritte": celle,
                "status": status,
                "errore": errore,
            }
        )

        # 3) Pivot SENZA CHIUSE (da df_aggr, usando cnt_incomplete)
        status = "OK"
        errore = ""
        righe = colonne = celle = 0
        try:
            df_pivot_senza_chiuse = pivot_from_aggregati(
                df_aggr, use_incomplete=True
            ).astype(str)
            righe = int(len(df_pivot_senza_chiuse))
            colonne = int(df_pivot_senza_chiuse.shape[1])
            celle = int(righe * colonne)

            export_to_sheets_chunked(
                df=df_pivot_senza_chiuse,
                creds=creds,
                sheet_id=SHEET_ID_OUTPUT_PIVOT_SENZA_CHIUSE,
                sheet_name=TAB_PIVOT,
                chunk_size=SHEETS_CHUNK_SIZE,
                expected_written_cols=colonne,
            )
        except Exception as e:
            status = "KO"
            errore = str(e)
            logging.error(f"Errore pivot SENZA CHIUSE: {e}", exc_info=True)

        export_rows.append(
            {
                "run_time_utc": run_time_utc,
                "output_type": "PIVOT_SENZA_CHIUSE",
                "sheet_id_target": SHEET_ID_OUTPUT_PIVOT_SENZA_CHIUSE,
                "tab_target": TAB_PIVOT,
                "righe_scritte": righe,
                "colonne_scritte": colonne,
                "celle_scritte": celle,
                "status": status,
                "errore": errore,
            }
        )

        # 4) META: solo sul controllo
        max_date_str = get_last_update_date(spark)
        job_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        update_df = sanitize_for_sheets(
            pd.DataFrame(
                {
                    "Ultimo aggiornamento dati (MAX requesttimestamp)": [max_date_str],
                    "Data esecuzione script (UTC)": [job_time_utc],
                }
            ),
            "-",
        ).astype(str)

        export_to_sheets_chunked(
            df=update_df,
            creds=creds,
            sheet_id=SHEET_ID_CONTROLLO,
            sheet_name=TAB_META,
            chunk_size=SHEETS_CHUNK_SIZE,
            expected_written_cols=len(update_df.columns),
        )

        # 5) ESITO: sempre best-effort su controllo
        _write_esito_controllo_safely(creds=creds, export_rows=export_rows)

        if any(r.get("status") == "KO" for r in export_rows):
            raise RuntimeError(
                "Export incompleto: verificare il tab 'Esito export' sullo Sheet di controllo."
            )

        logging.info("Processo completato con successo.")

    except Exception as e:
        if not export_rows:
            export_rows.append(
                {
                    "run_time_utc": run_time_utc,
                    "output_type": "JOB",
                    "sheet_id_target": "",
                    "tab_target": "",
                    "righe_scritte": 0,
                    "colonne_scritte": 0,
                    "celle_scritte": 0,
                    "status": "KO",
                    "errore": str(e),
                }
            )
        _write_esito_controllo_safely(creds=creds, export_rows=export_rows)
        logging.error(
            "Esecuzione terminata con errore; dettagli nel log e nel tab di controllo, se disponibile.",
            exc_info=True,
        )
        raise

    finally:
        try:
            if df_aggr is not None:
                df_aggr.unpersist(blocking=False)
        except Exception:
            logging.warning("unpersist df_aggr fallito; continuo in chiusura.")
        try:
            spark.stop()
        except Exception:
            logging.warning("spark.stop() fallito; continuo in chiusura.")


if __name__ == "__main__":
    main()
