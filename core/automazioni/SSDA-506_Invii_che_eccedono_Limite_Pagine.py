import logging
import os
from datetime import datetime, timezone

import gspread
import numpy as np
import pandas as pd
from google.oauth2 import service_account
from pdnd_google_utils import Sheet
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------- CONFIGURAZIONE BASE ----------------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"
SHEET_ID = "1Ts6LJ0P4kt-K7t5k6HF4YxhrzvYoWrsAKvOTRdCXD1o"

TAB_DA_ANNULLARE = "Weekly - da annullare"
TAB_ANNULLATE = "Weekly - annullate"
TAB_PIVOT = "Weekly - pivot"
TAB_LOG = "Log di controllo"

INITIAL_WS_ROWS = 2
GSHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEETS_CELL_LIMIT = 10_000_000

MIGRATION_REFERENCE_DATE = "2026-04-07 00:00:00"
START_DATE = "2026-04-14"

# Schema unico per estrazione, storico e tab Google.
BUSINESS_COLS = [
    "iun",
    "sentat",
    "senderpaid",
    "senderdenomination",
    "sendertaxid",
    "physicalCommunicationType",
    "subject",
    "taxonomycode",
    "requestid",
    "attempt_number",
    "cap",
    "prodotto",
    "requesttimestamp",
    "affido_consolidatore_data",
    "scarto_consolidatore_data",
    "statusrequest",
    "pages",
    "weight",
]
STOCK_COLS = BUSINESS_COLS + ["insert_timestamp_utc", "last_extraction"]
CANCELLED_COLS = BUSINESS_COLS + [
    "insert_timestamp_stock_utc",
    "insert_timestamp_annullate_utc",
    "last_extraction",
    "tms_cancelled",
]
MISSING_VALUES = {"", "-", "nan", "None", "NaT", "null", "NULL"}


# ---------------- ECCEZIONI ----------------
class CapacityLimitError(RuntimeError):
    """Errore fatale: l'export non è rappresentabile in Google Sheets entro i limiti strutturali."""


# ---------------- FUNZIONI BASE ----------------
def load_google_credentials(secret_path: str) -> dict:
    """Legge i file di credenziali e li restituisce come dizionario."""
    creds = {
        name: open(os.path.join(secret_path, name)).read().strip()
        for name in os.listdir(secret_path)
        if os.path.isfile(os.path.join(secret_path, name))
    }
    logging.info(f"Credenziali caricate: {list(creds.keys())}")
    return creds


def get_sheet_client(creds: dict) -> Sheet:
    return Sheet(sheet_id=SHEET_ID, service_credentials=creds, id_mode="key")


def validate_sheet_headers(creds: dict) -> None:
    """Blocca il job se i tab storici non hanno intestazioni valide nella prima riga."""
    credentials = service_account.Credentials.from_service_account_info(
        creds, scopes=GSHEETS_SCOPES
    )
    spreadsheet = gspread.authorize(credentials).open_by_key(SHEET_ID)

    # Sono richiesti solo i campi comuni anche ai report legacy.
    required = {
        "iun",
        "sentat",
        "senderpaid",
        "senderdenomination",
        "cap",
        "pages",
        "weight",
    }
    for tab_name in (TAB_DA_ANNULLARE, TAB_ANNULLATE):
        headers = [
            str(value).strip()
            for value in spreadsheet.worksheet(tab_name).row_values(1)
        ]
        missing = sorted(required - set(headers))
        if (
            not headers
            or headers[0] != "iun"
            or missing
            or len(headers) != len(set(headers))
        ):
            reason = []
            if not headers or headers[0] != "iun":
                reason.append("la cella A1 deve contenere 'iun'")
            if missing:
                reason.append(f"colonne obbligatorie mancanti: {missing}")
            if len(headers) != len(set(headers)):
                reason.append("intestazioni duplicate")
            message = (
                f"Intestazioni non valide nel tab '{tab_name}', riga 1: {'; '.join(reason)}. "
                "Ripristinare la prima riga prima di rieseguire il job. Nessun export eseguito."
            )
            logging.error(message)
            raise ValueError(message)
        logging.info(
            "Intestazioni verificate nel tab '%s': %d colonne", tab_name, len(headers)
        )


def safe_download_sheet(sheet: Sheet, sheet_name: str) -> pd.DataFrame:
    """Legge il tab; un errore di accesso interrompe il job per proteggere lo storico."""
    logging.info("Lettura sheet: %s", sheet_name)
    try:
        df = sheet.download(sheet_name)
    except Exception:
        logging.exception(
            "Lettura del tab '%s' fallita: interrompo il job.", sheet_name
        )
        raise
    if df is None:
        logging.warning("Tab '%s' privo di dati.", sheet_name)
        return pd.DataFrame()
    df = pd.DataFrame(df)
    if df.empty:
        logging.warning("Tab '%s' vuoto.", sheet_name)
    return df


def spark_to_df_per_gsheet(df_spark, missing: str = "-") -> pd.DataFrame:
    """
    Converte un DataFrame Spark in un DataFrame Pandas pronto per Google Sheets:
      - materializza sul driver (toPandas)
      - normalizza inf/-inf → NaN
      - sostituisce NaN con placeholder
      - converte tutto a stringa
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
    Best-effort resize della griglia del worksheet.
    - Se target_rows/target_cols è None, mantiene il valore corrente.
    - Fail-fast solo se la griglia risultante supera il limite celle.
    - Errori API/permessi: warning e si prosegue.
    """
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
                f"Resize worksheet='{worksheet_name}' su spreadsheet={sheet_id} "
                f"porterebbe la griglia a {would_allocate} celle "
                f"(rows={final_rows} * cols={final_cols}) >= {SHEETS_CELL_LIMIT}."
            )

        if final_rows == current_rows and final_cols == current_cols:
            logging.info(
                f"Resize griglia worksheet '{worksheet_name}' non necessario: "
                f"rows={current_rows}, cols={current_cols}"
            )
            return

        logging.info(
            f"Resize griglia worksheet '{worksheet_name}' su spreadsheet={sheet_id}: "
            f"rows {current_rows}->{final_rows}, cols {current_cols}->{final_cols}"
        )
        ws.resize(rows=final_rows, cols=final_cols)

    except CapacityLimitError:
        raise
    except Exception as e:
        logging.warning(
            f"Resize griglia fallito per worksheet='{worksheet_name}' "
            f"(sheet_id={sheet_id}). Continuo senza resize. Dettaglio: {e}"
        )


def ensure_columns(df: pd.DataFrame, required_columns: list[str]) -> pd.DataFrame:
    for col in required_columns:
        if col not in df.columns:
            df[col] = ""
    return df


def normalize_string_columns(df: pd.DataFrame, missing: str = "-") -> pd.DataFrame:
    if df.empty:
        return df
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.fillna(missing)
    for col in df.columns:
        df[col] = df[col].astype(str)
        df.loc[df[col].isin(["nan", "None", "NaT"]), col] = missing
    return df


def export_to_sheets(df: pd.DataFrame, creds: dict, sheet_name: str):
    """
    Scrive su Google Sheet con resize best-effort della griglia.
    """
    logging.info(f"Scrittura su Google Sheet: {sheet_name}")

    df = normalize_string_columns(df, missing="-")

    target_rows = max(len(df) + 1, INITIAL_WS_ROWS)  # +1 header
    target_cols = max(len(df.columns), 1)

    _resize_worksheet_grid_best_effort(
        creds=creds,
        sheet_id=SHEET_ID,
        worksheet_name=sheet_name,
        target_rows=target_rows,
        target_cols=target_cols,
    )

    sheet = get_sheet_client(creds)
    sheet.upload(sheet_name, df)
    logging.info(f"Scrittura completata su tab: {sheet_name}")


def get_last_update_date(spark: SparkSession) -> str:
    """Estrae la data più recente (MAX requesttimestamp) dal dataset."""
    logging.info("Estrazione data ultimo aggiornamento...")
    max_date_df = spark.sql(
        "SELECT MAX(requesttimestamp) AS max_ts FROM send.gold_postalizzazione_analytics"
    )
    max_date = max_date_df.collect()[0]["max_ts"]

    if isinstance(max_date, datetime):
        return max_date.strftime("%Y-%m-%d %H:%M:%S")
    return str(max_date)


# ---------------- FUNZIONI DI MIGRAZIONE LEGACY ----------------
def migrate_legacy_stock_df(previous_stock_df: pd.DataFrame) -> pd.DataFrame:
    """Allinea le colonne del vecchio stock preservando la data di primo ingresso."""
    if previous_stock_df.empty:
        return pd.DataFrame(columns=STOCK_COLS)

    df = ensure_columns(previous_stock_df.copy(), BUSINESS_COLS)
    if "insert_timestamp_utc" not in df.columns:
        logging.info("Stock legacy: uso data iniziale %s", MIGRATION_REFERENCE_DATE)
        df["insert_timestamp_utc"] = MIGRATION_REFERENCE_DATE
    else:
        df["insert_timestamp_utc"] = (
            df["insert_timestamp_utc"]
            .replace(list(MISSING_VALUES), np.nan)
            .fillna(MIGRATION_REFERENCE_DATE)
        )

    df["last_extraction"] = "0"
    df = ensure_columns(df, STOCK_COLS)
    return df[STOCK_COLS].drop_duplicates(subset=["iun"], keep="first").copy()


def migrate_legacy_cancelled_df(previous_cancelled_df: pd.DataFrame) -> pd.DataFrame:
    """Allinea lo storico annullate senza perdere righe e timestamp preesistenti."""
    if previous_cancelled_df.empty:
        return pd.DataFrame(columns=CANCELLED_COLS)
    df = ensure_columns(previous_cancelled_df.copy(), CANCELLED_COLS)
    df["last_extraction"] = "0"
    return df[CANCELLED_COLS].drop_duplicates(subset=["iun"], keep="first").copy()


# ---------------- QUERY ----------------
def run_current_stock_query(spark: SparkSession) -> pd.DataFrame:
    """Estrae le ultime spedizioni CON996 oltre la soglia del relativo prodotto."""
    logging.info("Estrazione stock: AR/RIR >100, RS/RIS >8, 890 >18; scarto CON996")
    query_sql = """
        WITH base AS (
            SELECT
                g.iun,
                g.requestid,
                g.attempt_number,
                g.geokey,
                g.prodotto,
                g.requesttimestamp,
                g.affido_consolidatore_data,
                g.scarto_consolidatore_data,
                g.statusrequest,
                g.numero_pagine,
                g.grammatura
            FROM send.gold_postalizzazione_analytics g
            JOIN send.gold_notification_analytics n ON n.iun = g.iun
            WHERE g.pcretry_rank = 1
              AND g.attempt_rank = 1
              AND g.scarto_consolidatore_stato = 'CON996'
              AND (
                    (g.prodotto IN ('AR', 'RIR') AND g.numero_pagine > 100)
                 OR (g.prodotto IN ('RS', 'RIS') AND g.numero_pagine > 8)
                 OR (g.prodotto = '890' AND g.numero_pagine > 18)
              )
              AND n.tms_viewed IS NULL
              AND n.tms_cancelled IS NULL
        )
        SELECT
            n.iun,
            n.sentat,
            n.senderpaid,
            n.senderdenomination,
            n.sendertaxid,
            n.physicalCommunicationType,
            n.subject,
            n.taxonomycode,
            g.requestid,
            g.attempt_number,
            concat("'", CAST(g.geokey AS STRING)) AS cap,
            g.prodotto,
            g.requesttimestamp,
            g.affido_consolidatore_data,
            g.scarto_consolidatore_data,
            g.statusrequest,
            g.numero_pagine AS pages,
            g.grammatura AS weight
        FROM base g
        JOIN send.silver_notification n ON n.iun = g.iun
    """
    df = spark_to_df_per_gsheet(spark.sql(query_sql), missing="-")
    df = ensure_columns(df, BUSINESS_COLS)[BUSINESS_COLS]
    if df["iun"].duplicated().any():
        raise ValueError(
            "Stock corrente con IUN duplicati: verificare join e ranking prima dell'export."
        )
    logging.info("Righe estratte per stock corrente: %s", len(df))
    return df


def run_cancelled_info_query(spark: SparkSession, iun_list: list[str]) -> pd.DataFrame:
    """Controlla l'annullamento effettivo degli IUN usciti, senza SQL IN dinamico."""
    if not iun_list:
        return pd.DataFrame(columns=["iun", "tms_cancelled"])

    iuns_df = spark.createDataFrame([(iun,) for iun in iun_list], ["iun"])
    status_df = spark.table("send.gold_notification_analytics").select(
        "iun", "tms_cancelled"
    )
    result_df = (
        iuns_df.join(status_df, on="iun", how="left")
        .groupBy("iun")
        .agg(F.max("tms_cancelled").alias("tms_cancelled"))
    )
    result = spark_to_df_per_gsheet(result_df, missing="-")
    logging.info("Controllati %s IUN usciti; risultati: %s", len(iun_list), len(result))
    return result


# ---------------- TRASFORMAZIONI ----------------
def build_current_stock(
    previous_stock_df: pd.DataFrame, current_extract_df: pd.DataFrame, job_time_utc: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Mantiene il timestamp originario degli IUN ancora presenti nello stock."""
    previous = ensure_columns(previous_stock_df.copy(), STOCK_COLS)
    current = ensure_columns(current_extract_df.copy(), BUSINESS_COLS)
    previous["iun"] = previous["iun"].astype(str)
    current["iun"] = current["iun"].astype(str)

    previous_iuns = set(previous["iun"])
    old_dates = previous.set_index("iun")["insert_timestamp_utc"].to_dict()
    current["insert_timestamp_utc"] = (
        current["iun"]
        .map(old_dates)
        .replace(list(MISSING_VALUES), np.nan)
        .fillna(job_time_utc)
    )
    current["last_extraction"] = current["iun"].apply(
        lambda iun: "0" if iun in previous_iuns else "1"
    )
    current = current[STOCK_COLS].drop_duplicates(subset=["iun"]).copy()
    new_rows_df = current[current["last_extraction"] == "1"].copy()
    return current, new_rows_df


def build_cancelled_rows(
    previous_stock_df: pd.DataFrame,
    current_stock_df: pd.DataFrame,
    previous_cancelled_df: pd.DataFrame,
    cancelled_info_df: pd.DataFrame,
    job_time_utc: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Sposta nelle annullate solo gli IUN usciti con tms_cancelled valorizzato."""
    previous = ensure_columns(previous_stock_df.copy(), STOCK_COLS)
    history = ensure_columns(previous_cancelled_df.copy(), CANCELLED_COLS)
    history["last_extraction"] = "0"

    current_iuns = set(current_stock_df["iun"].astype(str))
    already_cancelled = set(history["iun"].astype(str))
    disappeared = previous[
        ~previous["iun"].astype(str).isin(current_iuns | already_cancelled)
    ].copy()
    logging.info(
        "IUN usciti da verificare (non già nello storico): %s", len(disappeared)
    )

    if disappeared.empty:
        return pd.DataFrame(columns=CANCELLED_COLS), history[CANCELLED_COLS]

    info = ensure_columns(cancelled_info_df.copy(), ["iun", "tms_cancelled"])
    info["iun"] = info["iun"].astype(str)
    info = info.drop_duplicates(subset=["iun"])
    disappeared = disappeared.merge(info, on="iun", how="left", validate="one_to_one")

    # La sola scomparsa dallo stock non prova un annullamento.
    valid = disappeared["tms_cancelled"].notna() & ~disappeared["tms_cancelled"].astype(
        str
    ).str.strip().isin(MISSING_VALUES)
    excluded_count = int((~valid).sum())
    cancelled = disappeared.loc[valid].copy()
    logging.info("IUN effettivamente annullati: %s", len(cancelled))
    logging.info(
        "IUN usciti senza annullamento (esclusi dal perimetro): %s", excluded_count
    )

    if cancelled.empty:
        return pd.DataFrame(columns=CANCELLED_COLS), history[CANCELLED_COLS]

    cancelled["insert_timestamp_stock_utc"] = cancelled["insert_timestamp_utc"]
    cancelled["insert_timestamp_annullate_utc"] = job_time_utc
    cancelled["last_extraction"] = "1"
    cancelled = ensure_columns(cancelled, CANCELLED_COLS)[CANCELLED_COLS]
    history = pd.concat([history[CANCELLED_COLS], cancelled], ignore_index=True)
    history = history.drop_duplicates(subset=["iun"], keep="first")
    return cancelled, history[CANCELLED_COLS].copy()


def build_pivot_df(
    current_stock_df: pd.DataFrame, cancelled_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Costruisce il tab pivot:
    - numero oggetti che hanno presentato la casistica = stock corrente + annullate
    - numero oggetti annullati = solo annullate
    - numero oggetti ancora da annullare = solo stock corrente
    """
    base_cols = ["iun", "senderpaid", "senderdenomination", "insert_date"]

    current_part = pd.DataFrame(
        columns=base_cols + ["presented_case", "cancelled_case"]
    )
    cancelled_part = pd.DataFrame(
        columns=base_cols + ["presented_case", "cancelled_case"]
    )

    if not current_stock_df.empty:
        tmp = current_stock_df.copy()
        tmp["insert_date"] = pd.to_datetime(
            tmp["insert_timestamp_utc"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        tmp["presented_case"] = 1
        tmp["cancelled_case"] = 0
        current_part = tmp[
            [
                "iun",
                "senderpaid",
                "senderdenomination",
                "insert_date",
                "presented_case",
                "cancelled_case",
            ]
        ]

    if not cancelled_df.empty:
        tmp = cancelled_df.copy()
        tmp["insert_date"] = pd.to_datetime(
            tmp["insert_timestamp_stock_utc"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        tmp["presented_case"] = 1
        tmp["cancelled_case"] = 1
        cancelled_part = tmp[
            [
                "iun",
                "senderpaid",
                "senderdenomination",
                "insert_date",
                "presented_case",
                "cancelled_case",
            ]
        ]

    union_df = pd.concat([current_part, cancelled_part], ignore_index=True)

    if union_df.empty:
        return pd.DataFrame(
            columns=[
                "senderpaid",
                "senderdenomination",
                "insert_date",
                "numero_oggetti_con_casistica",
                "numero_oggetti_annullati",
                "numero_oggetti_ancora_da_annullare",
            ]
        )

    presented_df = (
        union_df.groupby(
            ["senderpaid", "senderdenomination", "insert_date"], dropna=False
        )["iun"]
        .nunique()
        .reset_index(name="numero_oggetti_con_casistica")
    )

    cancelled_only = (
        cancelled_part.groupby(
            ["senderpaid", "senderdenomination", "insert_date"], dropna=False
        )["iun"]
        .nunique()
        .reset_index(name="numero_oggetti_annullati")
        if not cancelled_part.empty
        else pd.DataFrame(
            columns=[
                "senderpaid",
                "senderdenomination",
                "insert_date",
                "numero_oggetti_annullati",
            ]
        )
    )

    current_only = (
        current_part.groupby(
            ["senderpaid", "senderdenomination", "insert_date"], dropna=False
        )["iun"]
        .nunique()
        .reset_index(name="numero_oggetti_ancora_da_annullare")
        if not current_part.empty
        else pd.DataFrame(
            columns=[
                "senderpaid",
                "senderdenomination",
                "insert_date",
                "numero_oggetti_ancora_da_annullare",
            ]
        )
    )

    pivot_df = presented_df.merge(
        cancelled_only,
        on=["senderpaid", "senderdenomination", "insert_date"],
        how="left",
    ).merge(
        current_only, on=["senderpaid", "senderdenomination", "insert_date"], how="left"
    )

    pivot_df["numero_oggetti_annullati"] = (
        pivot_df["numero_oggetti_annullati"].fillna(0).astype(int)
    )
    pivot_df["numero_oggetti_ancora_da_annullare"] = (
        pivot_df["numero_oggetti_ancora_da_annullare"].fillna(0).astype(int)
    )

    pivot_df = pivot_df.sort_values(
        by=["senderdenomination", "insert_date"],
        ascending=[True, False],
    ).copy()

    return pivot_df


def build_log_df(
    max_date_str: str,
    job_time_utc: str,
    current_stock_df: pd.DataFrame,
    new_rows_df: pd.DataFrame,
    disappeared_count: int,
    excluded_not_cancelled_count: int,
    new_cancelled_rows_df: pd.DataFrame,
    cancelled_history_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Costruisce il tab Log di controllo.
    Il cumulato storico delle annullate parte dalla data di primo lancio ufficiale del nuovo job.
    """
    cumulative_cancelled_count = 0

    if not cancelled_history_df.empty:
        tmp = cancelled_history_df.copy()
        tmp["insert_timestamp_annullate_utc_dt"] = pd.to_datetime(
            tmp["insert_timestamp_annullate_utc"], errors="coerce", utc=True
        )
        cumulative_cancelled_count = tmp[
            tmp["insert_timestamp_annullate_utc_dt"]
            >= pd.Timestamp(START_DATE, tz="UTC")
        ]["iun"].nunique()

    log_df = pd.DataFrame(
        {
            "Ultimo aggiornamento dati (MAX requesttimestamp)": [max_date_str],
            "Data esecuzione script (UTC)": [job_time_utc],
            f"Totale IUN tab '{TAB_DA_ANNULLARE}'": [len(current_stock_df)],
            f"IUN aggiunti nell'ultima estrazione nel tab '{TAB_DA_ANNULLARE}'": [
                len(new_rows_df)
            ],
            f"IUN usciti da '{TAB_DA_ANNULLARE}' rispetto alla settimana precedente": [
                disappeared_count
            ],
            "IUN usciti senza annullamento verificato": [excluded_not_cancelled_count],
            f"Nuovi IUN inseriti nel tab '{TAB_ANNULLATE}'": [
                len(new_cancelled_rows_df)
            ],
            f"Totale storico IUN nel tab '{TAB_ANNULLATE}' dal {START_DATE}": [
                cumulative_cancelled_count
            ],
        }
    )

    return log_df


# ---------------- MAIN SCRIPT ----------------
def main():
    logging.info("Inizializzazione SparkSession...")
    spark = SparkSession.builder.appName(
        "SSDA-506 Invii che Eccedono il Limite di Pagine - Enhanced"
    ).getOrCreate()

    try:
        job_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"Timestamp esecuzione job (UTC): {job_time_utc}")

        creds = load_google_credentials(GOOGLE_SECRET_PATH)
        sheet = get_sheet_client(creds)

        # Controllo preventivo: nessuna lettura/migrazione con intestazioni alterate.
        validate_sheet_headers(creds)

        previous_stock_df = safe_download_sheet(sheet, TAB_DA_ANNULLARE)
        previous_cancelled_df = safe_download_sheet(sheet, TAB_ANNULLATE)

        previous_stock_df = migrate_legacy_stock_df(previous_stock_df)
        previous_cancelled_df = migrate_legacy_cancelled_df(previous_cancelled_df)

        logging.info(
            f"IUN letti/migrati da tab precedente '{TAB_DA_ANNULLARE}': {len(previous_stock_df)}"
        )
        logging.info(
            f"IUN letti/migrati da tab precedente '{TAB_ANNULLATE}': {len(previous_cancelled_df)}"
        )

        current_extract_df = run_current_stock_query(spark)
        if current_extract_df.empty and not previous_stock_df.empty:
            raise RuntimeError(
                "Nuova estrazione vuota con stock precedente presente: blocco export per sicurezza."
            )

        current_stock_df, new_rows_df = build_current_stock(
            previous_stock_df=previous_stock_df,
            current_extract_df=current_extract_df,
            job_time_utc=job_time_utc,
        )

        previous_count = len(previous_stock_df) if not previous_stock_df.empty else 0
        current_iuns = (
            set(current_stock_df["iun"].astype(str))
            if not current_stock_df.empty
            else set()
        )
        previous_iuns = (
            set(previous_stock_df["iun"].astype(str))
            if (not previous_stock_df.empty and "iun" in previous_stock_df.columns)
            else set()
        )

        disappeared_iuns = sorted(list(previous_iuns - current_iuns))
        disappeared_count = len(disappeared_iuns)

        logging.info(f"Totale IUN stock precedente: {previous_count}")
        logging.info(f"Totale IUN stock corrente: {len(current_stock_df)}")
        logging.info(f"Nuovi IUN entrati nello stock: {len(new_rows_df)}")
        logging.info(f"IUN usciti dallo stock: {disappeared_count}")

        cancelled_info_df = run_cancelled_info_query(spark, disappeared_iuns)

        new_cancelled_rows_df, updated_cancelled_df = build_cancelled_rows(
            previous_stock_df=previous_stock_df,
            current_stock_df=current_stock_df,
            previous_cancelled_df=previous_cancelled_df,
            cancelled_info_df=cancelled_info_df,
            job_time_utc=job_time_utc,
        )

        # Non contiamo come esclusi gli IUN già presenti nello storico annullate.
        already_cancelled_iuns = set(previous_cancelled_df["iun"].astype(str))
        new_cancelled_iuns = set(new_cancelled_rows_df["iun"].astype(str))
        excluded_not_cancelled_count = len(
            set(disappeared_iuns) - already_cancelled_iuns - new_cancelled_iuns
        )
        logging.info(
            "IUN usciti senza nuovo annullamento verificato: %s",
            excluded_not_cancelled_count,
        )
        logging.info(
            f"Nuovi IUN inseriti in '{TAB_ANNULLATE}': {len(new_cancelled_rows_df)}"
        )
        logging.info(
            f"Totale storico IUN in '{TAB_ANNULLATE}': {len(updated_cancelled_df)}"
        )

        pivot_df = build_pivot_df(
            current_stock_df=current_stock_df, cancelled_df=updated_cancelled_df
        )
        logging.info(f"Righe pivot generate: {len(pivot_df)}")

        max_date_str = get_last_update_date(spark)
        logging.info(f"Ultimo aggiornamento dati gold_postalizzazione: {max_date_str}")

        log_df = build_log_df(
            max_date_str=max_date_str,
            job_time_utc=job_time_utc,
            current_stock_df=current_stock_df,
            new_rows_df=new_rows_df,
            disappeared_count=disappeared_count,
            excluded_not_cancelled_count=excluded_not_cancelled_count,
            new_cancelled_rows_df=new_cancelled_rows_df,
            cancelled_history_df=updated_cancelled_df,
        )

        export_to_sheets(current_stock_df, creds, TAB_DA_ANNULLARE)
        export_to_sheets(updated_cancelled_df, creds, TAB_ANNULLATE)
        export_to_sheets(pivot_df, creds, TAB_PIVOT)
        export_to_sheets(log_df, creds, TAB_LOG)

        logging.info("Processo completato con successo.")

    finally:
        spark.stop()
        logging.info("Spark session terminata.")


if __name__ == "__main__":
    main()
