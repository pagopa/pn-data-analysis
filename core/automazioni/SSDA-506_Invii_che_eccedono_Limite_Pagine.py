import logging
import os
from datetime import datetime, timezone

import gspread
import numpy as np
import pandas as pd
from google.oauth2 import service_account
from pdnd_google_utils import Sheet
from pyspark.sql import SparkSession

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


def safe_download_sheet(sheet: Sheet, sheet_name: str) -> pd.DataFrame:
    """
    Legge uno sheet e restituisce un DataFrame Pandas.
    Se il tab non esiste ancora o è vuoto, restituisce un DataFrame vuoto.
    """
    try:
        logging.info(f"Lettura sheet: {sheet_name}")
        df = sheet.download(sheet_name)
        if df is None:
            logging.warning(
                f"Il tab {sheet_name} non contiene dati. Restituisco DataFrame vuoto."
            )
            return pd.DataFrame()
        df = pd.DataFrame(df)
        if df.empty:
            logging.warning(f"Il tab {sheet_name} è vuoto.")
        return df
    except Exception as e:
        logging.warning(
            f"Impossibile leggere il tab {sheet_name}: {e}. Restituisco DataFrame vuoto."
        )
        return pd.DataFrame()


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
    """
    Retrocompatibilità per il tab legacy 'Weekly - da annullare'.

    Se il foglio esiste già ma non contiene le nuove colonne tecniche:
    - aggiunge insert_timestamp_utc valorizzandolo con MIGRATION_REFERENCE_DATE
    - aggiunge last_extraction = 0
    """
    if previous_stock_df.empty:
        return previous_stock_df

    previous_stock_df = previous_stock_df.copy()

    required_business_cols = [
        "iun",
        "sentat",
        "senderpaid",
        "senderdenomination",
        "physicalCommunicationType",
        "subject",
        "taxonomycode",
        "cap",
        "pages",
        "weight",
    ]
    previous_stock_df = ensure_columns(previous_stock_df, required_business_cols)

    if "insert_timestamp_utc" not in previous_stock_df.columns:
        logging.info(
            "Tab legacy 'da annullare' rilevato senza colonna 'insert_timestamp_utc'. "
            f"Valorizzo tutte le righe pregresse con {MIGRATION_REFERENCE_DATE}."
        )
        previous_stock_df["insert_timestamp_utc"] = MIGRATION_REFERENCE_DATE
    else:
        previous_stock_df["insert_timestamp_utc"] = previous_stock_df[
            "insert_timestamp_utc"
        ].replace(["", "-", "nan", "None", "NaT"], np.nan)
        previous_stock_df["insert_timestamp_utc"] = previous_stock_df[
            "insert_timestamp_utc"
        ].fillna(MIGRATION_REFERENCE_DATE)

    if "last_extraction" not in previous_stock_df.columns:
        logging.info(
            "Tab legacy 'da annullare' rilevato senza colonna 'last_extraction'. "
            "Imposto 0 su tutte le righe pregresse."
        )
        previous_stock_df["last_extraction"] = "0"
    else:
        previous_stock_df["last_extraction"] = "0"

    desired_order = [
        "iun",
        "sentat",
        "senderpaid",
        "senderdenomination",
        "physicalCommunicationType",
        "subject",
        "taxonomycode",
        "cap",
        "pages",
        "weight",
        "insert_timestamp_utc",
        "last_extraction",
    ]

    previous_stock_df = ensure_columns(previous_stock_df, desired_order)
    previous_stock_df = (
        previous_stock_df[desired_order].drop_duplicates(subset=["iun"]).copy()
    )

    return previous_stock_df


def migrate_legacy_cancelled_df(previous_cancelled_df: pd.DataFrame) -> pd.DataFrame:
    """
    Retrocompatibilità per il tab 'Weekly - annullate'.

    Se il tab non esiste o è vuoto, restituisce uno schema vuoto corretto.
    Se esiste ma con schema incompleto, aggiunge le colonne mancanti.
    """
    desired_order = [
        "iun",
        "sentat",
        "senderpaid",
        "senderdenomination",
        "physicalCommunicationType",
        "subject",
        "taxonomycode",
        "cap",
        "pages",
        "weight",
        "insert_timestamp_stock_utc",
        "insert_timestamp_annullate_utc",
        "last_extraction",
        "tms_cancelled",
    ]

    if previous_cancelled_df.empty:
        logging.info(
            "Tab 'Weekly - annullate' assente o vuoto. "
            "Creo struttura vuota compatibile con il nuovo job."
        )
        return pd.DataFrame(columns=desired_order)

    previous_cancelled_df = previous_cancelled_df.copy()
    previous_cancelled_df = ensure_columns(previous_cancelled_df, desired_order)
    previous_cancelled_df["last_extraction"] = "0"
    previous_cancelled_df = (
        previous_cancelled_df[desired_order]
        .drop_duplicates(subset=["iun"], keep="first")
        .copy()
    )

    return previous_cancelled_df


# ---------------- QUERY ----------------
def run_current_stock_query(spark: SparkSession) -> pd.DataFrame:
    """
    Estrae lo stock corrente delle notifiche eccedenti il limite di pagine
    ancora presenti nel perimetro.
    Granularità attesa: 1 riga = 1 IUN.
    """
    logging.info("Esecuzione query stock corrente notifiche da annullare...")

    query_sql = """
        WITH base AS (
            SELECT
                g.iun,
                g.geokey AS cap,
                g.numero_pagine AS pages,
                g.grammatura AS weight
            FROM send.gold_postalizzazione_analytics g
            JOIN send.gold_notification_analytics n ON n.iun=g.iun
            WHERE 1=1
            AND g.pcretry_rank = 1
            AND g.attempt_rank = 1
            AND n.tms_viewed IS NULL
            AND n.tms_cancelled IS NULL
            )
        SELECT
            n.iun,
            n.sentat,
            n.senderpaid,
            n.senderdenomination,
            n.physicalCommunicationType,
            n.subject,
            n.taxonomycode,
            t.cap,
            t.pages,
            t.weight
        FROM base t
        JOIN send.silver_notification n
            ON n.iun = t.iun
        WHERE
            (
                t.pages > 18
                AND n.physicalCommunicationType = 'REGISTERED_LETTER_890'
            )
            OR
            (
                t.pages > 100
                AND n.physicalCommunicationType = 'AR_REGISTERED_LETTER'
            );
    """

    df_spark = spark.sql(query_sql)
    df = spark_to_df_per_gsheet(df_spark, missing="-")
    logging.info(f"Righe estratte per stock corrente: {len(df)}")
    return df


def run_cancelled_info_query(spark: SparkSession, iun_list: list[str]) -> pd.DataFrame:
    """
    Recupera informazioni di annullamento per gli IUN usciti dallo stock.
    Nessun filtro sullo stato: la data è solo informativa.
    """
    if not iun_list:
        return pd.DataFrame(columns=["iun", "tms_cancelled"])

    quoted_iuns = ", ".join([f"'{iun}'" for iun in iun_list])

    query_sql = f"""
        SELECT DISTINCT
            iun,
            tms_cancelled
        FROM send.gold_notification_analytics
        WHERE iun IN ({quoted_iuns})
    """

    logging.info(
        "Esecuzione query informazioni di annullamento per IUN usciti dallo stock..."
    )
    df_spark = spark.sql(query_sql)
    df = spark_to_df_per_gsheet(df_spark, missing="-")
    logging.info(f"Righe info annullamento recuperate: {len(df)}")
    return df


# ---------------- TRASFORMAZIONI ----------------
def build_current_stock(
    previous_stock_df: pd.DataFrame, current_extract_df: pd.DataFrame, job_time_utc: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Costruisce il nuovo stock corrente preservando il timestamp originario di ingresso
    e valorizzando il flag di novità dell'ultima estrazione.
    """
    technical_cols = ["insert_timestamp_utc", "last_extraction"]
    previous_stock_df = previous_stock_df.copy()
    current_extract_df = current_extract_df.copy()

    if previous_stock_df.empty:
        previous_stock_df = pd.DataFrame(
            columns=list(current_extract_df.columns) + technical_cols
        )

    previous_stock_df = ensure_columns(
        previous_stock_df, list(current_extract_df.columns) + technical_cols
    )
    current_extract_df = ensure_columns(
        current_extract_df, list(current_extract_df.columns)
    )

    previous_iuns = (
        set(previous_stock_df["iun"].astype(str))
        if "iun" in previous_stock_df.columns
        else set()
    )
    current_extract_df["iun"] = current_extract_df["iun"].astype(str)

    old_insert_map = (
        previous_stock_df.set_index("iun")["insert_timestamp_utc"].to_dict()
        if "iun" in previous_stock_df.columns
        else {}
    )

    current_extract_df["insert_timestamp_utc"] = current_extract_df["iun"].map(
        old_insert_map
    )
    current_extract_df["insert_timestamp_utc"] = current_extract_df[
        "insert_timestamp_utc"
    ].replace(["", "-", "nan", "None", "NaT"], np.nan)
    current_extract_df["insert_timestamp_utc"] = current_extract_df[
        "insert_timestamp_utc"
    ].fillna(job_time_utc)

    current_extract_df["last_extraction"] = current_extract_df["iun"].apply(
        lambda x: "0" if x in previous_iuns else "1"
    )

    new_rows_df = current_extract_df[
        current_extract_df["last_extraction"] == "1"
    ].copy()

    desired_order = [
        "iun",
        "sentat",
        "senderpaid",
        "senderdenomination",
        "physicalCommunicationType",
        "subject",
        "taxonomycode",
        "cap",
        "pages",
        "weight",
        "insert_timestamp_utc",
        "last_extraction",
    ]

    current_extract_df = ensure_columns(current_extract_df, desired_order)
    current_extract_df = (
        current_extract_df[desired_order].drop_duplicates(subset=["iun"]).copy()
    )

    return current_extract_df, new_rows_df


def build_cancelled_rows(
    previous_stock_df: pd.DataFrame,
    current_stock_df: pd.DataFrame,
    previous_cancelled_df: pd.DataFrame,
    cancelled_info_df: pd.DataFrame,
    job_time_utc: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Costruisce le nuove righe da appendere nel tab annullate.
    Un IUN è considerato annullato se era nel vecchio stock e non è più nel nuovo stock.
    """
    if previous_stock_df.empty:
        empty_cols = [
            "iun",
            "sentat",
            "senderpaid",
            "senderdenomination",
            "physicalCommunicationType",
            "subject",
            "taxonomycode",
            "cap",
            "pages",
            "weight",
            "insert_timestamp_stock_utc",
            "insert_timestamp_annullate_utc",
            "last_extraction",
            "tms_cancelled",
        ]
        return pd.DataFrame(columns=empty_cols), previous_cancelled_df.copy()

    previous_stock_df = previous_stock_df.copy()
    current_stock_df = current_stock_df.copy()
    previous_cancelled_df = previous_cancelled_df.copy()
    cancelled_info_df = cancelled_info_df.copy()

    previous_stock_df["iun"] = previous_stock_df["iun"].astype(str)
    current_iuns = (
        set(current_stock_df["iun"].astype(str))
        if not current_stock_df.empty
        else set()
    )

    disappeared_df = previous_stock_df[
        ~previous_stock_df["iun"].isin(current_iuns)
    ].copy()

    if disappeared_df.empty:
        logging.info("Nessun IUN uscito dallo stock corrente.")
        if previous_cancelled_df.empty:
            previous_cancelled_df = pd.DataFrame(
                columns=[
                    "iun",
                    "sentat",
                    "senderpaid",
                    "senderdenomination",
                    "physicalCommunicationType",
                    "subject",
                    "taxonomycode",
                    "cap",
                    "pages",
                    "weight",
                    "insert_timestamp_stock_utc",
                    "insert_timestamp_annullate_utc",
                    "last_extraction",
                    "tms_cancelled",
                ]
            )
        previous_cancelled_df["last_extraction"] = "0"
        return (
            pd.DataFrame(columns=previous_cancelled_df.columns),
            previous_cancelled_df,
        )

    if previous_cancelled_df.empty:
        previous_cancelled_df = pd.DataFrame(
            columns=[
                "iun",
                "sentat",
                "senderpaid",
                "senderdenomination",
                "physicalCommunicationType",
                "subject",
                "taxonomycode",
                "cap",
                "pages",
                "weight",
                "insert_timestamp_stock_utc",
                "insert_timestamp_annullate_utc",
                "last_extraction",
                "tms_cancelled",
            ]
        )
    else:
        previous_cancelled_df = ensure_columns(
            previous_cancelled_df,
            [
                "iun",
                "sentat",
                "senderpaid",
                "senderdenomination",
                "physicalCommunicationType",
                "subject",
                "taxonomycode",
                "cap",
                "pages",
                "weight",
                "insert_timestamp_stock_utc",
                "insert_timestamp_annullate_utc",
                "last_extraction",
                "tms_cancelled",
            ],
        )
        previous_cancelled_df["last_extraction"] = "0"
        previous_cancelled_df["iun"] = previous_cancelled_df["iun"].astype(str)

    already_cancelled_iuns = (
        set(previous_cancelled_df["iun"].astype(str))
        if not previous_cancelled_df.empty
        else set()
    )
    disappeared_df = disappeared_df[
        ~disappeared_df["iun"].isin(already_cancelled_iuns)
    ].copy()

    if disappeared_df.empty:
        logging.info(
            "Gli IUN usciti dallo stock risultano già presenti nello storico annullate."
        )
        return (
            pd.DataFrame(columns=previous_cancelled_df.columns),
            previous_cancelled_df,
        )

    cancelled_info_df = ensure_columns(cancelled_info_df, ["iun", "tms_cancelled"])
    if not cancelled_info_df.empty:
        cancelled_info_df["iun"] = cancelled_info_df["iun"].astype(str)
        disappeared_df = disappeared_df.merge(
            cancelled_info_df[["iun", "tms_cancelled"]],
            on="iun",
            how="left",
        )
    else:
        disappeared_df["tms_cancelled"] = "-"

    disappeared_df["insert_timestamp_stock_utc"] = disappeared_df[
        "insert_timestamp_utc"
    ]
    disappeared_df["insert_timestamp_annullate_utc"] = job_time_utc
    disappeared_df["last_extraction"] = "1"

    desired_order = [
        "iun",
        "sentat",
        "senderpaid",
        "senderdenomination",
        "physicalCommunicationType",
        "subject",
        "taxonomycode",
        "cap",
        "pages",
        "weight",
        "insert_timestamp_stock_utc",
        "insert_timestamp_annullate_utc",
        "last_extraction",
        "tms_cancelled",
    ]

    disappeared_df = ensure_columns(disappeared_df, desired_order)
    disappeared_df = (
        disappeared_df[desired_order].drop_duplicates(subset=["iun"]).copy()
    )

    updated_cancelled_df = pd.concat(
        [previous_cancelled_df, disappeared_df], ignore_index=True
    )
    updated_cancelled_df = ensure_columns(updated_cancelled_df, desired_order)
    updated_cancelled_df = (
        updated_cancelled_df[desired_order]
        .drop_duplicates(subset=["iun"], keep="first")
        .copy()
    )

    return disappeared_df, updated_cancelled_df


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
