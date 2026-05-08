# ---------------- INIZIO SCRIPT ----------------
import json
import logging
import os
import socket
import urllib.request
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

APP_NAME = "SSDA-794_Monthly_RADD_External_Code_Registry_Check"

GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"
SHEET_ID = "1I35amT0vMMkiJffyv36fUAE90g599oyYh6Rrfpf9rso"

TAB_OUTPUT_BASE = "radd_external_code_not_valid"
TAB_LOG = "Log di controllo"

SLACK_CONFIG_CANDIDATE_PATHS = [
    "/Slack/notifications_webhook.txt",
    "/app/mount/Slack/notifications_webhook.txt",
    "/app/mount/notifications_webhook.txt",
]
SLACK_WEBHOOK_NAME = "webhook_1"
SLACK_LABEL = "SSDA-794 Monthly RADD Registry Transaction"

INITIAL_WS_ROWS = 2
GSHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEETS_CELL_LIMIT = 10_000_000


# ---------------- ECCEZIONI ----------------


class CapacityLimitError(RuntimeError):
    """Errore fatale: l'export non è rappresentabile in Google Sheets entro i limiti strutturali."""


# ---------------- FUNZIONI TEMPO ----------------


def now_utc_dt() -> datetime:
    return datetime.now(timezone.utc)


def now_utc_str() -> str:
    return now_utc_dt().strftime("%Y-%m-%d %H:%M:%S")


def elapsed_minutes_str(start_dt: datetime, end_dt: datetime | None = None) -> str:
    if end_dt is None:
        end_dt = now_utc_dt()
    elapsed_sec = (end_dt - start_dt).total_seconds()
    return f"{elapsed_sec / 60:.2f}"


# ---------------- FUNZIONI CREDENZIALI ----------------


def load_google_credentials(secret_path: str) -> dict:
    """Legge i file di credenziali e li restituisce come dizionario."""
    creds = {
        name: open(os.path.join(secret_path, name)).read().strip()
        for name in os.listdir(secret_path)
        if os.path.isfile(os.path.join(secret_path, name))
    }
    logging.info(f"Credenziali caricate: {list(creds.keys())}")
    return creds


# ---------------- FUNZIONI GOOGLE SHEETS ----------------


def get_sheet_client(creds: dict) -> Sheet:
    """Restituisce il client Sheet già configurato."""
    return Sheet(sheet_id=SHEET_ID, service_credentials=creds, id_mode="key")


def get_gspread_client(creds: dict):
    """Restituisce il client gspread autorizzato."""
    credentials = service_account.Credentials.from_service_account_info(
        creds, scopes=GSHEETS_SCOPES
    )
    return gspread.authorize(credentials)


def ensure_worksheet_exists(
    creds: dict,
    sheet_id: str,
    worksheet_name: str,
    rows: int = INITIAL_WS_ROWS,
    cols: int = 1,
):
    """Crea il worksheet se non esiste già."""
    gc = get_gspread_client(creds)
    sh = gc.open_by_key(sheet_id)

    try:
        sh.worksheet(worksheet_name)
        logging.info(f"Worksheet '{worksheet_name}' già presente.")
    except gspread.WorksheetNotFound:
        logging.info(
            f"Worksheet '{worksheet_name}' non presente. Creazione in corso..."
        )
        sh.add_worksheet(title=worksheet_name, rows=max(rows, 1), cols=max(cols, 1))
        logging.info(f"Worksheet '{worksheet_name}' creato correttamente.")


def get_reference_month_tab_name(spark: SparkSession) -> str:
    """
    Costruisce il nome del tab mensile riferito al mese precedente rispetto alla run.
    Esempio: run a giugno 2026 -> radd_external_code_not_valid_05_26
    """
    row = spark.sql("""
        SELECT DATE_FORMAT(ADD_MONTHS(TRUNC(NOW(), 'MM'), -1), 'MM_yy') AS month_suffix
        """).collect()[0]

    month_suffix = row["month_suffix"]
    tab_name = f"{TAB_OUTPUT_BASE}_{month_suffix}"

    logging.info(f"Tab mensile di output determinato: {tab_name}")
    return tab_name


# ---------------- FUNZIONI ESPORTAZIONE ----------------


def sanitize_for_sheets(df: pd.DataFrame, missing: str = "-") -> pd.DataFrame:
    """Sanitizza il DataFrame per la scrittura su Google Sheets."""
    if df is None:
        return pd.DataFrame()

    df = df.copy()
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df = df.fillna(missing)

    for col in df.columns:
        df[col] = df[col].astype(str)
        df.loc[df[col].isin(["nan", "None", "NaT"]), col] = missing

    return df


def _resize_worksheet_grid_best_effort(
    creds: dict,
    sheet_id: str,
    worksheet_name: str,
    target_rows: int | None,
    target_cols: int | None,
):
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
                f"Resize non necessario per '{worksheet_name}': "
                f"rows={current_rows}, cols={current_cols}"
            )
            return

        logging.info(
            f"Resize griglia worksheet '{worksheet_name}': "
            f"rows {current_rows}->{final_rows}, cols {current_cols}->{final_cols}"
        )
        ws.resize(rows=final_rows, cols=final_cols)

    except CapacityLimitError:
        raise
    except Exception as e:
        logging.warning(
            f"Resize fallito per worksheet='{worksheet_name}' "
            f"(sheet_id={sheet_id}). Continuo senza resize. Dettaglio: {e}"
        )


def format_header_row(
    creds: dict,
    sheet_id: str,
    worksheet_name: str,
):
    """Formatta la riga header come da standard: Arial 10, bold, italic, viola chiaro 3."""

    try:
        gc = get_gspread_client(creds)
        sh = gc.open_by_key(sheet_id)
        ws = sh.worksheet(worksheet_name)

        ws.format(
            "1:1",
            {
                "backgroundColor": {
                    "red": 0.8509803922,
                    "green": 0.8235294118,
                    "blue": 0.9137254902,
                },
                "textFormat": {
                    "fontFamily": "Arial",
                    "fontSize": 10,
                    "bold": True,
                    "italic": True,
                    "foregroundColor": {
                        "red": 0,
                        "green": 0,
                        "blue": 0,
                    },
                },
            },
        )

        logging.info(
            f"Header formattato correttamente per worksheet '{worksheet_name}'."
        )

    except Exception as e:
        logging.warning(
            f"Formattazione header fallita per worksheet='{worksheet_name}'. "
            f"Dettaglio: {e}"
        )


def export_to_sheets(
    df: pd.DataFrame, creds: dict, sheet_name: str, missing: str = "-"
) -> pd.DataFrame:
    """
    Scrive il DataFrame sul tab indicato.
    A parità di nome tab, il contenuto viene sovrascritto.
    """
    logging.info(f"Scrittura su Google Sheet: tab '{sheet_name}'...")

    df = sanitize_for_sheets(df, missing=missing)

    target_rows = max(len(df) + 1, INITIAL_WS_ROWS)
    target_cols = max(len(df.columns), 1)

    ensure_worksheet_exists(
        creds=creds,
        sheet_id=SHEET_ID,
        worksheet_name=sheet_name,
        rows=target_rows,
        cols=target_cols,
    )

    _resize_worksheet_grid_best_effort(
        creds=creds,
        sheet_id=SHEET_ID,
        worksheet_name=sheet_name,
        target_rows=target_rows,
        target_cols=target_cols,
    )

    sheet = get_sheet_client(creds)
    sheet.upload(sheet_name, df)

    format_header_row(
        creds=creds,
        sheet_id=SHEET_ID,
        worksheet_name=sheet_name,
    )

    logging.info(
        f"Scrittura completata su tab '{sheet_name}'. "
        f"Righe scritte: {len(df)} | Colonne: {len(df.columns)}"
    )

    return df


def spark_to_sheets(
    df_spark, creds: dict, sheet_name: str, missing: str = "-"
) -> pd.DataFrame:
    if df_spark is None:
        logging.warning(f"DataFrame Spark nullo, skip scrittura su tab '{sheet_name}'.")
        return pd.DataFrame()

    logging.info(f"Conversione DataFrame Spark -> Pandas per tab '{sheet_name}'...")
    df = df_spark.toPandas()
    logging.info(f"Conversione completata. Righe convertite: {len(df)}")

    return export_to_sheets(df, creds, sheet_name, missing=missing)


def upsert_log_to_sheets(
    log_df: pd.DataFrame,
    creds: dict,
    sheet_name: str = TAB_LOG,
    missing: str = "-",
):
    """
    Scrive il log in modalità upsert sulla chiave 'Tab report':
    - se esiste già una riga per lo stesso Tab report, la sovrascrive;
    - se non esiste, aggiunge una nuova riga.

    Le colonne del log restano quelle storiche, con l'aggiunta della sola colonna
    iniziale 'Tab report'.
    """
    logging.info(f"Upsert log su Google Sheet: tab '{sheet_name}'...")

    log_df = sanitize_for_sheets(log_df, missing=missing)

    ensure_worksheet_exists(
        creds=creds,
        sheet_id=SHEET_ID,
        worksheet_name=sheet_name,
        rows=INITIAL_WS_ROWS,
        cols=max(len(log_df.columns), 1),
    )

    gc = get_gspread_client(creds)
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(sheet_name)

    expected_header = list(log_df.columns)
    existing_values = ws.get_all_values()

    if not existing_values:
        logging.info(f"Tab log '{sheet_name}' vuoto: scrittura header e prima riga.")
        ws.append_row(expected_header)
        ws.append_rows(log_df.values.tolist())
        return

    current_header = existing_values[0]

    if current_header != expected_header:
        logging.warning(
            f"Header del tab log '{sheet_name}' diverso da quello atteso. "
            f"Header attuale={current_header} | Header atteso={expected_header}"
        )
        raise RuntimeError(
            f"Header del tab log '{sheet_name}' non compatibile con lo schema atteso."
        )

    tab_report_value = str(log_df.iloc[0]["Tab report"])
    rows = existing_values[1:]
    target_row_number = None

    for idx, row in enumerate(rows, start=2):
        if row and row[0] == tab_report_value:
            target_row_number = idx
            break

    new_row = log_df.iloc[0].tolist()

    _resize_worksheet_grid_best_effort(
        creds=creds,
        sheet_id=SHEET_ID,
        worksheet_name=sheet_name,
        target_rows=max(len(existing_values) + 1, INITIAL_WS_ROWS),
        target_cols=len(expected_header),
    )

    if target_row_number is not None:
        logging.info(
            f"Riga log già presente per Tab report='{tab_report_value}'. "
            f"Sovrascrittura riga {target_row_number}."
        )
        end_col_letter = gspread.utils.rowcol_to_a1(1, len(expected_header)).replace(
            "1", ""
        )
        ws.update(
            f"A{target_row_number}:{end_col_letter}{target_row_number}",
            [new_row],
        )
    else:
        logging.info(
            f"Nessuna riga log presente per Tab report='{tab_report_value}'. "
            "Append nuova riga."
        )
        ws.append_rows([new_row])

    logging.info(f"Upsert log completato su tab '{sheet_name}'.")


# ---------------- SLACK ----------------


def load_slack_webhooks(config_paths: list[str]) -> dict[str, str]:
    for config_path in config_paths:
        try:
            if not os.path.isfile(config_path):
                continue

            webhooks = {}

            with open(config_path, "r", encoding="utf-8") as f:
                for raw_line in f:
                    line = raw_line.strip()

                    if not line or line.startswith("#"):
                        continue

                    if "=" not in line:
                        logging.warning(
                            f"Riga non valida nel file webhook '{config_path}': {line}"
                        )
                        continue

                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()

                    if not key or not value:
                        logging.warning(
                            f"Riga incompleta nel file webhook '{config_path}': {line}"
                        )
                        continue

                    webhooks[key] = value

            if webhooks:
                logging.info(
                    f"Configurazione webhook caricata da: {config_path}. "
                    f"Webhook disponibili: {list(webhooks.keys())}"
                )
            else:
                logging.warning(
                    f"File webhook trovato ma senza webhook validi: {config_path}"
                )

            return webhooks

        except Exception as e:
            logging.warning(
                f"Errore nella lettura del file webhook '{config_path}'. "
                f"Procedo col path successivo. Dettaglio: {e}"
            )

    logging.warning(
        "Nessun file webhook valido trovato. Le notifiche Slack saranno disabilitate."
    )
    return {}


def get_slack_webhook_by_name(
    webhooks: dict[str, str], webhook_name: str
) -> str | None:
    if not webhook_name:
        logging.warning("Nome webhook non valorizzato.")
        return None

    webhook_url = webhooks.get(webhook_name)

    if not webhook_url:
        logging.warning(
            f"Webhook '{webhook_name}' non trovato nella configurazione. "
            "Le notifiche Slack saranno disabilitate."
        )
        return None

    return webhook_url


def send_slack(webhook_url: str | None, message: str) -> None:
    if not webhook_url:
        logging.warning("Webhook Slack non valorizzato: nessuna notifica inviata.")
        return

    payload = {"text": message}
    req = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=20) as resp:
        _ = resp.read()


def build_slack_prefix() -> str:
    prefix = "*CDE Job Alert*"
    if SLACK_LABEL:
        prefix += f" ({SLACK_LABEL})"
    return prefix


# ---------------- QUERY PRINCIPALI ----------------


def run_query(spark: SparkSession):
    logging.info("Costruzione DataFrame base RADD final_complete...")

    query = """
    WITH
    tr_last_month AS (
        SELECT DISTINCT
            t.transactionid,
            SUBSTRING(t.transactionid, 6, 11) AS pt,
            SUBSTRING(t.uid, INSTR(t.uid, '@') + 1, LENGTH(t.uid)) AS external_code,
            CAST(t.operationstartdate AS TIMESTAMP) AS transaction_time,
            t.operationtype,
            t.operation_status,
            t.uid,
            t.operationstartdate,
            t.operationenddate,
            t.dl_event_tms
        FROM send.silver_radd_transaction_entity t
        WHERE t.operationstartdate IS NOT NULL
          AND CAST(t.operationstartdate AS TIMESTAMP) >= ADD_MONTHS(TRUNC(NOW(), 'MM'), -1)
          AND CAST(t.operationstartdate AS TIMESTAMP) < TRUNC(NOW(), 'MM')
          AND t.uid IS NOT NULL
          AND INSTR(t.uid, '@') > 0
    ),
    registry_raw AS (
        SELECT DISTINCT
            r.partnerid,
            r.locationid,
            c AS external_code,
            r.description,
            r.partnertype,
            CAST(r.startvalidity AS TIMESTAMP) AS startvalidity_ts,
            CAST(r.endvalidity AS TIMESTAMP) AS endvalidity_ts,
            CAST(r.updatetimestamp AS TIMESTAMP) AS updatetimestamp_ts,
            CASE
                WHEN r.dynamodb_eventname IS NULL THEN 'NULL'
                ELSE UPPER(TRIM(r.dynamodb_eventname))
            END AS event_name_norm,
            r.dl_is_deleted,
            r.dl_event_tms,
            COALESCE(CAST(r.updatetimestamp AS TIMESTAMP), r.dl_event_tms) AS registry_event_time
        FROM send.silver_radd_registry r
        LATERAL VIEW EXPLODE(r.externalcodes) exploded_codes AS c
        WHERE c IS NOT NULL
          AND TRIM(c) <> ''
          AND r.partnerid IS NOT NULL
          AND r.locationid IS NOT NULL
    ),
    registry_classified AS (
        SELECT
            rr.*,
            CASE
                WHEN rr.event_name_norm IN ('INSERT', 'MODIFY')
                 AND COALESCE(rr.dl_is_deleted, FALSE) = FALSE
                    THEN 1
                WHEN rr.event_name_norm = 'REMOVE'
                 AND rr.dl_is_deleted = TRUE
                    THEN 0
                ELSE NULL
            END AS is_active_event,
            CASE
                WHEN rr.event_name_norm IN ('INSERT', 'MODIFY')
                 AND COALESCE(rr.dl_is_deleted, FALSE) = FALSE
                    THEN 1
                WHEN rr.event_name_norm = 'REMOVE'
                 AND rr.dl_is_deleted = TRUE
                    THEN 1
                ELSE 0
            END AS is_supported_event
        FROM registry_raw rr
    ),
    registry_timeline AS (
        SELECT
            rc.*,
            LAG(rc.is_active_event) OVER (
                PARTITION BY rc.partnerid, rc.locationid, rc.external_code
                ORDER BY rc.registry_event_time, rc.dl_event_tms
            ) AS prev_is_active_event,
            LEAD(rc.registry_event_time) OVER (
                PARTITION BY rc.partnerid, rc.locationid, rc.external_code
                ORDER BY rc.registry_event_time, rc.dl_event_tms
            ) AS next_registry_event_time
        FROM registry_classified rc
        WHERE rc.is_supported_event = 1
    ),
    registry_validity_windows AS (
        SELECT
            rt.partnerid,
            rt.locationid,
            rt.external_code,
            rt.description,
            rt.partnertype,
            rt.event_name_norm,
            rt.dl_is_deleted,
            rt.startvalidity_ts,
            rt.endvalidity_ts,
            rt.updatetimestamp_ts,
            rt.dl_event_tms,
            rt.registry_event_time,
            rt.prev_is_active_event,
            rt.next_registry_event_time,
            CASE
                WHEN rt.prev_is_active_event = 0
                    THEN rt.registry_event_time
                WHEN rt.startvalidity_ts IS NOT NULL
                    THEN rt.startvalidity_ts
                ELSE rt.registry_event_time
            END AS valid_from,
            CASE
                WHEN rt.endvalidity_ts IS NOT NULL AND rt.next_registry_event_time IS NOT NULL
                    THEN LEAST(rt.endvalidity_ts, rt.next_registry_event_time)
                WHEN rt.endvalidity_ts IS NOT NULL
                    THEN rt.endvalidity_ts
                WHEN rt.next_registry_event_time IS NOT NULL
                    THEN rt.next_registry_event_time
                ELSE CAST('2999-12-31 00:00:00' AS TIMESTAMP)
            END AS valid_to_eff
        FROM registry_timeline rt
        WHERE rt.is_active_event = 1
    ),
    static_match AS (
        SELECT DISTINCT
            tr.transactionid
        FROM tr_last_month tr
        JOIN (
            SELECT DISTINCT
                partnerid,
                external_code
            FROM registry_raw
        ) reg
          ON tr.pt = reg.partnerid
         AND tr.external_code = reg.external_code
    ),
    temporal_match AS (
        SELECT DISTINCT
            tr.transactionid
        FROM tr_last_month tr
        JOIN registry_validity_windows rvw
          ON tr.pt = rvw.partnerid
         AND tr.external_code = rvw.external_code
         AND tr.transaction_time >= rvw.valid_from
         AND tr.transaction_time < rvw.valid_to_eff
    ),
    final_diagnostic_base AS (
        SELECT
            tr.pt,
            tr.external_code,
            tr.transactionid,
            tr.transaction_time,
            tr.operationtype,
            tr.operation_status,
            tr.uid,
            tr.operationstartdate,
            tr.operationenddate,
            tr.dl_event_tms,
            CASE
                WHEN tm.transactionid IS NOT NULL THEN 'CODICE_VALIDO_NEL_MOMENTO'
                WHEN sm.transactionid IS NOT NULL THEN 'CODICE_PRESENTE_MA_NON_VALIDO'
                ELSE 'CODICE_NON_PRESENTE_NEL_REGISTRY'
            END AS esito_controllo_codice
        FROM tr_last_month tr
        LEFT JOIN static_match sm
            ON tr.transactionid = sm.transactionid
        LEFT JOIN temporal_match tm
            ON tr.transactionid = tm.transactionid
    ),
    final_complete AS (
        SELECT
            pt AS partnerid_transaction,
            external_code,
            transactionid,
            transaction_time,
            operationtype,
            operation_status,
            uid,
            operationstartdate,
            operationenddate,
            dl_event_tms,
            esito_controllo_codice
        FROM final_diagnostic_base
    )
    SELECT
        partnerid_transaction,
        external_code,
        transactionid,
        transaction_time,
        operationtype,
        operation_status,
        uid,
        operationstartdate,
        operationenddate,
        dl_event_tms,
        esito_controllo_codice
    FROM final_complete
    """

    df = spark.sql(query)
    logging.info("DataFrame base costruito correttamente.")
    return df


def get_operationstartdate_range_last_month(spark: SparkSession) -> tuple[str, str]:
    logging.info(
        "Recupero MIN/MAX(operationstartdate) sul perimetro del mese precedente..."
    )

    df = spark.sql("""
        SELECT
            MIN(operationstartdate) AS min_operationstartdate_last_month,
            MAX(operationstartdate) AS max_operationstartdate_last_month
        FROM send.silver_radd_transaction_entity
        WHERE operationstartdate IS NOT NULL
          AND CAST(operationstartdate AS TIMESTAMP) >= ADD_MONTHS(TRUNC(NOW(), 'MM'), -1)
          AND CAST(operationstartdate AS TIMESTAMP) < TRUNC(NOW(), 'MM')
          AND uid IS NOT NULL
          AND INSTR(uid, '@') > 0
        """)

    row = df.collect()[0]
    min_value = row["min_operationstartdate_last_month"]
    max_value = row["max_operationstartdate_last_month"]

    min_operationstartdate_last_month = str(min_value) if min_value is not None else "-"
    max_operationstartdate_last_month = str(max_value) if max_value is not None else "-"

    logging.info(
        "Range operationstartdate mese precedente recuperato: "
        f"MIN={min_operationstartdate_last_month} | "
        f"MAX={max_operationstartdate_last_month}"
    )

    return min_operationstartdate_last_month, max_operationstartdate_last_month


def build_log_df(
    report_tab_name: str,
    min_operationstartdate_last_month: str,
    max_operationstartdate_last_month: str,
    job_start_ts_utc: str,
    output_df: pd.DataFrame,
    count_present_but_not_valid: int,
    count_never_present: int,
) -> pd.DataFrame:
    """Costruisce la riga di log mantenendo le colonne storiche e aggiungendo solo Tab report."""

    total_rows = len(output_df)
    total_columns = len(output_df.columns)

    distinct_partnerid = (
        output_df["partnerid_transaction"].nunique()
        if not output_df.empty and "partnerid_transaction" in output_df.columns
        else 0
    )

    distinct_external_code = (
        output_df["external_code"].nunique()
        if not output_df.empty and "external_code" in output_df.columns
        else 0
    )

    distinct_transactionid = (
        output_df["transactionid"].nunique()
        if not output_df.empty and "transactionid" in output_df.columns
        else 0
    )

    return pd.DataFrame(
        {
            "Tab report": [report_tab_name],
            "Data esecuzione script start (UTC)": [job_start_ts_utc],
            "MIN operationstartdate mese precedente": [
                min_operationstartdate_last_month
            ],
            "MAX operationstartdate mese precedente": [
                max_operationstartdate_last_month
            ],
            "Colonne estratte": [total_columns],
            "Righe estratte": [total_rows],
            "transactionid distinti": [int(distinct_transactionid)],
            "external_code distinti": [int(distinct_external_code)],
            "partnerid distinti": [int(distinct_partnerid)],
            "# CODICE_PRESENTE_MA_NON_VALIDO": [int(count_present_but_not_valid)],
            "# CODICE_NON_PRESENTE_NEL_REGISTRY": [int(count_never_present)],
        }
    )


# ---------------- MAIN ----------------


def main():
    logging.info("Inizializzazione SparkSession...")
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    job_start_dt = now_utc_dt()
    job_start_str = job_start_dt.strftime("%Y-%m-%d %H:%M:%S")
    run_id = job_start_dt.strftime("%Y%m%dT%H%M%SZ")
    hostname = socket.gethostname()

    slack_prefix = build_slack_prefix()
    slack_webhooks = load_slack_webhooks(SLACK_CONFIG_CANDIDATE_PATHS)
    slack_webhook_url = get_slack_webhook_by_name(slack_webhooks, SLACK_WEBHOOK_NAME)

    creds = None
    base_df = None
    output_df = pd.DataFrame()
    counts_pdf = pd.DataFrame()

    report_tab_name = "-"
    min_operationstartdate_last_month = "-"
    max_operationstartdate_last_month = "-"
    count_present_but_not_valid = 0
    count_never_present = 0

    try:
        creds = load_google_credentials(GOOGLE_SECRET_PATH)
        report_tab_name = get_reference_month_tab_name(spark)

        base_df = run_query(spark)
        base_df.persist(StorageLevel.MEMORY_AND_DISK)

        logging.info("Materializzazione DataFrame base persistito...")
        base_count = base_df.count()
        logging.info(f"Materializzazione completata. Righe base: {base_count}")

        counts_df = (
            base_df.groupBy("esito_controllo_codice")
            .count()
            .withColumnRenamed("count", "numero_transaction")
        )

        counts_pdf = counts_df.toPandas() if counts_df is not None else pd.DataFrame()

        if not counts_pdf.empty and "esito_controllo_codice" in counts_pdf.columns:
            counts_pdf["numero_transaction"] = pd.to_numeric(
                counts_pdf["numero_transaction"], errors="coerce"
            ).fillna(0)

            count_present_but_not_valid = int(
                counts_pdf.loc[
                    counts_pdf["esito_controllo_codice"]
                    == "CODICE_PRESENTE_MA_NON_VALIDO",
                    "numero_transaction",
                ].sum()
            )

            count_never_present = int(
                counts_pdf.loc[
                    counts_pdf["esito_controllo_codice"]
                    == "CODICE_NON_PRESENTE_NEL_REGISTRY",
                    "numero_transaction",
                ].sum()
            )

        output_spark_df = base_df.filter(
            "esito_controllo_codice <> 'CODICE_VALIDO_NEL_MOMENTO'"
        ).orderBy("partnerid_transaction", "external_code", "transaction_time")

        output_df = spark_to_sheets(output_spark_df, creds, report_tab_name)

        (
            min_operationstartdate_last_month,
            max_operationstartdate_last_month,
        ) = get_operationstartdate_range_last_month(spark)

        log_df = build_log_df(
            report_tab_name=report_tab_name,
            min_operationstartdate_last_month=min_operationstartdate_last_month,
            max_operationstartdate_last_month=max_operationstartdate_last_month,
            job_start_ts_utc=job_start_str,
            output_df=output_df,
            count_present_but_not_valid=count_present_but_not_valid,
            count_never_present=count_never_present,
        )

        upsert_log_to_sheets(log_df, creds, TAB_LOG)

        job_end_dt = now_utc_dt()
        job_elapsed_min = elapsed_minutes_str(job_start_dt, job_end_dt)

        slack_msg = (
            f"{slack_prefix}\n"
            f"✅✅✅ *SUCCESS* ✅✅✅\n"
            f"*Job:* {APP_NAME}\n"
            f"*RunId (UTC):* {run_id}\n"
            f"*Host:* {hostname}\n"
            f"*Tab report:* {report_tab_name}\n"
            f"*Periodo operationstartdate:* {min_operationstartdate_last_month} → {max_operationstartdate_last_month}\n"
            f"*Righe estratte:* {len(output_df)}\n"
            f"*PartnerId distinti:* "
            f"{output_df['partnerid_transaction'].nunique() if not output_df.empty and 'partnerid_transaction' in output_df.columns else 0}\n"
            f"*External code distinti:* "
            f"{output_df['external_code'].nunique() if not output_df.empty and 'external_code' in output_df.columns else 0}\n"
            f"*CODICE_PRESENTE_MA_NON_VALIDO:* {count_present_but_not_valid}\n"
            f"*CODICE_NON_PRESENTE_NEL_REGISTRY:* {count_never_present}\n"
            f"*Tempo impiegato minuti:* {job_elapsed_min}\n"
        )

        logging.info("Processo completato con successo.")
        send_slack(slack_webhook_url, slack_msg)

    except Exception:
        logging.error("Errore durante l'esecuzione del job.", exc_info=True)

        fail_msg = (
            f"{slack_prefix}\n"
            f"❌❌❌ *FAILURE* ❌❌❌\n"
            f"*Job:* {APP_NAME}\n"
            f"*RunId (UTC):* {run_id}\n"
            f"*Host:* {hostname}\n"
            f"*Tab report previsto:* {report_tab_name}\n"
            f"Il job non è terminato correttamente.\n"
        )

        try:
            send_slack(slack_webhook_url, fail_msg)
        finally:
            raise

    finally:
        try:
            if base_df is not None:
                logging.info("Rilascio DataFrame persistito...")
                base_df.unpersist(blocking=True)
        except Exception:
            logging.warning(
                "Errore durante unpersist del DataFrame base.", exc_info=True
            )

        try:
            spark.stop()
            logging.info("Spark session terminata.")
        except Exception:
            logging.warning("spark.stop() fallito in chiusura.", exc_info=True)


if __name__ == "__main__":
    main()
