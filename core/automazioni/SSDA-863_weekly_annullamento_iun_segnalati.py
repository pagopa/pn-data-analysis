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
from pyspark.sql import SparkSession

# ---------------- CONFIGURAZIONE BASE ----------------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

APP_NAME = "SSDA-863_weekly_annullamento_iun_segnalati"

GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"

# Spreadsheet di output
SHEET_ID = "1k19pEfU0HnMIqbVrqLBL89LBXqjbhQr4GpCKDQfJfHw"

# Spreadsheet di input: contiene la colonna IUN
INPUT_SHEET_ID = "1_xr1o18Ro48cwgdemaHaGMH4YgYrAbs1NpnlBXzeLGY"
INPUT_TAB = "iun_segnalati"
INPUT_IUN_COLUMN = "iun"

TAB_DA_ANNULLARE = "Weekly - da annullare"
TAB_LOG = "Log di controllo"

SLACK_CONFIG_CANDIDATE_PATHS = [
    "/Slack/notifications_webhook.txt",
    "/app/mount/Slack/notifications_webhook.txt",
    "/app/mount/notifications_webhook.txt",
]

SLACK_WEBHOOK_NAME = "webhook_1"
SLACK_LABEL = "SSDA-863 Annullamento IUN segnalati"

INITIAL_WS_ROWS = 2
GSHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEETS_CELL_LIMIT = 10_000_000
QUERY_IN_CHUNK_SIZE = 800

OUTPUT_COLS = [
    "iun",
    "sentat",
    "senderpaid",
    "senderdenomination",
    "subject",
    "taxonomycode",
    "annullamento",
]


# ---------------- ECCEZIONI ----------------
class CapacityLimitError(RuntimeError):
    """Errore fatale: l'export non è rappresentabile in Google Sheets entro i limiti strutturali."""


# ---------------- TEMPO ----------------
def now_utc_dt() -> datetime:
    return datetime.now(timezone.utc)


# ---------------- CREDENZIALI ----------------
def load_google_credentials(secret_path: str) -> dict:
    """Legge i file di credenziali e li restituisce come dizionario."""
    creds = {
        name: open(os.path.join(secret_path, name)).read().strip()
        for name in os.listdir(secret_path)
        if os.path.isfile(os.path.join(secret_path, name))
    }
    logging.info(f"Credenziali caricate: {list(creds.keys())}")
    return creds


# ---------------- GSHEET ----------------
def build_google_sheet_url(sheet_id: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"


def get_sheet_client(creds: dict) -> Sheet:
    return Sheet(sheet_id=SHEET_ID, service_credentials=creds, id_mode="key")


def get_input_sheet_client(creds: dict) -> Sheet:
    return Sheet(sheet_id=INPUT_SHEET_ID, service_credentials=creds, id_mode="key")


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


def sanitize_for_sheets(df: pd.DataFrame, missing: str = "-") -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()

    df = df.copy()
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df = df.fillna(missing)

    for col in df.columns:
        df[col] = df[col].astype(str)
        df.loc[df[col].isin(["nan", "None", "NaT"]), col] = missing

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
    df = sanitize_for_sheets(df, missing=missing)
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


def export_to_sheets(df: pd.DataFrame, creds: dict, sheet_name: str):
    """
    Scrive su Google Sheet con resize best-effort della griglia.
    """
    logging.info(f"Scrittura su Google Sheet: {sheet_name}")

    df = sanitize_for_sheets(df, missing="-")

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
                        continue

                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()

                    if key and value:
                        webhooks[key] = value

            return webhooks

        except Exception as e:
            logging.warning(f"Errore lettura webhook '{config_path}': {e}")

    return {}


def get_slack_webhook_by_name(
    webhooks: dict[str, str],
    webhook_name: str,
) -> str | None:
    return webhooks.get(webhook_name)


def send_slack(
    webhook_url: str | None,
    message: str,
) -> None:
    if not webhook_url:
        logging.warning("Webhook Slack non configurato.")
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


# ---------------- FUNZIONI INPUT ----------------
def dedupe_iun_list(values: list[str]) -> list[str]:
    seen = set()
    iun_list = []

    for value in values:
        iun = str(value).strip()
        if not iun or iun in ["-", "nan", "None", "NaT"]:
            continue
        if iun.lower() == "iun":
            continue
        if iun not in seen:
            seen.add(iun)
            iun_list.append(iun)

    return iun_list


def read_input_iuns(creds: dict) -> list[str]:
    """
    Legge la lista IUN dal file di input.
    La colonna viene ricercata in modo case-insensitive.
    """
    input_sheet = get_input_sheet_client(creds)
    input_df = safe_download_sheet(input_sheet, INPUT_TAB)

    if input_df.empty:
        logging.warning("Tab input vuoto o non leggibile: nessuno IUN da processare.")
        return []

    column_map = {str(c).strip().lower(): c for c in input_df.columns}
    requested_column = INPUT_IUN_COLUMN.strip().lower()

    if requested_column not in column_map:
        raise ValueError(
            f"Colonna IUN '{INPUT_IUN_COLUMN}' non trovata nel tab input '{INPUT_TAB}'. "
            f"Colonne disponibili: {list(input_df.columns)}"
        )

    iun_list = dedupe_iun_list(input_df[column_map[requested_column]].tolist())
    logging.info(f"IUN distinti letti da input: {len(iun_list)}")
    return iun_list


# ---------------- QUERY ----------------
def get_last_update_date(spark: SparkSession) -> str:
    """Estrae la data più recente (MAX sentat) dal dataset notifiche."""
    logging.info("Estrazione data ultimo aggiornamento...")
    max_date_df = spark.sql(
        "SELECT MAX(sentat) AS max_ts FROM send.gold_notification_analytics"
    )
    max_date = max_date_df.collect()[0]["max_ts"]

    if isinstance(max_date, datetime):
        return max_date.strftime("%Y-%m-%d %H:%M:%S")
    return str(max_date) if max_date is not None else "-"


def escape_sql_literal(value: str) -> str:
    return str(value).replace("'", "''")


def chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[i : i + size] for i in range(0, len(values), size)]


def run_notification_info_query(
    spark: SparkSession, iun_list: list[str]
) -> pd.DataFrame:
    """
    Recupera le informazioni richieste per gli IUN in input.
    Granularità attesa: 1 riga = 1 IUN.
    """
    query_cols = OUTPUT_COLS + ["actual_status"]

    if not iun_list:
        return pd.DataFrame(columns=query_cols)

    frames = []

    for part in chunked(iun_list, QUERY_IN_CHUNK_SIZE):
        quoted_iuns = ", ".join([f"'{escape_sql_literal(iun)}'" for iun in part])

        query_sql = f"""
            WITH institution_base AS (
                SELECT
                    internalistitutionid AS senderpaid,
                    CASE
                        WHEN subunittype IS NOT NULL
                            THEN CONCAT(rootparent_description, ' - ', description)
                        ELSE description
                    END AS senderdenomination,
                    updatedat
                FROM selfcare.gold_contracts
                WHERE product = 'prod-pn'
                  AND state = 'ACTIVE'
                  AND internalistitutionid IS NOT NULL
            ),
            institution AS (
                SELECT
                    senderpaid,
                    senderdenomination
                FROM (
                    SELECT
                        senderpaid,
                        senderdenomination,
                        ROW_NUMBER() OVER (
                            PARTITION BY senderpaid
                            ORDER BY updatedat DESC
                        ) AS rn
                    FROM institution_base
                ) t
                WHERE rn = 1
            )
            SELECT DISTINCT
                sn.iun,
                sn.sentat,
                sn.senderpaid,
                COALESCE(sn.senderdenomination, i.senderdenomination) AS senderdenomination,
                sn.subject,
                sn.taxonomycode,
                CASE
                    WHEN n.actual_status = 'CANCELLED' THEN 'SI'
                    ELSE 'NO'
                END AS annullamento,
                n.actual_status
            FROM send.silver_notification sn
            LEFT JOIN send.gold_notification_analytics n
                ON sn.iun = n.iun
            LEFT JOIN institution i
                ON sn.senderpaid = i.senderpaid
            WHERE sn.iun IN ({quoted_iuns})
        """

        logging.info(
            f"Esecuzione query informazioni notifiche per chunk da {len(part)} IUN..."
        )
        frames.append(spark_to_df_per_gsheet(spark.sql(query_sql), missing="-"))

    notification_info_df = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=query_cols)
    )
    notification_info_df = ensure_columns(notification_info_df, query_cols)
    notification_info_df = (
        notification_info_df[query_cols]
        .drop_duplicates(subset=["iun"], keep="first")
        .copy()
    )

    logging.info(f"Righe info notifiche recuperate: {len(notification_info_df)}")
    return notification_info_df


# ---------------- TRASFORMAZIONI ----------------
def build_output_df(notification_info_df: pd.DataFrame) -> pd.DataFrame:
    """
    Costruisce il tab finale Weekly - da annullare.
    La colonna annullamento vale:
    - SI se actual_status = CANCELLED
    - NO in tutti gli altri casi
    """
    output_df = notification_info_df.copy()
    output_df = ensure_columns(output_df, OUTPUT_COLS)
    output_df = output_df[OUTPUT_COLS].drop_duplicates(subset=["iun"], keep="first")
    return output_df.copy()


def build_log_df(
    max_date_str: str,
    job_time_utc: str,
    stato_run: str,
    input_count: int,
    found_count: int,
    annullamento_si_count: int,
    annullamento_no_count: int,
) -> pd.DataFrame:
    """
    Costruisce il tab Log di controllo.
    """
    log_df = pd.DataFrame(
        {
            "Data esecuzione script (UTC)": [job_time_utc],
            "Ultimo aggiornamento dati (MAX sentat gold_notification_analytics)": [
                max_date_str
            ],
            "Stato run": [stato_run],
            "# IUN letti in input": [input_count],
            "# IUN con informazioni": [found_count],
            "# annullamento SI": [annullamento_si_count],
            "# annullamento NO": [annullamento_no_count],
        }
    )

    return sanitize_for_sheets(log_df)


def write_tab_log(
    creds: dict,
    max_date_str: str,
    job_time_utc: str,
    stato_run: str,
    input_count: int,
    found_count: int,
    annullamento_si_count: int,
    annullamento_no_count: int,
):
    log_df = build_log_df(
        max_date_str=max_date_str,
        job_time_utc=job_time_utc,
        stato_run=stato_run,
        input_count=input_count,
        found_count=found_count,
        annullamento_si_count=annullamento_si_count,
        annullamento_no_count=annullamento_no_count,
    )

    export_to_sheets(
        df=log_df,
        creds=creds,
        sheet_name=TAB_LOG,
    )


# ---------------- MAIN SCRIPT ----------------
def main():
    logging.info("Inizializzazione SparkSession...")
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    job_start_dt = now_utc_dt()
    job_start_str = job_start_dt.strftime("%Y-%m-%d %H:%M:%S")
    run_id = job_start_dt.strftime("%Y%m%dT%H%M%SZ")
    hostname = socket.gethostname()

    sheet_url = build_google_sheet_url(SHEET_ID)
    slack_prefix = build_slack_prefix()

    slack_webhooks = load_slack_webhooks(SLACK_CONFIG_CANDIDATE_PATHS)
    slack_webhook_url = get_slack_webhook_by_name(
        slack_webhooks,
        SLACK_WEBHOOK_NAME,
    )

    max_date_str = "-"
    input_count = 0
    found_count = 0
    annullamento_si_count = 0
    annullamento_no_count = 0

    try:
        logging.info(f"RunId: {run_id}")
        logging.info(f"Host: {hostname}")
        logging.info(f"Google Sheet output: {sheet_url}")
        logging.info(f"Tab output: {TAB_DA_ANNULLARE}")
        logging.info(f"Tab log: {TAB_LOG}")

        creds = load_google_credentials(GOOGLE_SECRET_PATH)

        max_date_str = get_last_update_date(spark)

        write_tab_log(
            creds=creds,
            max_date_str=max_date_str,
            job_time_utc=job_start_str,
            stato_run="IN CORSO",
            input_count=0,
            found_count=0,
            annullamento_si_count=0,
            annullamento_no_count=0,
        )

        input_iuns = read_input_iuns(creds)
        input_count = len(input_iuns)

        logging.info(f"Totale IUN distinti letti da input: {input_count}")

        notification_info_df = run_notification_info_query(spark, input_iuns)
        output_df = build_output_df(notification_info_df)

        found_count = len(output_df)
        annullamento_si_count = (
            int((output_df["annullamento"] == "SI").sum()) if not output_df.empty else 0
        )
        annullamento_no_count = (
            int((output_df["annullamento"] == "NO").sum()) if not output_df.empty else 0
        )

        logging.info(f"IUN con informazioni recuperate: {found_count}")
        logging.info(f"IUN con annullamento SI: {annullamento_si_count}")
        logging.info(f"IUN con annullamento NO: {annullamento_no_count}")

        export_to_sheets(output_df, creds, TAB_DA_ANNULLARE)

        write_tab_log(
            creds=creds,
            max_date_str=max_date_str,
            job_time_utc=job_start_str,
            stato_run="OK",
            input_count=input_count,
            found_count=found_count,
            annullamento_si_count=annullamento_si_count,
            annullamento_no_count=annullamento_no_count,
        )

        slack_msg = (
            f"{slack_prefix}\n"
            f"✅✅✅ *SUCCESS* ✅✅✅\n"
            f"*Job:* {APP_NAME}\n"
            f"*Job started at:* {job_start_str}\n"
            f"*Google Sheet:* {sheet_url}\n"
            f"*Tab output:* {TAB_DA_ANNULLARE}\n"
            f"*Tab log:* {TAB_LOG}\n"
            f"*# IUN letti in input:* {input_count}\n"
            f"*# IUN con informazioni:* {found_count}\n"
            f"*# annullamento SI:* {annullamento_si_count}\n"
            f"*# annullamento NO:* {annullamento_no_count}\n"
        )

        logging.info("Processo completato con successo.")
        send_slack(slack_webhook_url, slack_msg)

    except Exception:
        logging.error("Errore durante il job.", exc_info=True)

        try:
            creds = load_google_credentials(GOOGLE_SECRET_PATH)

            write_tab_log(
                creds=creds,
                max_date_str=max_date_str,
                job_time_utc=job_start_str,
                stato_run="KO",
                input_count=input_count,
                found_count=found_count,
                annullamento_si_count=annullamento_si_count,
                annullamento_no_count=annullamento_no_count,
            )

        except Exception:
            logging.warning("Scrittura tab log KO fallita.", exc_info=True)

        fail_msg = (
            f"{slack_prefix}\n"
            f"❌❌❌ *FAILURE* ❌❌❌\n"
            f"*Job:* {APP_NAME}\n"
            f"*Job started at:* {job_start_str}\n"
            f"*Google Sheet:* {sheet_url}\n"
        )

        try:
            send_slack(slack_webhook_url, fail_msg)

        finally:
            raise

    finally:
        try:
            spark.stop()
            logging.info("Spark session terminata.")
        except Exception:
            logging.warning("Errore spark.stop().")


if __name__ == "__main__":
    main()
