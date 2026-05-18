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
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

APP_NAME = "SSDA-476_Estrazione_Key_Client"

GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"
SHEET_ID = "1lUXRHPi1TajWOMxWhDO_kef7eSENjjXMwQWjDzBND5c"

TAB_ESTRAZIONE = "weekly SSDA-476"
TAB_LOG = "Log di controllo"

SLACK_CONFIG_CANDIDATE_PATHS = [
    "/Slack/notifications_webhook.txt",
    "/app/mount/Slack/notifications_webhook.txt",
    "/app/mount/notifications_webhook.txt",
]

SLACK_WEBHOOK_NAME = "webhook_1"
SLACK_LABEL = "SSDA-476 Key Client"

INITIAL_WS_ROWS = 2
GSHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEETS_CELL_LIMIT = 10_000_000


# ---------------- ECCEZIONI ----------------


class CapacityLimitError(RuntimeError):
    """Errore fatale: limite celle Google Sheets superato."""


# ---------------- TEMPO ----------------


def now_utc_dt() -> datetime:
    return datetime.now(timezone.utc)


# ---------------- CREDENZIALI ----------------


def load_google_credentials(secret_path: str) -> dict:
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
    return Sheet(
        sheet_id=SHEET_ID,
        service_credentials=creds,
        id_mode="key",
    )


def sanitize_for_sheets(
    df: pd.DataFrame,
    missing: str = "-",
) -> pd.DataFrame:
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
            creds,
            scopes=GSHEETS_SCOPES,
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
                f"Worksheet '{worksheet_name}' oltre limite celle: {would_allocate}"
            )

        if final_rows == current_rows and final_cols == current_cols:
            logging.info(f"Resize non necessario per '{worksheet_name}'")
            return

        logging.info(
            f"Resize worksheet '{worksheet_name}' "
            f"rows {current_rows}->{final_rows}, "
            f"cols {current_cols}->{final_cols}"
        )

        ws.resize(rows=final_rows, cols=final_cols)

    except CapacityLimitError:
        raise

    except Exception as e:
        logging.warning(f"Resize fallito per '{worksheet_name}'. Dettaglio: {e}")


def export_to_sheets(
    df: pd.DataFrame,
    creds: dict,
    sheet_name: str,
    missing: str = "-",
) -> pd.DataFrame:
    logging.info(f"Scrittura tab '{sheet_name}'...")

    df = sanitize_for_sheets(df, missing=missing)

    target_rows = INITIAL_WS_ROWS if df.empty else len(df) + 1
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

    logging.info(
        f"Tab '{sheet_name}' scritta correttamente. "
        f"Righe={len(df)} Colonne={len(df.columns)}"
    )

    return df


def spark_to_sheets(
    df_spark,
    creds: dict,
    sheet_name: str,
    missing: str = "-",
) -> pd.DataFrame:
    if df_spark is None:
        logging.warning(f"DataFrame Spark nullo per '{sheet_name}'")
        return pd.DataFrame()

    logging.info(f"Conversione Spark -> Pandas per '{sheet_name}'...")
    df = df_spark.toPandas()
    logging.info(f"Conversione completata. Righe={len(df)}")

    return export_to_sheets(
        df=df,
        creds=creds,
        sheet_name=sheet_name,
        missing=missing,
    )


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


# ---------------- QUERY ----------------


def run_query(spark: SparkSession):
    logging.info("Esecuzione query Key Client...")

    query_sql = """
    WITH postalizzazione_pruned AS (
        SELECT
            g.senderpaid,
            g.codice_oggetto,
            g.recapitista_unificato,
            g.fine_recapito_stato,
            g.flag_wi7_report_postalizzazioni_incomplete,
            COALESCE(
                LEAST(
                    g.accettazione_recapitista_con018_data,
                    g.affido_recapitista_con016_data
                ),
                g.accettazione_recapitista_con018_data,
                g.affido_recapitista_con016_data,
                g.requesttimestamp
            ) AS affido_accettazione_rec_data
        FROM send.gold_postalizzazione_analytics g
        LEFT ANTI JOIN (
            SELECT DISTINCT requestid
            FROM send_dev.wi7_poste_da_escludere
            WHERE requestid IS NOT NULL
        ) w ON g.requestid = w.requestid
        WHERE g.statusrequest NOT IN ('PN999', 'PN998')
        AND g.prodotto NOT IN ('RIS', 'RIR')
        AND COALESCE(
                LEAST(
                    g.accettazione_recapitista_con018_data,
                    g.affido_recapitista_con016_data
                ),
                g.accettazione_recapitista_con018_data,
                g.affido_recapitista_con016_data,
                g.requesttimestamp
            ) IS NOT NULL
    ),
    postalizzazione_base AS (
        SELECT
            senderpaid,
            YEAR(affido_accettazione_rec_data) AS anno,
            CASE
                WHEN codice_oggetto LIKE 'R14%' THEN 'Fulmine'
                WHEN codice_oggetto LIKE '777%'
                  OR codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
                WHEN codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
                WHEN codice_oggetto LIKE '69%'
                  OR codice_oggetto LIKE '381%'
                  OR codice_oggetto LIKE 'RB1%' THEN 'Poste'
                ELSE recapitista_unificato
            END AS recapitista_unif,
            fine_recapito_stato,
            flag_wi7_report_postalizzazioni_incomplete
        FROM postalizzazione_pruned
    ),
    vista_conteggio AS (
        SELECT
            senderpaid,
            anno,
            recapitista_unif,
            COUNT(*) AS totale_affidi,
            SUM(CASE WHEN fine_recapito_stato IS NOT NULL THEN 1 ELSE 0 END) AS completate,
            SUM(CASE WHEN fine_recapito_stato IS NULL THEN 1 ELSE 0 END) AS totale_in_corso,
            SUM(
                CASE
                    WHEN fine_recapito_stato IS NULL
                    AND COALESCE(flag_wi7_report_postalizzazioni_incomplete, 0) = 0
                    THEN 1
                    ELSE 0
                END
            ) AS in_corso,
            SUM(
                CASE
                    WHEN COALESCE(flag_wi7_report_postalizzazioni_incomplete, 0) = 1
                    THEN 1
                    ELSE 0
                END
            ) AS totale_fuori_sla
        FROM postalizzazione_base
        GROUP BY
            senderpaid,
            anno,
            recapitista_unif
    ),
    selfcare_view AS (
        SELECT
            internalistitutionid AS paid,
            denomination AS senderdenomination
        FROM (
            SELECT
                c.internalistitutionid,
                CASE
                    WHEN c.subunittype IS NOT NULL
                        THEN CONCAT(c.rootparent_description, ' - ', c.description)
                    ELSE c.description
                END AS denomination,
                ROW_NUMBER() OVER (
                    PARTITION BY c.internalistitutionid
                    ORDER BY c.updatedat DESC
                ) AS rn
            FROM selfcare.gold_contracts c
            WHERE c.product = 'prod-pn'
            AND c.state = 'ACTIVE'
            AND c.internalistitutionid IS NOT NULL
        ) t
        WHERE rn = 1
    )
    SELECT
        v.senderpaid,
        s.senderdenomination,
        v.anno,
        v.recapitista_unif AS recapitista,
        v.totale_affidi,
        v.completate,
        v.totale_in_corso,
        v.in_corso,
        v.totale_fuori_sla
    FROM vista_conteggio v
    LEFT JOIN selfcare_view s
        ON v.senderpaid = s.paid
    WHERE v.totale_in_corso >= 1000
    ORDER BY
        v.totale_fuori_sla DESC,
        v.totale_in_corso DESC,
        v.senderpaid,
        v.anno,
        v.recapitista_unif
    """

    df = spark.sql(query_sql)
    logging.info("DataFrame estrazione costruito.")
    return df


def get_last_update_date(spark: SparkSession) -> str:
    logging.info("Estrazione ultimo aggiornamento gold_postalizzazione...")

    max_date_df = spark.sql("""
        SELECT
            MAX(requesttimestamp) AS max_ts
        FROM send.gold_postalizzazione_analytics
    """)

    max_date = max_date_df.collect()[0]["max_ts"]

    if isinstance(max_date, datetime):
        return max_date.strftime("%Y-%m-%d %H:%M:%S")

    return str(max_date) if max_date is not None else "-"


# ---------------- LOG ----------------


def build_log_df(
    max_requesttimestamp_str: str,
    start_run_str: str,
    stato_run: str = "IN CORSO",
    righe_output: int = 0,
    colonne_output: int = 0,
) -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "Data esecuzione script (UTC)": [start_run_str],
            "Ultimo aggiornamento dati (MAX requesttimestamp)": [
                max_requesttimestamp_str
            ],
            "Stato run": [stato_run],
            "Righe output": [int(righe_output)],
            "Colonne output": [int(colonne_output)],
        }
    )

    return sanitize_for_sheets(df)


def write_tab_log(
    creds: dict,
    max_requesttimestamp_str: str,
    start_run_str: str,
    stato_run: str = "IN CORSO",
    righe_output: int = 0,
    colonne_output: int = 0,
):
    log_df = build_log_df(
        max_requesttimestamp_str=max_requesttimestamp_str,
        start_run_str=start_run_str,
        stato_run=stato_run,
        righe_output=righe_output,
        colonne_output=colonne_output,
    )

    export_to_sheets(
        df=log_df,
        creds=creds,
        sheet_name=TAB_LOG,
    )


# ---------------- MAIN ----------------


def main():
    logging.info("Inizializzazione SparkSession...")

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    job_start_dt = now_utc_dt()
    job_start_str = job_start_dt.strftime("%Y-%m-%d %H:%M:%S")
    run_id = job_start_dt.strftime("%Y%m%dT%H%M%SZ")
    hostname = socket.gethostname()

    sheet_url = build_google_sheet_url(SHEET_ID)

    logging.info(f"RunId: {run_id}")
    logging.info(f"Host: {hostname}")
    logging.info(f"Google Sheet output: {sheet_url}")
    logging.info(f"Tab estrazione: {TAB_ESTRAZIONE}")
    logging.info(f"Tab log: {TAB_LOG}")

    slack_prefix = build_slack_prefix()

    slack_webhooks = load_slack_webhooks(SLACK_CONFIG_CANDIDATE_PATHS)
    slack_webhook_url = get_slack_webhook_by_name(
        slack_webhooks,
        SLACK_WEBHOOK_NAME,
    )

    max_requesttimestamp_str = "-"
    righe_output = 0
    colonne_output = 0

    base_df = None

    try:
        creds = load_google_credentials(GOOGLE_SECRET_PATH)

        max_requesttimestamp_str = get_last_update_date(spark)

        write_tab_log(
            creds=creds,
            max_requesttimestamp_str=max_requesttimestamp_str,
            start_run_str=job_start_str,
            stato_run="IN CORSO",
            righe_output=0,
            colonne_output=0,
        )

        base_df = run_query(spark).persist(StorageLevel.MEMORY_AND_DISK)

        logging.info("Materializzazione DataFrame e calcolo metriche output...")

        righe_output = base_df.count()
        colonne_output = len(base_df.columns)

        logging.info(
            f"Metriche output calcolate. "
            f"Righe={righe_output}, Colonne={colonne_output}"
        )

        output_df = base_df.orderBy(
            "totale_fuori_sla",
            "totale_in_corso",
            ascending=False,
        )

        logging.info(
            f"Avvio export principale verso Google Sheet. "
            f"URL={sheet_url}, Tab={TAB_ESTRAZIONE}, "
            f"Righe={righe_output}, Colonne={colonne_output}"
        )

        spark_to_sheets(
            df_spark=output_df,
            creds=creds,
            sheet_name=TAB_ESTRAZIONE,
            missing="-",
        )

        logging.info(f"Export principale completato. Google Sheet: {sheet_url}")

        write_tab_log(
            creds=creds,
            max_requesttimestamp_str=max_requesttimestamp_str,
            start_run_str=job_start_str,
            stato_run="OK",
            righe_output=righe_output,
            colonne_output=colonne_output,
        )

        slack_msg = (
            f"{slack_prefix}\n"
            f"✅✅✅ *SUCCESS* ✅✅✅\n"
            f"*Job:* {APP_NAME}\n"
            f"*RunId:* {run_id}\n"
            f"*Host:* {hostname}\n"
            f"*Google Sheet:* {sheet_url}\n"
            f"*Tab estrazione:* {TAB_ESTRAZIONE}\n"
            f"*Tab log:* {TAB_LOG}\n"
            f"*MAX requesttimestamp:* {max_requesttimestamp_str}\n"
            f"*Righe output:* {righe_output}\n"
            f"*Colonne output:* {colonne_output}\n"
        )

        logging.info("Processo completato con successo.")

        send_slack(
            slack_webhook_url,
            slack_msg,
        )

    except Exception:

        logging.error(
            "Errore durante il job.",
            exc_info=True,
        )

        try:
            creds = load_google_credentials(GOOGLE_SECRET_PATH)

            write_tab_log(
                creds=creds,
                max_requesttimestamp_str=max_requesttimestamp_str,
                start_run_str=job_start_str,
                stato_run="KO",
                righe_output=righe_output,
                colonne_output=colonne_output,
            )

        except Exception:
            logging.warning(
                "Scrittura tab log KO fallita.",
                exc_info=True,
            )

        fail_msg = (
            f"{slack_prefix}\n"
            f"❌❌❌ *FAILURE* ❌❌❌\n"
            f"*Job:* {APP_NAME}\n"
            f"*RunId:* {run_id}\n"
            f"*Host:* {hostname}\n"
            f"*Google Sheet:* {sheet_url}\n"
            f"*MAX requesttimestamp:* {max_requesttimestamp_str}\n"
        )

        try:
            send_slack(
                slack_webhook_url,
                fail_msg,
            )

        finally:
            raise

    finally:
        try:
            if base_df is not None:
                base_df.unpersist(blocking=True)
                logging.info("base_df rilasciato.")

        except Exception:
            logging.warning("Errore unpersist base_df.")

        try:
            spark.stop()
            logging.info("Spark session terminata.")

        except Exception:
            logging.warning("Errore spark.stop().")


if __name__ == "__main__":
    main()
