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

APP_NAME = "SSDA-854_Monthly_Notifiche_ACCEPTED"

GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"

SHEET_ID = "1xSoosjBA1HPvlGB9GNg_dCi-RB9wtoHF0htdlCKyub8"

TAB_LOG = "Log di controllo"
TAB_ESTRAZIONE = "SSDA-854_monthly_notifiche_accepted"

SLACK_CONFIG_CANDIDATE_PATHS = [
    "/Slack/notifications_webhook.txt",
    "/app/mount/Slack/notifications_webhook.txt",
    "/app/mount/notifications_webhook.txt",
]

SLACK_WEBHOOK_NAME = "webhook_1"
SLACK_LABEL = "SSDA-854 Monthly Notifiche ACCEPTED"

INITIAL_WS_ROWS = 2
GSHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEETS_CELL_LIMIT = 10_000_000


# ---------------- ECCEZIONI ----------------


class CapacityLimitError(RuntimeError):
    """Errore fatale: limite celle Google Sheets superato."""


class DataQualityError(RuntimeError):
    """Errore di qualità dati."""


# ---------------- FUNZIONI TEMPO ----------------


def now_utc_dt() -> datetime:
    return datetime.now(timezone.utc)


def cumulative_until_previous_month_utc(
    ref_dt: datetime | None = None,
) -> str:

    if ref_dt is None:
        ref_dt = now_utc_dt()

    first_day_current_month = ref_dt.replace(
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )

    cutoff_ts = first_day_current_month.strftime("%Y-%m-%d %H:%M:%S")

    return cutoff_ts


# ---------------- FUNZIONI CREDENZIALI ----------------


def load_google_credentials(secret_path: str) -> dict:

    creds = {
        name: open(os.path.join(secret_path, name)).read().strip()
        for name in os.listdir(secret_path)
        if os.path.isfile(os.path.join(secret_path, name))
    }

    logging.info(f"Credenziali caricate: {list(creds.keys())}")

    return creds


# ---------------- FUNZIONI GSHEET ----------------


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
            raise CapacityLimitError(f"Worksheet oltre limite celle: {would_allocate}")

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


def load_slack_webhooks(
    config_paths: list[str],
) -> dict[str, str]:

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

                    if not key or not value:
                        continue

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


def run_query(
    spark: SparkSession,
    cutoff_ts: str,
):

    logging.info(
        f"Estrazione cumulativa notifiche ACCEPTED " f"con sentat < {cutoff_ts}"
    )

    query = f"""
    WITH institution_base AS (
        SELECT DISTINCT
            internalistitutionid,
            CASE
                WHEN subunittype IS NOT NULL
                    THEN CONCAT(rootparent_description, ' - ', description)
                ELSE description
            END AS denomination,
            taxcode,
            updatedat
        FROM selfcare.gold_contracts
        WHERE product = 'prod-pn'
        AND state = 'ACTIVE'
        AND internalistitutionid IS NOT NULL
        AND taxcode IS NOT NULL
    ),
    institution AS (
        SELECT
            internalistitutionid,
            denomination,
            taxcode
        FROM (
            SELECT
                internalistitutionid,
                denomination,
                taxcode,
                ROW_NUMBER() OVER (
                    PARTITION BY internalistitutionid
                    ORDER BY updatedat DESC
                ) AS rn
            FROM institution_base
        ) t
        WHERE rn = 1
    ),
    iun_base AS (
        SELECT
            n.iun
        FROM send.gold_notification_analytics n
        JOIN institution i
            ON n.senderpaid = i.internalistitutionid
        WHERE n.actual_status = 'ACCEPTED'
        AND n.sentat < CAST('{cutoff_ts}' AS TIMESTAMP)
    ),
    notification_silver AS (
        SELECT
            sn.iun,
            MAX(sn.physicalCommunicationType) AS physicalCommunicationType,
            MAX(sn.subject) AS subject
        FROM send.silver_notification sn
        INNER JOIN iun_base ib
            ON sn.iun = ib.iun
        GROUP BY sn.iun
    ),
    timeline_analog AS (
        SELECT
            t.iun,
            MAX(CAST(get_json_object(t.details, '$.numberOfPages') AS BIGINT)) AS pages,
            MAX(CAST(get_json_object(t.details, '$.envelopeWeight') AS BIGINT)) AS weight
        FROM send.silver_timeline t
        INNER JOIN iun_base ib
            ON t.iun = ib.iun
        WHERE t.category = 'SEND_ANALOG_DOMICILE'
        GROUP BY t.iun
    ),
    payment_timeline AS (
        SELECT
            t.iun,
            MAX(CAST(get_json_object(t.details, '$.eventTimestamp') AS TIMESTAMP)) AS tms_timeline_payment
        FROM send.silver_timeline t
        INNER JOIN iun_base ib
            ON t.iun = ib.iun
        WHERE t.category = 'PAYMENT'
        GROUP BY t.iun
    ),
    radd_iun AS (
        SELECT
            t.iun,
            1 AS flag_radd
        FROM send.silver_timeline t
        INNER JOIN iun_base ib
            ON t.iun = ib.iun
        WHERE t.category = 'AAR_CREATION_REQUEST'
        AND get_json_object(t.details, '$.aarTemplateType') = 'AAR_NOTIFICATION_RADD_ALT'
        GROUP BY t.iun
    )
    SELECT
        n.iun,
        n.sentat,
        n.senderpaid,
        i.denomination AS senderdenomination,
        sn.physicalCommunicationType,
        sn.subject,
        n.taxonomycode,
        n.zip_code AS cap,
        t.pages,
        t.weight,
        pt.tms_timeline_payment,
        CASE
            WHEN pt.iun IS NOT NULL THEN 1
            ELSE 0
        END AS flag_pagamento_timeline,
        CASE
            WHEN r.iun IS NOT NULL THEN 1
            ELSE 0
        END AS flag_radd,
        CONCAT(
            'Notifiche in stato ACCEPTED pregresse con sentat < ',
            '{cutoff_ts}'
        ) AS casistica_cluster
    FROM send.gold_notification_analytics n
    JOIN institution i
        ON n.senderpaid = i.internalistitutionid
    LEFT JOIN notification_silver sn
        ON n.iun = sn.iun
    LEFT JOIN timeline_analog t
        ON n.iun = t.iun
    LEFT JOIN payment_timeline pt
        ON n.iun = pt.iun
    LEFT JOIN radd_iun r
        ON n.iun = r.iun
    WHERE n.actual_status = 'ACCEPTED'
    AND n.sentat < CAST('{cutoff_ts}' AS TIMESTAMP)
    """

    df = spark.sql(query)

    logging.info("DataFrame estrazione costruito.")

    return df


def validate_granularity_iun(
    df_spark,
) -> tuple[int, int]:

    logging.info("Validazione granularità dataset...")

    df_spark.createOrReplaceTempView("tmp_output_accepted")

    check_df = df_spark.sql_ctx.sql("""
        SELECT
            COUNT(*) AS righe_totali,
            COUNT(DISTINCT iun) AS iun_distinti
        FROM tmp_output_accepted
        """)

    row = check_df.collect()[0]

    righe_totali = int(row["righe_totali"])
    iun_distinti = int(row["iun_distinti"])

    if righe_totali != iun_distinti:
        raise DataQualityError(
            f"Granularità non rispettata: "
            f"righe={righe_totali}, "
            f"iun={iun_distinti}"
        )

    logging.info(f"Granularità OK. Righe={righe_totali}, IUN={iun_distinti}")

    return righe_totali, iun_distinti


def get_last_update_date(
    spark: SparkSession,
) -> str:

    logging.info("Estrazione ultimo aggiornamento...")

    max_date_df = spark.sql("""
        SELECT
            MAX(sentat) AS max_ts
        FROM send.gold_notification_analytics
        """)

    max_date = max_date_df.collect()[0]["max_ts"]

    if isinstance(max_date, datetime):
        return max_date.strftime("%Y-%m-%d %H:%M:%S")

    return str(max_date) if max_date is not None else "-"


# ---------------- LOG ----------------


def build_log_df(
    max_date_str: str,
    start_run_str: str,
    stato_run: str = "IN CORSO",
    righe_output: int = 0,
    iun_distinti: int = 0,
) -> pd.DataFrame:

    df = pd.DataFrame(
        {
            "Ultimo aggiornamento dati (MAX sentat)": [max_date_str],
            "Start run time (UTC)": [start_run_str],
            "Stato run": [stato_run],
            "Righe output": [int(righe_output)],
            "IUN distinti": [int(iun_distinti)],
        }
    )

    return sanitize_for_sheets(df)


def write_tab_log(
    creds: dict,
    max_date_str: str,
    start_run_str: str,
    stato_run: str = "IN CORSO",
    righe_output: int = 0,
    iun_distinti: int = 0,
):

    log_df = build_log_df(
        max_date_str=max_date_str,
        start_run_str=start_run_str,
        stato_run=stato_run,
        righe_output=righe_output,
        iun_distinti=iun_distinti,
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

    cutoff_ts = cumulative_until_previous_month_utc(job_start_dt)

    slack_prefix = build_slack_prefix()

    slack_webhooks = load_slack_webhooks(SLACK_CONFIG_CANDIDATE_PATHS)

    slack_webhook_url = get_slack_webhook_by_name(
        slack_webhooks,
        SLACK_WEBHOOK_NAME,
    )

    max_date_str = "-"
    righe_output = 0
    iun_distinti = 0

    base_df = None

    try:
        creds = load_google_credentials(GOOGLE_SECRET_PATH)

        max_date_str = get_last_update_date(spark)

        write_tab_log(
            creds=creds,
            max_date_str=max_date_str,
            start_run_str=job_start_str,
            stato_run="IN CORSO",
            righe_output=0,
            iun_distinti=0,
        )

        base_df = run_query(
            spark=spark,
            cutoff_ts=cutoff_ts,
        ).persist(StorageLevel.MEMORY_AND_DISK)

        logging.info("Materializzazione DataFrame...")

        righe_output, iun_distinti = validate_granularity_iun(base_df)

        output_df = base_df.orderBy(
            "sentat",
            "iun",
        )

        spark_to_sheets(
            df_spark=output_df,
            creds=creds,
            sheet_name=TAB_ESTRAZIONE,
            missing="-",
        )

        write_tab_log(
            creds=creds,
            max_date_str=max_date_str,
            start_run_str=job_start_str,
            stato_run="OK",
            righe_output=righe_output,
            iun_distinti=iun_distinti,
        )

        slack_msg = (
            f"{slack_prefix}\n"
            f"✅✅✅ *SUCCESS* ✅✅✅\n"
            f"*Job:* {APP_NAME}\n"
            f"*RunId:* {run_id}\n"
            f"*Host:* {hostname}\n"
            f"*Perimetro estrazione:* notifiche ACCEPTED con sentat < {cutoff_ts}\n"
            f"*Righe output:* {righe_output}\n"
            f"*IUN distinti:* {iun_distinti}\n"
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
                max_date_str=max_date_str,
                start_run_str=job_start_str,
                stato_run="KO",
                righe_output=righe_output,
                iun_distinti=iun_distinti,
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
            f"*Perimetro estrazione:* notifiche ACCEPTED con sentat < {cutoff_ts}\n"
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
