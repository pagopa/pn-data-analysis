import json
import logging
import os
import socket
import urllib.request
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from pdnd_google_utils import Sheet
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

# ---------------- CONFIGURAZIONE BASE ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

APP_NAME = "SSDA-510 Rendicontazione corretta da perfezionare"
GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"
SHEET_ID = "10zzJXlXct5fpNqLv_YOEAY6fvdkrFAbL9SUld3qx7ZI"

TAB_META = "Data aggiornamento"
TAB_OUTPUT = "Delta weekly SSDA-510"

SLACK_CONFIG_CANDIDATE_PATHS = [
    "/Slack/notifications_webhook.txt",
    "/app/mount/Slack/notifications_webhook.txt",
    "/app/mount/notifications_webhook.txt",
]
SLACK_WEBHOOK_NAME = "webhook_1"
SLACK_LABEL = "SSDA-510"


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


# ---------------- FUNZIONI GENERALI ----------------
def load_google_credentials(secret_path: str) -> dict:
    creds = {
        name: open(os.path.join(secret_path, name)).read().strip()
        for name in os.listdir(secret_path)
        if os.path.isfile(os.path.join(secret_path, name))
    }
    logging.info(f"Credenziali caricate: {list(creds.keys())}")
    return creds


def to_pandas_for_sheets(
    df_spark=None, df_pd: pd.DataFrame | None = None, missing: str = "-"
) -> pd.DataFrame:
    """
    Prepara un dataframe per Google Sheets.
    Accetta:
      - df_spark: converte con toPandas()
      - df_pd: sanitizza direttamente un pandas DataFrame
    Normalizza inf/-inf, NaN/NaT, e converte tutto a stringa.
    """
    if df_spark is not None:
        df = df_spark.toPandas()
    elif df_pd is not None:
        df = df_pd.copy()
    else:
        df = pd.DataFrame()

    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df = df.fillna(missing)
    df = df.astype(object).where(pd.notna(df), missing)
    df = df.astype(str)
    return df


def export_to_sheets(df: pd.DataFrame, creds: dict, sheet_id: str, sheet_name: str):
    logging.info(f"Scrittura su Google Sheet: {sheet_name}")
    df = to_pandas_for_sheets(df_pd=df)
    sheet = Sheet(sheet_id=sheet_id, service_credentials=creds, id_mode="key")
    sheet.upload(sheet_name, df)
    logging.info("Scrittura completata.")


def get_last_update_date(spark: SparkSession) -> str:
    logging.info("Estrazione data ultimo aggiornamento...")
    max_date_df = spark.sql(
        "SELECT MAX(requesttimestamp) AS max_ts FROM send.gold_postalizzazione_analytics"
    )
    max_date = max_date_df.collect()[0]["max_ts"]

    if isinstance(max_date, datetime):
        return max_date.strftime("%Y-%m-%d %H:%M:%S")
    return str(max_date)


def read_previous_week(sheet: Sheet) -> pd.DataFrame:
    logging.info(f"Lettura tab precedente '{TAB_OUTPUT}'...")
    try:
        df_old = sheet.download(TAB_OUTPUT)
        if df_old is None or df_old.empty:
            logging.info("Tab precedente vuoto o non valorizzato.")
            return pd.DataFrame()
        df_old = to_pandas_for_sheets(df_pd=df_old)
        logging.info(f"Righe settimana precedente lette: {df_old.shape[0]}")
        return df_old
    except Exception as e:
        logging.warning(
            f"Impossibile leggere il tab '{TAB_OUTPUT}'. Procedo come se fosse vuoto. Dettaglio: {e}"
        )
        return pd.DataFrame()


def build_previous_requestids_df(spark: SparkSession, df_old: pd.DataFrame):
    if df_old.empty or "requestid" not in df_old.columns:
        return spark.createDataFrame([], "requestid string")

    requestids = (
        df_old["requestid"]
        .astype(str)
        .str.strip()
        .replace("-", np.nan)
        .dropna()
        .drop_duplicates()
        .tolist()
    )

    if not requestids:
        return spark.createDataFrame([], "requestid string")

    return spark.createDataFrame([(x,) for x in requestids], ["requestid"])


def build_meta_df(
    max_date_str: str,
    start_run_str: str,
    end_run_str: str,
    elapsed_min_str: str,
    stato_run: str,
    righe_settimana_precedente: int,
    oggetti_comuni_settimana_precedente: int,
    righe_settimana_corrente: int,
    count_pre_distinct: int,
    count_post_distinct: int,
) -> pd.DataFrame:
    return to_pandas_for_sheets(
        df_pd=pd.DataFrame(
            {
                "Ultimo aggiornamento dati (MAX requesttimestamp)": [max_date_str],
                "Start run time (UTC)": [start_run_str],
                "End run time (UTC)": [end_run_str],
                "Tempo impiegato (min)": [elapsed_min_str],
                "Stato run": [stato_run],
                "Righe settimana precedente": [righe_settimana_precedente],
                "Count pre distinct": [count_pre_distinct],
                "Count post distinct": [count_post_distinct],
                "Oggetti già presentati": [oggetti_comuni_settimana_precedente],
                "Righe settimana corrente": [righe_settimana_corrente],
            }
        )
    )


def write_meta_tab(
    creds: dict,
    max_date_str: str,
    start_run_str: str,
    end_run_str: str,
    elapsed_min_str: str,
    stato_run: str,
    righe_settimana_precedente: int,
    oggetti_comuni_settimana_precedente: int,
    righe_settimana_corrente: int,
    count_pre_distinct: int,
    count_post_distinct: int,
):
    df_meta = build_meta_df(
        max_date_str=max_date_str,
        start_run_str=start_run_str,
        end_run_str=end_run_str,
        elapsed_min_str=elapsed_min_str,
        stato_run=stato_run,
        righe_settimana_precedente=righe_settimana_precedente,
        oggetti_comuni_settimana_precedente=oggetti_comuni_settimana_precedente,
        righe_settimana_corrente=righe_settimana_corrente,
        count_pre_distinct=count_pre_distinct,
        count_post_distinct=count_post_distinct,
    )
    export_to_sheets(df_meta, creds, SHEET_ID, TAB_META)


# ---------------- SLACK ----------------
def load_slack_webhooks(config_paths: list[str]) -> dict[str, str]:
    """
    Legge uno o più webhook da file di configurazione.
    Formato atteso:
        nome_webhook = https://...
    Ignora:
      - righe vuote
      - commenti che iniziano con '#'
    In caso di file non trovato o non leggibile, restituisce dizionario vuoto.
    """
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


def get_slack_webhooks_by_names(
    webhooks: dict[str, str], webhook_names: list[str]
) -> list[str]:
    urls = []

    for webhook_name in webhook_names:
        webhook_url = get_slack_webhook_by_name(webhooks, webhook_name)
        if webhook_url:
            urls.append(webhook_url)

    return urls


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


# ---------------- QUERY ----------------
def build_query_sql() -> str:
    query = """
    WITH gold_base AS (
        SELECT *
        FROM send.gold_postalizzazione_analytics s
        WHERE s.scarto_consolidatore_stato IS NULL
          AND s.flag_prodotto_estero = 0
          AND s.statusrequest NOT IN (
                'PN999', 'PN998', 'error', 'internalError', 'syntaxError',
                'transformationError', 'semanticError', 'authenticationError', 'duplicatedRequest'
          )
          AND s.attempt_rank = 1
          AND s.pcretry_rank = 1
          AND s.tms_cancelled IS NULL
          AND (
                s.certificazione_recapito_stato NOT IN ('RECRS006','RECRS013','RECRN006','RECRN013','RECAG004','RECAG013')
                OR s.certificazione_recapito_stato IS NULL
          )
          AND COALESCE(s.tentativo_recapito_stato, '') NOT IN ('PN998','PN999')
          AND COALESCE(s.certificazione_recapito_stato, '') NOT IN ('PN998','PN999')
          AND COALESCE(s.fine_recapito_stato, '') NOT IN ('PN998','PN999')
          AND (
                s.certificazione_recapito_dettagli NOT IN ('M02')
                OR s.certificazione_recapito_dettagli IS NULL
          )
    ),
    temp_demat AS (
        SELECT
            REGEXP_REPLACE(p.requestid, '^pn-cons-000~', '') AS requestid,
            a.documenttype AS documenttype,
            ROW_NUMBER() OVER (
                PARTITION BY REGEXP_REPLACE(p.requestid, '^pn-cons-000~', '')
                ORDER BY e.paperprogrstatus.clientrequesttimestamp DESC
            ) AS rn_demat
        FROM send.silver_postalizzazione p
        LATERAL VIEW explode(p.eventslist) ev AS e
        LATERAL VIEW explode(e.paperprogrstatus.attachments) av AS a
        WHERE a.documenttype IN ('23L', 'AR')
    ),

    temp_silver_postalizzazione AS (
        SELECT
            REGEXP_REPLACE(t.requestid, '^pn-cons-000~', '') AS requestid,
            e.paperprogrstatus.statuscode AS statuscode,
            CASE
                WHEN LENGTH(e.paperprogrstatus.statusdatetime) = 17 THEN CONCAT(
                    SUBSTR(e.paperprogrstatus.statusdatetime, 1, 16),
                    ':00Z'
                )
                ELSE e.paperprogrstatus.statusdatetime
            END AS statusdatetime,
            CASE
                WHEN LENGTH(e.paperprogrstatus.clientrequesttimestamp) = 17 THEN CONCAT(
                    SUBSTR(e.paperprogrstatus.clientrequesttimestamp, 1, 16),
                    ':00.000Z'
                )
                ELSE e.paperprogrstatus.clientrequesttimestamp
            END AS clientrequesttimestamp,
            CASE
                WHEN e.paperprogrstatus.statuscode IN (
                    'RECRN003C','RECRN004C','RECRN005C','RECRN001C','RECRN002C','RECRN002F','RECAG001C',
                    'RECAG002C','RECAG003C','RECAG003F','RECAG005C','RECAG006C','RECAG007C','RECAG008C'
                ) THEN 'FINE_RECAPITO'
                WHEN e.paperprogrstatus.statuscode IN (
                    'RECRN003A','RECRN004A','RECRN005A','RECRN001A','RECRN002A','RECRN002D','RECAG001A',
                    'RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A','RECAG007A','RECAG008A'
                ) THEN 'CERTIFICAZIONE_RECAPITO'
                ELSE NULL
            END AS tipo,
            ROW_NUMBER() OVER (
                PARTITION BY
                    REGEXP_REPLACE(t.requestid, '^pn-cons-000~', ''),
                    CASE
                        WHEN e.paperprogrstatus.statuscode IN (
                            'RECRN003C','RECRN004C','RECRN005C','RECRN001C','RECRN002C','RECRN002F','RECAG001C',
                            'RECAG002C','RECAG003C','RECAG003F','RECAG005C','RECAG006C','RECAG007C','RECAG008C'
                        ) THEN 'FINE_RECAPITO'
                        WHEN e.paperprogrstatus.statuscode IN (
                            'RECRN003A','RECRN004A','RECRN005A','RECRN001A','RECRN002A','RECRN002D','RECAG001A',
                            'RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A','RECAG007A','RECAG008A'
                        ) THEN 'CERTIFICAZIONE_RECAPITO'
                    END
                ORDER BY e.paperprogrstatus.clientrequesttimestamp DESC
            ) AS rn
        FROM send.silver_postalizzazione t
        LATERAL VIEW explode(t.eventslist) ev AS e
        WHERE e.paperprogrstatus.statuscode IN (
            'RECRN001A','RECRN002A','RECRN002D','RECRN003A','RECRN004A','RECRN005A','RECAG001A','RECAG002A',
            'RECAG003A','RECAG003D','RECAG005A','RECAG006A','RECAG007A','RECAG008A','RECRN001C','RECRN002C',
            'RECRN002F','RECRN003C','RECRN004C','RECRN005C','RECAG001C','RECAG002C','RECAG003C','RECAG003F',
            'RECAG005C','RECAG006C','RECAG007C','RECAG008C'
        )
    ),

    temp_ultimi_eventi_silver_postalizzazione AS (
        SELECT
            requestid,
            MAX(CASE WHEN tipo = 'FINE_RECAPITO' THEN statuscode END) AS fine_recapito_stato,
            MAX(CASE WHEN tipo = 'FINE_RECAPITO' THEN statusdatetime END) AS fine_recapito_data,
            MAX(CASE WHEN tipo = 'FINE_RECAPITO' THEN clientrequesttimestamp END) AS fine_recapito_rendicontazione,
            MAX(CASE WHEN tipo = 'CERTIFICAZIONE_RECAPITO' THEN statuscode END) AS certificazione_recapito_stato,
            MAX(CASE WHEN tipo = 'CERTIFICAZIONE_RECAPITO' THEN statusdatetime END) AS certificazione_recapito_data,
            MAX(CASE WHEN tipo = 'CERTIFICAZIONE_RECAPITO' THEN clientrequesttimestamp END) AS certificazione_recapito_rendicontazione
        FROM temp_silver_postalizzazione
        WHERE rn = 1
        GROUP BY requestid
    ),

    temp_analog_attempt AS (
        SELECT DISTINCT
            iun,
            CAST(regexp_extract(timelineelementid, 'ATTEMPT_([0-9]+)', 1) AS INT) AS attempt_number_timeline
        FROM send.silver_timeline
        WHERE category = 'SEND_ANALOG_FEEDBACK'
    ),

    temp_controlli AS (
        SELECT
            s.senderpaid,
            sn.senderdenomination,
            s.iun,
            s.requestid,
            s.requesttimestamp,
            s.prodotto,
            s.geokey,
            CASE
                WHEN s.codice_oggetto LIKE 'R14%' THEN 'Fulmine'
                WHEN s.codice_oggetto LIKE '777%' OR s.codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
                WHEN s.codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
                WHEN s.codice_oggetto LIKE '69%' OR s.codice_oggetto LIKE '381%' OR s.codice_oggetto LIKE 'RB1%' THEN 'Poste'
                ELSE s.recapitista_unificato
            END AS recapitista_unif,
            s.lotto,
            s.codice_oggetto,
            CONCAT("'", s.codice_oggetto) AS codiceOggetto,
            s.scarto_consolidatore_stato,
            s.scarto_consolidatore_data,
            s.affido_recapitista_con016_data,
            s.accettazione_recapitista_con018_data,
            CASE
                WHEN s.accettazione_recapitista_CON018_data IS NOT NULL THEN s.accettazione_recapitista_CON018_data
                WHEN s.affido_recapitista_con016_data IS NOT NULL THEN s.affido_recapitista_con016_data
                ELSE s.requesttimestamp
            END AS affido_accettazione_rec_data,
            s.data_deposito,
            s.tentativo_recapito_stato,
            s.tentativo_recapito_data,
            s.tentativo_recapito_data_rendicontazione,
            s.messaingiacenza_recapito_stato,
            s.messaingiacenza_recapito_data,
            s.messaingiacenza_recapito_data_rendicontazione,
            s.certificazione_recapito_stato,
            s.certificazione_recapito_dettagli,
            s.certificazione_recapito_data,
            s.certificazione_recapito_data_rendicontazione,
            s.demat_23l_ar_stato,
            s.demat_23l_ar_data_rendicontazione,
            s.demat_plico_stato,
            s.demat_plico_data_rendicontazione,
            s.demat_arcad_stato,
            s.demat_arcad_data_rendicontazione,
            s.fine_recapito_stato,
            s.fine_recapito_data,
            s.fine_recapito_data_rendicontazione,
            s.accettazione_23l_recag012_data,
            s.accettazione_23l_recag012_data_rendicontazione,
            s.recindex_number,
            s.perfezionamento_data,
            s.perfezionamento_tipo,
            s.perfezionamento_notificationdate,
            s.perfezionamento_stato,
            s.perfezionamento_stato_dettagli,
            s.ultimo_evento_stato,
            LEAST(
                COALESCE(n.tms_viewed, n.tms_effective_date),
                COALESCE(n.tms_effective_date, n.tms_viewed)
            ) AS tms_perfezionamento_notification,
            CASE
                WHEN tl.category = 'SCHEDULE_REFINEMENT' THEN 1
                ELSE 0
            END AS flag_schedule_refinement,
            CASE
                WHEN a.attempt_number_timeline = 0 AND CAST(s.attempt_number AS INT) = 0 THEN 1
                ELSE 0
            END AS flag_feedback_attempt_0,
            s.tms_cancelled,
            n.tms_date_payment,
            s.flag_wi7_consolidatore,
            s.flag_wi7_report_postalizzazioni_incomplete,
            s.wi7_cluster,
            s.pcretry_rank,
            s.attempt_rank,
            t.certificazione_recapito_stato AS certificazione_recapito_stato_silver,
            t.certificazione_recapito_data AS certificazione_recapito_data_silver,
            t.certificazione_recapito_rendicontazione AS certificazione_recapito_rendicontazione_silver,
            t.fine_recapito_stato AS fine_recapito_stato_silver,
            t.fine_recapito_data AS fine_recapito_data_silver,
            t.fine_recapito_rendicontazione AS fine_recapito_rendicontazione_silver,
            d.documenttype,
            CASE WHEN n.type_notif = 'MULTI' THEN 1 ELSE 0 END AS flag_multi_destinatario,
            CASE
                WHEN s.attempt_rank = 1 AND s.pcretry_rank = 1 THEN 1
                ELSE 0
            END AS flag_ultima_postalizzazione,
            s.flag_prodotto_estero,
            CASE
                WHEN s.certificazione_recapito_dettagli IN ('M02') THEN 1
                ELSE 0
            END AS flag_destinatario_deceduto,
            CASE
                WHEN s.flag_wi7_report_postalizzazioni_incomplete = 1 THEN 1
                ELSE 0
            END AS flag_fuori_sla,
            CASE
                WHEN s.certificazione_recapito_stato NOT IN ('RECRS002A','RECRN002A','RECAG003A','RECRS002D','RECRN002D','RECAG003D')
                    AND s.certificazione_recapito_dettagli IN ('M01','M03','M04','M02','M05','M06','M07','M08','M09') THEN 1
                WHEN s.certificazione_recapito_stato IN ('RECRS002A', 'RECRN002A', 'RECAG003A')
                    AND (
                        s.certificazione_recapito_dettagli NOT IN ('M02', 'M05', 'M06', 'M07', 'M08', 'M09')
                        OR s.certificazione_recapito_dettagli IS NULL
                    ) THEN 1
                WHEN s.certificazione_recapito_stato IN ('RECRS002D', 'RECRN002D', 'RECAG003D')
                    AND (
                        s.certificazione_recapito_dettagli NOT IN ('M01', 'M03', 'M04')
                        OR s.certificazione_recapito_dettagli IS NULL
                    ) THEN 1
                ELSE 0
            END AS controllo_causale,
            CASE
                WHEN s.certificazione_recapito_stato IN ('RECRN003A', 'RECRN004A', 'RECRN005A')
                    AND (s.tentativo_recapito_stato = 'RECRN010' OR s.tentativo_recapito_stato IS NULL) THEN 0
                WHEN s.certificazione_recapito_stato IN ('RECRS003A', 'RECRS004A', 'RECRS005A')
                    AND (s.tentativo_recapito_stato = 'RECRS010' OR s.tentativo_recapito_stato IS NULL) THEN 0
                WHEN s.certificazione_recapito_stato IN ('RECAG005A','RECAG006A','RECAG007A','RECAG008A')
                    AND (s.tentativo_recapito_stato = 'RECAG010' OR s.tentativo_recapito_stato IS NULL) THEN 0
                WHEN s.certificazione_recapito_stato NOT IN ('RECRN003A','RECRN004A','RECRN005A','RECAG005A','RECRS003A',
                                                             'RECRS004A','RECRS005A','RECAG006A','RECAG007A','RECAG008A')
                    AND s.tentativo_recapito_stato = s.certificazione_recapito_stato THEN 0
                ELSE 1
            END AS controllo_inesito_casi_giacenza,
            CASE
                WHEN s.certificazione_recapito_stato = 'RECRN001A'
                    AND (s.demat_23l_ar_stato = 'RECRN001B' OR s.demat_plico_stato = 'RECRN001B' OR s.demat_arcad_stato = 'RECRN001B')
                    AND t.fine_recapito_stato = 'RECRN001C' THEN 0
                WHEN s.certificazione_recapito_stato = 'RECRN002A'
                    AND (s.demat_23l_ar_stato = 'RECRN002B' OR s.demat_plico_stato = 'RECRN002B' OR s.demat_arcad_stato = 'RECRN002B')
                    AND t.fine_recapito_stato = 'RECRN002C' THEN 0
                WHEN s.certificazione_recapito_stato = 'RECRN002D'
                    AND (s.demat_23l_ar_stato = 'RECRN002E' OR s.demat_plico_stato = 'RECRN002E' OR s.demat_arcad_stato = 'RECRN002E')
                    AND t.fine_recapito_stato = 'RECRN002F' THEN 0
                WHEN s.certificazione_recapito_stato = 'RECRN003A'
                    AND (s.demat_23l_ar_stato = 'RECRN003B' OR s.demat_plico_stato = 'RECRN003B' OR s.demat_arcad_stato = 'RECRN003B')
                    AND t.fine_recapito_stato = 'RECRN003C' THEN 0
                WHEN s.certificazione_recapito_stato = 'RECRN004A'
                    AND (s.demat_23l_ar_stato = 'RECRN004B' OR s.demat_plico_stato = 'RECRN004B' OR s.demat_arcad_stato = 'RECRN004B')
                    AND t.fine_recapito_stato = 'RECRN004C' THEN 0
                WHEN s.certificazione_recapito_stato = 'RECRN005A'
                    AND (s.demat_23l_ar_stato = 'RECRN005B' OR s.demat_plico_stato = 'RECRN005B' OR s.demat_arcad_stato = 'RECRN005B')
                    AND t.fine_recapito_stato = 'RECRN005C' THEN 0
                WHEN s.certificazione_recapito_stato = 'RECAG001A'
                    AND (s.demat_23l_ar_stato = 'RECAG001B' OR s.demat_plico_stato = 'RECAG001B' OR s.demat_arcad_stato = 'RECAG001B')
                    AND t.fine_recapito_stato = 'RECAG001C' THEN 0
                WHEN s.certificazione_recapito_stato = 'RECAG002A'
                    AND (s.demat_23l_ar_stato = 'RECAG002B' OR s.demat_plico_stato = 'RECAG002B' OR s.demat_arcad_stato = 'RECAG002B')
                    AND t.fine_recapito_stato = 'RECAG002C' THEN 0
                WHEN s.certificazione_recapito_stato = 'RECAG003A'
                    AND (s.demat_23l_ar_stato = 'RECAG003B' OR s.demat_plico_stato = 'RECAG003B' OR s.demat_arcad_stato = 'RECAG003B')
                    AND t.fine_recapito_stato = 'RECAG003C' THEN 0
                WHEN s.certificazione_recapito_stato = 'RECAG003D'
                    AND (s.demat_23l_ar_stato = 'RECAG003E' OR s.demat_plico_stato = 'RECAG003E' OR s.demat_arcad_stato = 'RECAG003E')
                    AND t.fine_recapito_stato = 'RECAG003F' THEN 0
                WHEN s.certificazione_recapito_stato = 'RECAG005A'
                    AND (s.demat_23l_ar_stato IN ('RECAG011B', 'RECAG005B')
                         OR s.demat_plico_stato IN ('RECAG011B', 'RECAG005B')
                         OR s.demat_arcad_stato IN ('RECAG011B', 'RECAG005B'))
                    AND t.fine_recapito_stato = 'RECAG005C' THEN 0
                WHEN s.certificazione_recapito_stato = 'RECAG006A'
                    AND (s.demat_23l_ar_stato IN ('RECAG011B', 'RECAG006B')
                         OR s.demat_plico_stato IN ('RECAG011B', 'RECAG006B')
                         OR s.demat_arcad_stato IN ('RECAG011B', 'RECAG006B'))
                    AND t.fine_recapito_stato = 'RECAG006C' THEN 0
                WHEN s.certificazione_recapito_stato = 'RECAG007A'
                    AND (s.demat_23l_ar_stato IN ('RECAG011B', 'RECAG007B')
                         OR s.demat_plico_stato IN ('RECAG011B', 'RECAG007B')
                         OR s.demat_arcad_stato IN ('RECAG011B', 'RECAG007B'))
                    AND t.fine_recapito_stato = 'RECAG007C' THEN 0
                WHEN s.certificazione_recapito_stato = 'RECAG008A'
                    AND (s.demat_23l_ar_stato IN ('RECAG011B', 'RECAG008B')
                         OR s.demat_plico_stato IN ('RECAG011B', 'RECAG008B')
                         OR s.demat_arcad_stato IN ('RECAG011B', 'RECAG008B'))
                    AND t.fine_recapito_stato = 'RECAG008C' THEN 0
                ELSE 1
            END AS controllo_tripletta,
            CASE
                WHEN CAST(t.certificazione_recapito_data AS TIMESTAMP) = CAST(t.fine_recapito_data AS TIMESTAMP) THEN 0
                ELSE 1
            END AS controllo_date_business,
            CASE
                WHEN s.certificazione_recapito_stato = 'RECRN005A'
                    AND DATEDIFF(CAST(s.certificazione_recapito_data AS DATE), CAST(s.tentativo_recapito_data AS DATE)) < 30 THEN 1
                ELSE 0
            END AS controllo_tempistiche_compiuta_giacenza,
            CASE
                WHEN s.prodotto = '890' AND d.documenttype = '23L' THEN 0
                WHEN s.prodotto = 'AR' AND d.documenttype = 'AR' THEN 0
                WHEN d.documenttype IS NULL THEN 0
                ELSE 1
            END AS controllo_documentType
        FROM gold_base s
        LEFT JOIN temp_demat d ON d.requestid = s.requestid AND d.rn_demat = 1
        LEFT JOIN send.silver_notification sn ON sn.iun = s.iun
        LEFT JOIN send.gold_notification_analytics n ON s.iun = n.iun
        LEFT JOIN send.silver_timeline tl ON s.iun = tl.iun AND tl.category = 'SCHEDULE_REFINEMENT'
        LEFT JOIN temp_analog_attempt a ON CAST(s.attempt_number AS INT) = CAST(a.attempt_number_timeline AS INT) AND s.iun = a.iun
        LEFT JOIN temp_ultimi_eventi_silver_postalizzazione t ON s.requestid = t.requestid
    ),

    temp_postalizzazione AS (
        SELECT
            s.*,
            IF(
                fine_recapito_stato IN ('RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C','RECAG005C','RECAG006C','RECAG007C','RECAG008C')
                AND tentativo_recapito_stato IS NULL,
                1, 0
            ) AS assenza_inesito,
            IF(
                fine_recapito_stato IN ('RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C','RECAG005C','RECAG006C','RECAG007C','RECAG008C')
                AND messaingiacenza_recapito_stato IS NULL,
                1, 0
            ) AS assenza_messa_in_giacenza,
            IF(certificazione_recapito_stato IS NULL, 1, 0) AS assenza_pre_esito,
            IF(
                (
                    fine_recapito_stato IN ('RECAG008C') AND (demat_23l_ar_stato IS NULL OR demat_plico_stato IS NULL)
                )
                OR (
                    demat_23l_ar_stato IS NULL AND demat_plico_stato IS NULL
                ),
                1, 0
            ) AS assenza_dematerializzazione_23l_ar_plico,
            IF(
                prodotto = '890'
                AND fine_recapito_stato IN ('RECAG005C','RECAG006C','RECAG007C','RECAG008C')
                AND demat_arcad_data_rendicontazione IS NULL,
                1, 0
            ) AS assenza_demat_ARCAD,
            IF(
                prodotto = '890'
                AND fine_recapito_stato IN ('RECAG005C','RECAG006C','RECAG007C','RECAG008C')
                AND accettazione_23l_recag012_data IS NULL,
                1, 0
            ) AS assenza_RECAG012
        FROM temp_controlli s
    ),

    temp_assenza_eventi AS (
        SELECT
            *,
            IF(
                assenza_inesito = 1 OR assenza_messa_in_giacenza = 1 OR assenza_pre_esito = 1
                OR assenza_dematerializzazione_23l_ar_plico = 1 OR assenza_demat_ARCAD = 1 OR assenza_RECAG012 = 1,
                1, 0
            ) AS flag_esiti_mancanti
        FROM temp_postalizzazione
    ),

    finale AS (
        SELECT
            t.senderpaid,
            t.senderdenomination,
            t.iun,
            t.requestid,
            t.requesttimestamp,
            t.prodotto,
            t.geokey,
            t.recapitista_unif,
            t.lotto,
            t.codice_oggetto,
            t.codiceOggetto,
            t.scarto_consolidatore_stato,
            t.scarto_consolidatore_data,
            t.affido_recapitista_con016_data,
            t.accettazione_recapitista_con018_data,
            t.affido_accettazione_rec_data,
            t.data_deposito,
            t.tentativo_recapito_stato,
            t.tentativo_recapito_data,
            t.tentativo_recapito_data_rendicontazione,
            t.messaingiacenza_recapito_stato,
            t.messaingiacenza_recapito_data,
            t.messaingiacenza_recapito_data_rendicontazione,
            t.certificazione_recapito_stato,
            t.certificazione_recapito_dettagli,
            t.certificazione_recapito_data,
            t.certificazione_recapito_data_rendicontazione,
            t.demat_23l_ar_stato,
            t.documenttype,
            t.demat_23l_ar_data_rendicontazione,
            t.demat_plico_stato,
            t.demat_plico_data_rendicontazione,
            t.demat_arcad_stato,
            t.demat_arcad_data_rendicontazione,
            t.fine_recapito_stato,
            t.fine_recapito_data,
            t.fine_recapito_data_rendicontazione,
            t.accettazione_23l_recag012_data,
            t.accettazione_23l_recag012_data_rendicontazione,
            t.recindex_number,
            t.perfezionamento_data,
            t.perfezionamento_tipo,
            t.perfezionamento_notificationdate,
            t.perfezionamento_stato,
            t.perfezionamento_stato_dettagli,
            t.tms_perfezionamento_notification,
            t.tms_date_payment,
            t.flag_schedule_refinement,
            t.flag_feedback_attempt_0,
            t.tms_cancelled,
            t.flag_wi7_consolidatore,
            t.flag_wi7_report_postalizzazioni_incomplete,
            t.wi7_cluster,
            t.pcretry_rank,
            t.attempt_rank,
            t.certificazione_recapito_stato_silver,
            t.certificazione_recapito_data_silver,
            t.certificazione_recapito_rendicontazione_silver,
            t.fine_recapito_stato_silver,
            t.fine_recapito_data_silver,
            t.fine_recapito_rendicontazione_silver,
            t.flag_multi_destinatario,
            t.flag_ultima_postalizzazione,
            t.flag_prodotto_estero,
            t.flag_destinatario_deceduto,
            t.flag_esiti_mancanti,
            CASE
                WHEN controllo_causale = 1 OR controllo_date_business = 1 OR controllo_tripletta = 1
                     OR controllo_tempistiche_compiuta_giacenza = 1 OR controllo_inesito_casi_giacenza = 1
                     OR controllo_documentType = 1
                THEN 1
                ELSE 0
            END AS flag_errore_rendicontazione,
            CONCAT_WS(
                ', ',
                CASE WHEN controllo_causale = 1 AND fine_recapito_stato IS NOT NULL THEN 'errore rend. causale' END,
                CASE WHEN controllo_date_business = 1 AND fine_recapito_stato IS NOT NULL THEN 'errore rend. date business' END,
                CASE WHEN controllo_tripletta = 1 AND fine_recapito_stato IS NOT NULL THEN 'errore rend. tripletta' END,
                CASE WHEN controllo_tempistiche_compiuta_giacenza = 1 AND fine_recapito_stato IS NOT NULL THEN 'errore rend. tempistiche compiuta giacenza' END,
                CASE WHEN controllo_inesito_casi_giacenza = 1 AND fine_recapito_stato IS NOT NULL THEN 'errore rend. inesito casi giacenza' END,
                CASE WHEN assenza_inesito = 1 AND fine_recapito_stato IS NOT NULL THEN 'assenza inesito' END,
                CASE WHEN assenza_messa_in_giacenza = 1 AND fine_recapito_stato IS NOT NULL THEN 'assenza messa in giacenza' END,
                CASE WHEN assenza_pre_esito = 1 AND fine_recapito_stato IS NOT NULL THEN 'assenza pre-esito' END,
                CASE WHEN assenza_dematerializzazione_23l_ar_plico = 1 AND fine_recapito_stato IS NOT NULL THEN 'assenza demat 23l_ar / plico' END,
                CASE WHEN assenza_demat_arcad = 1 AND fine_recapito_stato IS NOT NULL THEN 'assenza demat ARCAD' END,
                CASE WHEN assenza_RECAG012 = 1 AND fine_recapito_stato IS NOT NULL THEN 'assenza RECAG012' END,
                CASE WHEN controllo_documentType = 1 AND fine_recapito_stato IS NOT NULL THEN 'errore documentType' END,
                CASE WHEN controllo_causale = 0 AND controllo_date_business = 0 AND controllo_tripletta = 0
                           AND controllo_tempistiche_compiuta_giacenza = 0 AND controllo_inesito_casi_giacenza = 0
                           AND assenza_inesito = 0 AND assenza_RECAG012 = 0 AND assenza_pre_esito = 0
                           AND assenza_dematerializzazione_23l_ar_plico = 0 AND assenza_messa_in_giacenza = 0
                           AND assenza_demat_arcad = 0 AND flag_destinatario_deceduto = 0
                           AND controllo_documentType = 0 AND flag_feedback_attempt_0 = 0
                     THEN 'da perfezionare' END,
                CASE WHEN controllo_causale = 0 AND controllo_date_business = 0 AND controllo_tripletta = 0
                           AND controllo_tempistiche_compiuta_giacenza = 0 AND controllo_inesito_casi_giacenza = 0
                           AND assenza_inesito = 0 AND assenza_RECAG012 = 0 AND assenza_pre_esito = 0
                           AND assenza_dematerializzazione_23l_ar_plico = 0 AND assenza_messa_in_giacenza = 0
                           AND assenza_demat_arcad = 0 AND flag_destinatario_deceduto = 0
                           AND controllo_documentType = 0 AND flag_feedback_attempt_0 = 1
                     THEN 'bloccato al primo attempt' END
            ) AS dettaglio_rendicontazione,
            CASE
                WHEN (flag_wi7_report_postalizzazioni_incomplete = 1 OR flag_wi7_consolidatore = 1) THEN 'Oggetto fuori sla'
                WHEN flag_destinatario_deceduto = 1 THEN 'Oggetto restituito al mittente (esito destinatario deceduto)'
                WHEN (
                    controllo_causale = 1 OR controllo_date_business = 1 OR controllo_tripletta = 1
                    OR controllo_tempistiche_compiuta_giacenza = 1 OR controllo_inesito_casi_giacenza = 1
                    OR flag_esiti_mancanti = 1 OR controllo_documentType = 1
                ) AND fine_recapito_stato IS NOT NULL
                    THEN 'Errore rendicontazione/assenza eventi intermedi'
                WHEN (
                    controllo_causale = 0 AND controllo_date_business = 0 AND controllo_tripletta = 0
                    AND controllo_tempistiche_compiuta_giacenza = 0 AND controllo_inesito_casi_giacenza = 0
                    AND assenza_inesito = 0 AND assenza_RECAG012 = 0 AND assenza_pre_esito = 0
                    AND assenza_dematerializzazione_23l_ar_plico = 0 AND assenza_messa_in_giacenza = 0
                    AND assenza_demat_arcad = 0 AND flag_destinatario_deceduto = 0
                    AND controllo_documentType = 0
                ) THEN 'Oggetto con rendicontazione corretta in analisi Team Prodotto'
                ELSE 'Oggetto in corso di postalizzazione'
            END AS cluster_mancato_perfezionamento
        FROM temp_assenza_eventi t
    ),

    finale_filtrato AS (
        SELECT
            CAST(CAST(NOW() AS DATE) AS STRING) AS data_estrazione,
            senderpaid,
            senderdenomination,
            iun,
            data_deposito,
            requestid,
            requesttimestamp,
            prodotto,
            geokey,
            recapitista_unif,
            lotto,
            codice_oggetto,
            codiceoggetto,
            affido_accettazione_rec_data,
            scarto_consolidatore_stato,
            scarto_consolidatore_data,
            affido_recapitista_con016_data,
            accettazione_recapitista_con018_data,
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
            documenttype,
            demat_23l_ar_data_rendicontazione,
            demat_plico_stato,
            demat_plico_data_rendicontazione,
            demat_arcad_stato,
            demat_arcad_data_rendicontazione,
            fine_recapito_stato,
            fine_recapito_data,
            fine_recapito_data_rendicontazione,
            accettazione_23l_recag012_data,
            accettazione_23l_recag012_data_rendicontazione,
            recindex_number,
            perfezionamento_data,
            perfezionamento_tipo,
            perfezionamento_notificationdate,
            perfezionamento_stato,
            perfezionamento_stato_dettagli,
            tms_perfezionamento_notification,
            flag_schedule_refinement,
            flag_feedback_attempt_0,
            tms_cancelled,
            flag_wi7_consolidatore,
            flag_wi7_report_postalizzazioni_incomplete,
            wi7_cluster,
            pcretry_rank,
            attempt_rank,
            certificazione_recapito_stato_silver,
            certificazione_recapito_data_silver,
            certificazione_recapito_rendicontazione_silver,
            fine_recapito_stato_silver,
            fine_recapito_data_silver,
            fine_recapito_rendicontazione_silver,
            tms_date_payment,
            cluster_mancato_perfezionamento,
            dettaglio_rendicontazione
        FROM finale
        WHERE flag_ultima_postalizzazione = 1
          AND flag_schedule_refinement = 0
          AND (certificazione_recapito_stato NOT IN ('RECRS006','RECRS013','RECRN006','RECRN013','RECAG004','RECAG013')
               OR certificazione_recapito_stato IS NULL)
          AND COALESCE(tentativo_recapito_stato, '') NOT IN ('PN998','PN999')
          AND COALESCE(certificazione_recapito_stato, '') NOT IN ('PN998','PN999')
          AND COALESCE(fine_recapito_stato, '') NOT IN ('PN998','PN999')
          AND tms_perfezionamento_notification IS NULL
          AND flag_destinatario_deceduto = 0
    )

    SELECT *
    FROM finale_filtrato
    WHERE cluster_mancato_perfezionamento = 'Oggetto con rendicontazione corretta in analisi Team Prodotto'
    """
    return query


def run_query(spark: SparkSession):
    logging.info("Esecuzione query base...")
    query_sql = build_query_sql()
    return spark.sql(query_sql)


# ---------------- MAIN SCRIPT ----------------
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

    max_date_str = "-"
    righe_settimana_precedente = 0
    oggetti_tolti = 0
    righe_settimana_corrente = 0
    count_pre_distinct = 0
    count_post_distinct = 0

    df_pre_distinct_spark = None
    df_post_distinct_spark = None
    df_clean_spark = None

    try:
        creds = load_google_credentials(GOOGLE_SECRET_PATH)
        sheet = Sheet(sheet_id=SHEET_ID, service_credentials=creds, id_mode="key")

        max_date_str = get_last_update_date(spark)

        write_meta_tab(
            creds=creds,
            max_date_str=max_date_str,
            start_run_str=job_start_str,
            end_run_str="-",
            elapsed_min_str="IN CORSO",
            stato_run="IN CORSO",
            righe_settimana_precedente=0,
            oggetti_comuni_settimana_precedente=0,
            righe_settimana_corrente=0,
            count_pre_distinct=0,
            count_post_distinct=0,
        )

        df_old = read_previous_week(sheet)
        righe_settimana_precedente = int(df_old.shape[0]) if not df_old.empty else 0

        # Query base con possibile presenza di duplicati
        df_pre_distinct_spark = run_query(spark).persist(StorageLevel.MEMORY_AND_DISK)
        count_pre_distinct = df_pre_distinct_spark.count()
        logging.info(f"Count pre distinct: {count_pre_distinct}")

        # Deduplica a partire dal risultato precedente
        df_post_distinct_spark = df_pre_distinct_spark.dropDuplicates().persist(
            StorageLevel.MEMORY_AND_DISK
        )
        count_post_distinct = df_post_distinct_spark.count()
        logging.info(f"Count post distinct (su requestid): {count_post_distinct}")

        df_old_ids_spark = build_previous_requestids_df(spark, df_old)

        if df_old_ids_spark.take(1):
            logging.info(
                "Applicazione esclusione requestid già presenti nella settimana precedente..."
            )
            df_clean_spark = df_post_distinct_spark.alias("n").join(
                df_old_ids_spark.alias("o"),
                on=F.col("n.requestid") == F.col("o.requestid"),
                how="left_anti",
            )
        else:
            logging.info(
                "Nessun requestid storico disponibile: nessuna esclusione applicata."
            )
            df_clean_spark = df_post_distinct_spark

        df_clean_spark = df_clean_spark.persist(StorageLevel.MEMORY_AND_DISK)

        righe_settimana_corrente = df_clean_spark.count()
        oggetti_tolti = count_post_distinct - righe_settimana_corrente

        logging.info(f"Righe settimana precedente: {righe_settimana_precedente}")
        logging.info(f"Delta settimanale: {oggetti_tolti}")
        logging.info(
            f"Righe settimana corrente da scrivere: {righe_settimana_corrente}"
        )

        df_output = to_pandas_for_sheets(df_spark=df_clean_spark)
        export_to_sheets(df_output, creds, SHEET_ID, TAB_OUTPUT)

        job_end_dt = now_utc_dt()
        job_end_str = job_end_dt.strftime("%Y-%m-%d %H:%M:%S")
        job_elapsed_min = elapsed_minutes_str(job_start_dt, job_end_dt)

        write_meta_tab(
            creds=creds,
            max_date_str=max_date_str,
            start_run_str=job_start_str,
            end_run_str=job_end_str,
            elapsed_min_str=job_elapsed_min,
            stato_run="OK",
            righe_settimana_precedente=righe_settimana_precedente,
            oggetti_comuni_settimana_precedente=oggetti_tolti,
            righe_settimana_corrente=righe_settimana_corrente,
            count_pre_distinct=count_pre_distinct,
            count_post_distinct=count_post_distinct,
        )

        if count_pre_distinct == count_post_distinct:
            slack_msg = (
                f"{slack_prefix}\n"
                f"✅✅✅ *SUCCESS* ✅✅✅\n"
                f"*Job:* {APP_NAME}\n"
                f"*RunId (UTC):* {run_id}\n"
                f"*Host:* {hostname}\n"
                f"*Esito distinct su requestid:* nessuna differenza\n"
                f"*Count pre distinct:* {count_pre_distinct}\n"
                f"*Count post distinct:* {count_post_distinct}\n"
                f"*Righe settimana precedente:* {righe_settimana_precedente}\n"
                f"*Oggetti tolti perché già presenti:* {oggetti_tolti}\n"
                f"*Righe settimana corrente scritte:* {righe_settimana_corrente}\n"
                f"*Ultimo aggiornamento dati:* {max_date_str}\n"
            )
        else:
            slack_msg = (
                f"{slack_prefix}\n"
                f"⚠️⚠️⚠️ *SUCCESS WITH DISTINCT DIFFERENCE* ⚠️⚠️⚠️\n"
                f"*Job:* {APP_NAME}\n"
                f"*RunId (UTC):* {run_id}\n"
                f"*Host:* {hostname}\n"
                f"*Count pre distinct:* {count_pre_distinct}\n"
                f"*Count post distinct:* {count_post_distinct}\n"
                f"*Differenza:* {count_pre_distinct - count_post_distinct}\n"
                f"*Righe settimana precedente:* {righe_settimana_precedente}\n"
                f"*Oggetti tolti perché già presenti:* {oggetti_tolti}\n"
                f"*Righe settimana corrente scritte:* {righe_settimana_corrente}\n"
                f"*Ultimo aggiornamento dati:* {max_date_str}\n"
            )

        logging.info("Processo completato con successo.")
        send_slack(slack_webhook_url, slack_msg)

    except Exception:
        logging.error("Errore durante l'esecuzione del job.", exc_info=True)

        try:
            job_end_dt = now_utc_dt()
            job_end_str = job_end_dt.strftime("%Y-%m-%d %H:%M:%S")
            job_elapsed_min = elapsed_minutes_str(job_start_dt, job_end_dt)

            creds = load_google_credentials(GOOGLE_SECRET_PATH)
            write_meta_tab(
                creds=creds,
                max_date_str=max_date_str,
                start_run_str=job_start_str,
                end_run_str=job_end_str,
                elapsed_min_str=job_elapsed_min,
                stato_run="KO",
                righe_settimana_precedente=righe_settimana_precedente,
                oggetti_comuni_settimana_precedente=oggetti_tolti,
                righe_settimana_corrente=righe_settimana_corrente,
                count_pre_distinct=count_pre_distinct,
                count_post_distinct=count_post_distinct,
            )
        except Exception:
            logging.warning(
                "Aggiornamento del tab meta fallito durante la gestione errore."
            )

        fail_msg = (
            f"{slack_prefix}\n"
            f"❌❌❌ *FAILURE* ❌❌❌\n"
            f"*Job:* {APP_NAME}\n"
            f"*RunId (UTC):* {run_id}\n"
            f"*Host:* {hostname}\n"
            f"Il job non è terminato correttamente.\n"
        )
        try:
            send_slack(slack_webhook_url, fail_msg)
        finally:
            raise

    finally:
        for df_name, df_obj in [
            ("df_clean_spark", df_clean_spark),
            ("df_post_distinct_spark", df_post_distinct_spark),
            ("df_pre_distinct_spark", df_pre_distinct_spark),
        ]:
            try:
                if df_obj is not None:
                    df_obj.unpersist()
                    logging.info(f"{df_name} rilasciato dalla cache.")
            except Exception:
                logging.warning(f"unpersist fallito per {df_name}.")

        try:
            spark.stop()
        except Exception:
            logging.warning("spark.stop() fallito in chiusura.")


if __name__ == "__main__":
    main()
