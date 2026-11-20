import io
import json
import logging
import os
import socket
import urllib.request
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from pdnd_google_utils import Sheet
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

# ---------------- CONFIGURAZIONE BASE ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

APP_NAME = "Rendicontazione corretta senza feedback analogico"
GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"
SHEET_ID_OUTPUT = "1HypHzaiQUHU8xv9EwlHzGVpBNSplgob79qwf8Yn6H38"
TAB_OUTPUT = "Rendicontazione corretta"
TAB_META = "Log di controllo"
TAB_WEEKLY = "Riepilogo prime rilevazioni"
COL_FIRST_DATE = "data_prima_estrazione"
COL_FIRST_WEEK = "settimana_prima_estrazione"
LOCAL_TZ = ZoneInfo("Europe/Rome")
# Prodotti abilitati: AR, RIR, 890. RS e RIS restano predisposti ma esclusi.
ACTIVE_PRODUCTS = ("AR", "RIR", "890")
# ACTIVE_PRODUCTS = ("AR", "RIR", "890", "RS", "RIS")  # Attivazione futura, previa verifica dei codici SQL.

SLACK_CONFIG_CANDIDATE_PATHS = [
    "/Slack/notifications_webhook.txt",
    "/app/mount/Slack/notifications_webhook.txt",
    "/app/mount/notifications_webhook.txt",
]
SLACK_WEBHOOK_NAME = "webhook_1"
SLACK_LABEL = "Rendicontazione corretta senza feedback"

## Per file CSV
DRIVE_FOLDER_ID = "11FONwQS-5zA6gV60MQ-HKAGEdXLyoRke"
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]
DRIVE_SUPPORTS_ALL_DRIVES = True

CSV_SEPARATOR = ";"
CSV_MIME_TYPE = "text/csv"
CSV_FILE_BASENAME = "Oggetti_con_rendicontazione_corretta"


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


def build_drive_service(creds: dict):
    drive_credentials = service_account.Credentials.from_service_account_info(
        creds,
        scopes=DRIVE_SCOPES,
    )

    return build(
        "drive",
        "v3",
        credentials=drive_credentials,
        cache_discovery=False,
    )


def upload_csv_to_drive(
    df: pd.DataFrame,
    creds: dict,
    folder_id: str,
    filename: str,
    separator: str = ";",
) -> dict:

    logging.info(
        "Preparazione CSV GO (stock completo): %s - righe: %d", filename, len(df)
    )
    csv_buffer = io.StringIO()

    df.to_csv(
        csv_buffer,
        sep=separator,
        index=False,
    )

    csv_bytes = csv_buffer.getvalue().encode("utf-8-sig")

    media = MediaIoBaseUpload(
        io.BytesIO(csv_bytes),
        mimetype=CSV_MIME_TYPE,
        resumable=True,
    )

    metadata = {
        "name": filename,
        "parents": [folder_id],
        "mimeType": CSV_MIME_TYPE,
    }

    drive_service = build_drive_service(creds)

    created_file = (
        drive_service.files()
        .create(
            body=metadata,
            media_body=media,
            fields="id,name,mimeType,parents,webViewLink,createdTime,modifiedTime",
            supportsAllDrives=DRIVE_SUPPORTS_ALL_DRIVES,
        )
        .execute()
    )

    logging.info(
        "CSV creato su Google Drive. Nome: %s - File ID: %s - Link: %s",
        created_file.get("name"),
        created_file.get("id"),
        created_file.get("webViewLink"),
    )

    return created_file


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


def read_previous_tab(sheet: Sheet, tab_name: str) -> pd.DataFrame:
    logging.info(f"Lettura tab precedente '{tab_name}'...")
    try:
        df_old = sheet.download(tab_name)
        if df_old is None or df_old.empty:
            logging.info(f"Tab precedente '{tab_name}' vuoto o non valorizzato.")
            return pd.DataFrame()
        df_old = to_pandas_for_sheets(df_pd=df_old)
        logging.info(f"Righe precedenti lette da '{tab_name}': {df_old.shape[0]}")
        return df_old
    except Exception as e:
        logging.error(
            "Errore lettura tab '%s': %s. Non sovrascrivo lo storico.", tab_name, e
        )
        raise


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
    righe_settimana_precedente,
    righe_settimana_corrente,
    oggetti_nuovi_rispetto_precedente=None,
    primo_attempt_corrente=0,
    secondo_attempt_corrente=0,
    tracker_attivo_corrente=0,
    usciti_da_stock=0,
) -> pd.DataFrame:
    if oggetti_nuovi_rispetto_precedente is None:
        oggetti_nuovi_rispetto_precedente = righe_settimana_corrente

    return to_pandas_for_sheets(
        df_pd=pd.DataFrame(
            {
                "Ultimo aggiornamento dati (MAX requesttimestamp)": [max_date_str],
                "Start run time (UTC)": [start_run_str],
                "End run time (UTC)": [end_run_str],
                "Tempo impiegato (min)": [elapsed_min_str],
                "Stato run": [stato_run],
                "Righe run precedente": [righe_settimana_precedente],
                "Oggetti nuovi": [oggetti_nuovi_rispetto_precedente],
                "Righe run corrente": [righe_settimana_corrente],
                "Primo attempt - run corrente": [primo_attempt_corrente],
                "Secondo attempt - run corrente": [secondo_attempt_corrente],
                "Tracker attivo - run corrente": [tracker_attivo_corrente],
                "Oggetti usciti dallo stock": [usciti_da_stock],
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
    righe_settimana_precedente,
    righe_settimana_corrente,
    sheet_id: str = SHEET_ID_OUTPUT,
    tab_meta: str = TAB_META,
    oggetti_nuovi_rispetto_precedente=None,
    primo_attempt_corrente=0,
    secondo_attempt_corrente=0,
    tracker_attivo_corrente=0,
    usciti_da_stock=0,
):
    df_meta = build_meta_df(
        max_date_str=max_date_str,
        start_run_str=start_run_str,
        end_run_str=end_run_str,
        elapsed_min_str=elapsed_min_str,
        stato_run=stato_run,
        righe_settimana_precedente=righe_settimana_precedente,
        righe_settimana_corrente=righe_settimana_corrente,
        oggetti_nuovi_rispetto_precedente=oggetti_nuovi_rispetto_precedente,
        primo_attempt_corrente=primo_attempt_corrente,
        secondo_attempt_corrente=secondo_attempt_corrente,
        tracker_attivo_corrente=tracker_attivo_corrente,
        usciti_da_stock=usciti_da_stock,
    )
    export_to_sheets(df_meta, creds, sheet_id, tab_meta)


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
        WITH gold_base_raw AS (
            SELECT
                s.senderpaid,
                s.iun,
                s.requestid,
                s.requesttimestamp,
                s.prodotto,
                s.geokey,
                s.recapitista_unificato,
                s.lotto,
                s.codice_oggetto,
                s.scarto_consolidatore_stato,
                s.scarto_consolidatore_data,
                s.affido_recapitista_con016_data,
                s.accettazione_recapitista_con018_data,
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
                CASE
                    WHEN s.demat_23l_ar_data_rendicontazione IS NULL
                     AND s.demat_plico_data_rendicontazione IS NULL THEN NULL
                    WHEN s.demat_23l_ar_data_rendicontazione IS NOT NULL
                     AND s.demat_plico_data_rendicontazione IS NULL THEN '23L_AR'
                    WHEN s.demat_23l_ar_data_rendicontazione IS NULL
                     AND s.demat_plico_data_rendicontazione IS NOT NULL THEN 'PLICO'
                    WHEN s.demat_plico_data_rendicontazione
                         > s.demat_23l_ar_data_rendicontazione THEN 'PLICO'
                    ELSE '23L_AR'
                END AS demat_documenttype_da_controllare,
                s.demat_arcad_stato,
                s.demat_arcad_data_rendicontazione,
                s.fine_recapito_stato,
                s.fine_recapito_data,
                s.fine_recapito_data_rendicontazione,
                s.accettazione_23l_recag012_data,
                s.accettazione_23l_recag012_data_rendicontazione,
                s.recindex_number,
                -- Campi nativi del perfezionamento: informativi e indipendenti
                -- dalla verifica sulla presenza di SEND_ANALOG_FEEDBACK.
                s.perfezionamento_data,
                s.perfezionamento_tipo,
                s.perfezionamento_notificationdate,
                s.perfezionamento_stato,
                s.perfezionamento_stato_dettagli,
                s.tms_cancelled,
                s.flag_wi7_consolidatore,
                s.flag_wi7_report_postalizzazioni_incomplete,
                s.wi7_cluster,
                s.pcretry_rank,
                s.attempt_rank,
                s.attempt_number,
                s.flag_prodotto_estero
            -- GOLD: i campi temporali sono TIMESTAMP che esprimono gia' l'orario italiano.
            FROM send.gold_postalizzazione_analytics s
            WHERE s.scarto_consolidatore_stato IS NULL
            AND COALESCE(s.statusrequest, '') NOT IN (
                    'PN999', 'PN998', 'error', 'internalError', 'syntaxError',
                    'transformationError', 'semanticError', 'authenticationError', 'duplicatedRequest'
            )
            AND s.attempt_rank = 1
            AND s.pcretry_rank = 1
            AND s.tms_cancelled IS NULL
            AND COALESCE(s.certificazione_recapito_stato, '') NOT IN (
                        'RECRS006','RECRS013','RECRN006','RECRN013','RECAG004','RECAG013'
                    )
            AND s.fine_recapito_data_rendicontazione IS NOT NULL
            AND CAST(s.fine_recapito_data_rendicontazione AS DATE) < DATE_SUB(CURRENT_DATE(), 2)
            -- RS e RIS temporaneamente esclusi dal perimetro corrente.
            -- Per riattivarli estendere il perimetro e decommentare gli status dedicati sotto.
            AND s.prodotto IN ('AR', 'RIR', '890')
        ),
        perimetro_iun_raw AS (
            SELECT DISTINCT
                iun
            FROM gold_base_raw
        ),
        returned_to_sender_iun AS (
            SELECT
                n.iun
            FROM  perimetro_iun_raw p
            INNER JOIN send.gold_notification_analytics n
                ON n.iun = p.iun
            WHERE n.tms_returned_to_sender IS NOT NULL
        ),
        gold_base AS (
            SELECT
                gb.*
            FROM gold_base_raw gb
            LEFT ANTI JOIN returned_to_sender_iun rts
                ON gb.iun = rts.iun
        ),
        perimetro_iun_base AS (
            SELECT DISTINCT iun FROM gold_base
        ),
        send_analog_feedback AS (
            SELECT
                tl.iun,
                CAST(REGEXP_EXTRACT(tl.timelineelementid, 'RECINDEX_([0-9]+)', 1) AS INT) AS recindex_number,
                CAST(REGEXP_EXTRACT(tl.timelineelementid, 'ATTEMPT_([0-9]+)', 1) AS INT) AS attempt_number
            FROM send.silver_timeline tl
            INNER JOIN perimetro_iun_base p ON tl.iun = p.iun
            WHERE tl.category = 'SEND_ANALOG_FEEDBACK'
        ),
        -- Il vincolo sull'assenza di feedback e' del report operativo, non dei
        -- controlli di correttezza della rendicontazione né del cluster.
        gold_base_senza_feedback AS (
            SELECT gb.*
            FROM gold_base gb
            LEFT ANTI JOIN send_analog_feedback saf
                ON gb.iun = saf.iun
                AND CAST(gb.recindex_number AS INT) = saf.recindex_number
                AND CAST(gb.attempt_number AS INT) = saf.attempt_number
            WHERE CAST(gb.attempt_number AS INT) IN (0, 1)
        ),
        perimetro_requestid AS (
            SELECT DISTINCT requestid, iun, CAST(attempt_number AS INT) AS attempt_number
            FROM gold_base_senza_feedback
        ),
        perimetro_iun AS (
            SELECT DISTINCT iun FROM perimetro_requestid
        ),

        -- SILVER: statusdatetime/clientrequesttimestamp sono stringhe UTC con Z.
        -- La conversione a Europe/Rome avviene UNA SOLA VOLTA in temp_silver_postalizzazione.
        silver_eventi_postalizzazione AS (
            SELECT
                REGEXP_REPLACE(p.requestid, '^pn-cons-000~', '') AS requestid,
                e.paperprogrstatus.statuscode AS statuscode,
                e.paperprogrstatus.deliveryfailurecause AS deliveryfailurecause_raw,
                e.paperprogrstatus.statusdatetime AS statusdatetime_raw,
                e.paperprogrstatus.clientrequesttimestamp AS clientrequesttimestamp_raw,
                e.paperprogrstatus.attachments AS attachments
            FROM send.silver_postalizzazione p
            INNER JOIN perimetro_requestid pr
                ON REGEXP_REPLACE(p.requestid, '^pn-cons-000~', '') = pr.requestid
            LATERAL VIEW explode(p.eventslist) ev AS e
        ),

        temp_demat AS (
            SELECT
                requestid,
                documenttype,
                rn_demat
            FROM (
                SELECT
                    se.requestid,
                    a.documenttype AS documenttype,
                    ROW_NUMBER() OVER (
                        PARTITION BY se.requestid
                        ORDER BY se.clientrequesttimestamp_raw DESC
                    ) AS rn_demat
                FROM silver_eventi_postalizzazione se
                LATERAL VIEW explode(se.attachments) av AS a
                WHERE a.documenttype IN ('23L', 'AR')
            ) x
        ),

        temp_silver_postalizzazione AS (
            SELECT
                requestid,
                statuscode,
                CASE WHEN deliveryfailurecause_raw IN
                    ('M01','M02','M03','M04','M05','M06','M07','M08','M09')
                    THEN deliveryfailurecause_raw ELSE NULL END AS deliveryfailurecause,
                -- SILVER: stringa ISO-8601 UTC (Z) -> timestamp con orario italiano.
                -- Si rimuove Z PRIMA del CAST per evitare la doppia conversione.
                FROM_UTC_TIMESTAMP(
                    CAST(REPLACE(REGEXP_REPLACE(
                        CASE
                            WHEN LENGTH(statusdatetime_raw) = 17 THEN CONCAT(
                                SUBSTR(statusdatetime_raw, 1, 16), ':00Z'
                            )
                            ELSE statusdatetime_raw
                        END, 'Z$', ''), 'T', ' ') AS TIMESTAMP),
                    'Europe/Rome'
                ) AS statusdatetime,
                -- SILVER: stringa ISO-8601 UTC (Z) -> timestamp con orario italiano.
                -- Si rimuove Z PRIMA del CAST per evitare la doppia conversione.
                FROM_UTC_TIMESTAMP(
                    CAST(REPLACE(REGEXP_REPLACE(
                        CASE
                            WHEN LENGTH(clientrequesttimestamp_raw) = 17 THEN CONCAT(
                                SUBSTR(clientrequesttimestamp_raw, 1, 16), ':00.000Z'
                            )
                            ELSE clientrequesttimestamp_raw
                        END, 'Z$', ''), 'T', ' ') AS TIMESTAMP),
                    'Europe/Rome'
                ) AS clientrequesttimestamp,
                tipo,
                ROW_NUMBER() OVER (
                    PARTITION BY requestid, tipo
                    ORDER BY clientrequesttimestamp_raw DESC
                ) AS rn
            FROM (
                SELECT
                    se.requestid,
                    se.statuscode,
                    se.deliveryfailurecause_raw,
                    se.statusdatetime_raw,
                    se.clientrequesttimestamp_raw,
                    CASE
                        WHEN se.statuscode IN (
                            -- RS temporaneamente escluso:
                            -- 'RECRS001C','RECRS002C','RECRS002F','RECRS003C','RECRS004C','RECRS005C',
                            'RECRN001C','RECRN002C','RECRN002F','RECRN003C','RECRN004C','RECRN005C',
                            'RECAG001C','RECAG002C','RECAG003C','RECAG003F','RECAG005C','RECAG006C','RECAG007C','RECAG008C',
                            -- RIS temporaneamente escluso: 'RECRSI003C','RECRSI004C',
                            'RECRI003C','RECRI004C'
                        ) THEN 'FINE_RECAPITO'
                        WHEN se.statuscode IN (
                            -- RS temporaneamente escluso:
                            -- 'RECRS001C','RECRS002A','RECRS002D','RECRS003C','RECRS004A','RECRS005A',
                            'RECRN001A','RECRN002A','RECRN002D','RECRN003A','RECRN004A','RECRN005A',
                            'RECAG001A','RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A','RECAG007A','RECAG008A',
                            -- RIS temporaneamente escluso: 'RECRSI003C','RECRSI004A',
                            'RECRI003A','RECRI004A'
                        ) THEN 'CERTIFICAZIONE_RECAPITO'
                        WHEN se.statuscode IN (
                            -- RS temporaneamente escluso: 'RECRS002B','RECRS002E','RECRS004B','RECRS005B',
                            'RECRN001B','RECRN002B','RECRN002E','RECRN003B','RECRN004B','RECRN005B',
                            'RECAG001B','RECAG002B','RECAG003B','RECAG003E','RECAG005B','RECAG006B','RECAG007B','RECAG008B',
                            -- RIS temporaneamente escluso: 'RECRSI004B',
                            'RECRI003B','RECRI004B'
                        ) THEN 'DEMAT_RECAPITO'
                        ELSE NULL
                    END AS tipo
                FROM silver_eventi_postalizzazione se
                -- I codici di stato sono definiti una sola volta nel CASE sopra.
                -- Le famiglie RS/RIS rimangono commentate fino alla riattivazione.

            ) x
            WHERE tipo IS NOT NULL
        ),

        temp_ultimi_eventi_silver_postalizzazione AS (
            SELECT
                requestid,
                MAX(CASE WHEN tipo = 'FINE_RECAPITO' THEN statuscode END) AS fine_recapito_stato,
                MAX(CASE WHEN tipo = 'FINE_RECAPITO' THEN deliveryfailurecause END) AS fine_recapito_deliveryfailurecause,
                MAX(CASE WHEN tipo = 'FINE_RECAPITO' THEN statusdatetime END) AS fine_recapito_data,
                MAX(CASE WHEN tipo = 'FINE_RECAPITO' THEN clientrequesttimestamp END) AS fine_recapito_rendicontazione,
                MAX(CASE WHEN tipo = 'CERTIFICAZIONE_RECAPITO' THEN statuscode END) AS certificazione_recapito_stato,
                MAX(CASE WHEN tipo = 'CERTIFICAZIONE_RECAPITO' THEN statusdatetime END) AS certificazione_recapito_data,
                MAX(CASE WHEN tipo = 'CERTIFICAZIONE_RECAPITO' THEN clientrequesttimestamp END) AS certificazione_recapito_rendicontazione,
                MAX(CASE WHEN tipo = 'DEMAT_RECAPITO' THEN statuscode END) AS demat_recapito_stato,
                MAX(CASE WHEN tipo = 'DEMAT_RECAPITO' THEN deliveryfailurecause END) AS demat_recapito_deliveryfailurecause,
                MAX(CASE WHEN tipo = 'DEMAT_RECAPITO' THEN statusdatetime END) AS demat_recapito_data,
                MAX(CASE WHEN tipo = 'DEMAT_RECAPITO' THEN clientrequesttimestamp END) AS demat_recapito_rendicontazione
            FROM temp_silver_postalizzazione
            WHERE rn = 1
            GROUP BY requestid
        ),

        temp_silver_notification AS (
            SELECT
                sn.iun,
                sn.senderdenomination
            FROM send.silver_notification sn
            INNER JOIN perimetro_iun p
                ON sn.iun = p.iun
        ),

        payment_timeline AS (
            -- Recupera l'evidenza di pagamento dalla timeline SILVER per gli IUN del perimetro.
            SELECT
                tl.iun,
                -- SILVER PAYMENT: eventTimestamp e' UTC (Z); per il confronto
                -- con GOLD si converte una sola volta in orario Europe/Rome.
                -- MAX mantiene la selezione dell'ultimo PAYMENT per IUN.
                MAX(
                    FROM_UTC_TIMESTAMP(
                        CAST(REPLACE(REGEXP_REPLACE(
                            get_json_object(tl.details, '$.eventTimestamp'),
                            'Z$', ''), 'T', ' ') AS TIMESTAMP),
                        'Europe/Rome'
                    )
                ) AS tms_payment_silver
            FROM send.silver_timeline tl
            INNER JOIN perimetro_iun p
                ON tl.iun = p.iun
            WHERE tl.category = 'PAYMENT'
            GROUP BY tl.iun
        ),

        temp_gold_notification_analytics AS (
            SELECT
                n.iun,
                n.tms_viewed,
                n.tms_effective_date,
                n.tms_date_payment AS tms_payment_gold,
                n.type_notif
            FROM send.gold_notification_analytics n
            INNER JOIN perimetro_iun p
                ON n.iun = p.iun
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
                CONCAT("'", s.codice_oggetto) AS codiceoggetto,
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
                s.demat_documenttype_da_controllare,
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
                s.tms_cancelled,
                n.tms_payment_gold,
                pt.tms_payment_silver,
                CASE
                    WHEN n.tms_payment_gold IS NULL THEN pt.tms_payment_silver
                    WHEN pt.tms_payment_silver IS NULL THEN n.tms_payment_gold
                    ELSE LEAST(n.tms_payment_gold, pt.tms_payment_silver)
                END AS tms_date_payment,
                s.flag_wi7_consolidatore,
                s.flag_wi7_report_postalizzazioni_incomplete,
                s.wi7_cluster,
                s.pcretry_rank,
                s.attempt_rank,
                s.attempt_number,
                -- Confronto tra DATE, senza conversione preliminare in UTC.
                CASE
                    WHEN s.prodotto = 'AR' AND CAST(s.requesttimestamp AS DATE) >= DATE '2026-07-28' THEN 1
                    WHEN s.prodotto = 'RS' AND CAST(s.requesttimestamp AS DATE) >= DATE '2026-10-15' THEN 1
                    WHEN s.prodotto IN ('RIR','RIS') AND CAST(s.requesttimestamp AS DATE) >= DATE '2026-10-28' THEN 1
                    -- 890: non risulta una data di attivazione.
                    ELSE 0
                END AS flag_tracker_attivo,
                t.certificazione_recapito_stato AS certificazione_recapito_stato_silver,
                t.certificazione_recapito_data AS certificazione_recapito_data_silver,
                t.certificazione_recapito_rendicontazione AS certificazione_recapito_rendicontazione_silver,
                t.fine_recapito_stato AS fine_recapito_stato_silver,
                t.fine_recapito_deliveryfailurecause AS fine_recapito_deliveryfailurecause_silver,
                t.fine_recapito_data AS fine_recapito_data_silver,
                t.fine_recapito_rendicontazione AS fine_recapito_rendicontazione_silver,
                t.demat_recapito_stato AS demat_stato_silver,
                t.demat_recapito_deliveryfailurecause AS demat_recapito_deliveryfailurecause_silver,
                t.demat_recapito_data AS demat_data_silver,
                t.demat_recapito_rendicontazione AS demat_rendicontazione_silver,
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
                        WHEN s.certificazione_recapito_stato IN ('RECRI004A','RECRSI004A')
                            AND s.certificazione_recapito_dettagli IS NULL THEN 1
                        WHEN s.certificazione_recapito_stato NOT IN ('RECRS002A','RECRN002A','RECAG003A','RECRS002D','RECRN002D','RECAG003D','RECRI004A','RECRSI004A')
                            AND s.certificazione_recapito_dettagli IN ('M01','M02','M03','M04','M05','M06','M07','M08','M09') THEN 1
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
                        WHEN s.certificazione_recapito_stato IN (
                            'RECRS002A','RECRN002A','RECAG003A',
                            'RECRS002D','RECRN002D','RECAG003D'
                        ) AND (
                            s.certificazione_recapito_dettagli IS DISTINCT FROM t.demat_recapito_deliveryfailurecause
                            OR t.demat_recapito_deliveryfailurecause IS DISTINCT FROM t.fine_recapito_deliveryfailurecause
                        ) THEN 1
                        ELSE 0
                    END AS controllo_causale,
                CASE
                        WHEN s.prodotto IN ('RIR','RIS') THEN 0
                        WHEN s.certificazione_recapito_stato IN ('RECRN003A', 'RECRN004A', 'RECRN005A')
                            AND (s.tentativo_recapito_stato = 'RECRN010' OR s.tentativo_recapito_stato IS NULL) THEN 0
                        WHEN s.certificazione_recapito_stato IN ('RECRS003C', 'RECRS004A', 'RECRS005A')
                            AND (s.tentativo_recapito_stato = 'RECRS010' OR s.tentativo_recapito_stato IS NULL) THEN 0
                        WHEN s.certificazione_recapito_stato IN ('RECAG005A','RECAG006A','RECAG007A','RECAG008A')
                            AND (s.tentativo_recapito_stato = 'RECAG010' OR s.tentativo_recapito_stato IS NULL) THEN 0
                        WHEN s.certificazione_recapito_stato NOT IN ('RECRN003A','RECRN004A','RECRN005A','RECAG005A','RECRS003C','RECRS004A','RECRS005A','RECAG006A','RECAG007A','RECAG008A')
                            AND s.tentativo_recapito_stato = s.certificazione_recapito_stato THEN 0
                        ELSE 1
                    END AS controllo_inesito_casi_giacenza,
                CASE
                        -- Casi con solo stato finale: non si richiede una tripletta A/B/C.
                        WHEN s.fine_recapito_stato IN ('RECRS001C','RECRS003C', 'RECRSI003C') THEN 0
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
                                AND (
                                    s.demat_23l_ar_stato IN ('RECAG011B', 'RECAG005B')
                                    OR s.demat_plico_stato IN ('RECAG011B', 'RECAG005B')
                                    OR s.demat_arcad_stato IN ('RECAG011B', 'RECAG005B')
                                )
                                AND t.fine_recapito_stato = 'RECAG005C' THEN 0
                        WHEN s.certificazione_recapito_stato = 'RECAG006A'
                                AND (
                                    s.demat_23l_ar_stato IN ('RECAG011B', 'RECAG006B')
                                    OR s.demat_plico_stato IN ('RECAG011B', 'RECAG006B')
                                    OR s.demat_arcad_stato IN ('RECAG011B', 'RECAG006B')
                                )
                                AND t.fine_recapito_stato = 'RECAG006C' THEN 0
                        WHEN s.certificazione_recapito_stato = 'RECAG007A'
                                AND (
                                    s.demat_23l_ar_stato IN ('RECAG011B', 'RECAG007B')
                                    OR s.demat_plico_stato IN ('RECAG011B', 'RECAG007B')
                                    OR s.demat_arcad_stato IN ('RECAG011B', 'RECAG007B')
                                )
                                AND t.fine_recapito_stato = 'RECAG007C' THEN 0
                        WHEN s.certificazione_recapito_stato = 'RECAG008A'
                                AND (
                                    s.demat_23l_ar_stato IN ('RECAG011B', 'RECAG008B')
                                    OR s.demat_plico_stato IN ('RECAG011B', 'RECAG008B')
                                    OR s.demat_arcad_stato IN ('RECAG011B', 'RECAG008B')
                                )
                                AND t.fine_recapito_stato = 'RECAG008C' THEN 0
                        WHEN s.certificazione_recapito_stato = 'RECRS002A'
                                AND (s.demat_23l_ar_stato = 'RECRS002B' OR s.demat_plico_stato = 'RECRS002B' OR s.demat_arcad_stato = 'RECRS002B')
                                AND t.fine_recapito_stato = 'RECRS002C' THEN 0
                        WHEN s.certificazione_recapito_stato = 'RECRS002D'
                                AND (s.demat_23l_ar_stato = 'RECRS002E' OR s.demat_plico_stato = 'RECRS002E' OR s.demat_arcad_stato = 'RECRS002E')
                                AND t.fine_recapito_stato = 'RECRS002F' THEN 0
                        WHEN s.certificazione_recapito_stato = 'RECRS004A'
                                AND (s.demat_23l_ar_stato = 'RECRS004B' OR s.demat_plico_stato = 'RECRS004B' OR s.demat_arcad_stato = 'RECRS004B')
                                AND t.fine_recapito_stato = 'RECRS004C' THEN 0
                        WHEN s.certificazione_recapito_stato = 'RECRS005A'
                                AND (s.demat_23l_ar_stato = 'RECRS005B' OR s.demat_plico_stato = 'RECRS005B' OR s.demat_arcad_stato = 'RECRS005B')
                                AND t.fine_recapito_stato = 'RECRS005C' THEN 0
                        WHEN s.certificazione_recapito_stato = 'RECRSI004A'
                                AND (s.demat_23l_ar_stato = 'RECRSI004B' OR s.demat_plico_stato = 'RECRSI004B' OR s.demat_arcad_stato = 'RECRSI004B')
                                AND t.fine_recapito_stato = 'RECRSI004C' THEN 0
                        WHEN s.certificazione_recapito_stato = 'RECRI003A'
                                AND (s.demat_23l_ar_stato = 'RECRI003B' OR s.demat_plico_stato = 'RECRI003B' OR s.demat_arcad_stato = 'RECRI003B')
                                AND t.fine_recapito_stato = 'RECRI003C' THEN 0
                        WHEN s.certificazione_recapito_stato = 'RECRI004A'
                                AND (s.demat_23l_ar_stato = 'RECRI004B' OR s.demat_plico_stato = 'RECRI004B' OR s.demat_arcad_stato = 'RECRI004B')
                                AND t.fine_recapito_stato = 'RECRI004C' THEN 0
                        ELSE 1
                    END AS controllo_tripletta,
                CASE
                    -- Casi business in cui è prevista la sola data di fine recapito.
                    WHEN s.fine_recapito_stato IN ('RECRS001C','RECRS003C', 'RECRSI003C') THEN 0
                    WHEN t.demat_recapito_data IS NULL
                         AND CAST(t.certificazione_recapito_data AS TIMESTAMP) = CAST(t.fine_recapito_data AS TIMESTAMP) THEN 0
                    WHEN CAST(t.certificazione_recapito_data AS TIMESTAMP) = CAST(t.demat_recapito_data AS TIMESTAMP)
                         AND CAST(t.demat_recapito_data AS TIMESTAMP) = CAST(t.fine_recapito_data AS TIMESTAMP) THEN 0
                    ELSE 1
                END AS controllo_date_business,
                CASE
                        WHEN s.certificazione_recapito_stato IN ('RECRS005A','RECRN005A')
                        AND DATEDIFF(
                            CAST(s.certificazione_recapito_data AS DATE),
                            CAST(s.tentativo_recapito_data AS DATE)
                        ) < 30 THEN 1
                        ELSE 0
                    END AS controllo_tempistiche_compiuta_giacenza,
                   CASE
                        -- Controllo solo la famiglia DEMAT con data di rendicontazione più recente.
                        WHEN (
                            s.demat_documenttype_da_controllare = '23L_AR'
                            AND s.demat_23l_ar_stato IS NOT NULL
                            AND s.demat_23l_ar_stato NOT IN (
                                'RECRN001B','RECRN003B',
                                'RECAG001B','RECAG002B','RECAG005B','RECAG006B',
                                'RECAG007B','RECAG008B','RECAG011B',
                                'RECRI003B'
                            )
                        ) THEN 1
                        WHEN (
                            s.demat_documenttype_da_controllare = 'PLICO'
                            AND s.demat_plico_stato IS NOT NULL
                            AND s.demat_plico_stato NOT IN (
                                -- RS temporaneamente escluso: 'RECRS002B','RECRS002E','RECRS004B','RECRS005B',
                                'RECRN002B','RECRN002E','RECRN004B','RECRN005B',
                                'RECAG003B','RECAG003E','RECAG007B','RECAG008B','RECAG011B',
                                'RECRI004B'
                                -- RIS temporaneamente escluso: 'RECRSI004B'
                            )
                        ) THEN 1
                        WHEN s.prodotto = '890' AND d.documenttype = '23L' THEN 0
                        WHEN s.prodotto IN ('AR', 'RIR') AND d.documenttype = 'AR' THEN 0
                        WHEN d.documenttype IS NULL THEN 0
                        ELSE 1
                    END AS controllo_documentType
            FROM gold_base_senza_feedback s
            LEFT JOIN temp_demat d
                ON d.requestid = s.requestid
            AND d.rn_demat = 1
            LEFT JOIN temp_silver_notification sn
                ON sn.iun = s.iun
            LEFT JOIN temp_gold_notification_analytics n
                ON s.iun = n.iun
            LEFT JOIN payment_timeline pt
                ON s.iun = pt.iun
            LEFT JOIN temp_ultimi_eventi_silver_postalizzazione t
                ON s.requestid = t.requestid
        ),

        temp_postalizzazione AS (
            SELECT
                s.*,
                -- Per il pre-tracker la sequenza non e' vincolante.
                -- Per il post-tracker la chiusura SILVER deve essere l'ultima
                -- rendicontazione rispetto agli eventi pertinenti, inclusi i GOLD.
                CASE
                    WHEN s.flag_tracker_attivo = 0 THEN 0
                    WHEN s.fine_recapito_rendicontazione_silver IS NULL THEN 1
                    WHEN
                        CAST(s.certificazione_recapito_data_rendicontazione AS TIMESTAMP) > CAST(s.fine_recapito_rendicontazione_silver AS TIMESTAMP)
                        OR CAST(s.demat_23l_ar_data_rendicontazione AS TIMESTAMP) > CAST(s.fine_recapito_rendicontazione_silver AS TIMESTAMP)
                        OR CAST(s.demat_plico_data_rendicontazione AS TIMESTAMP) > CAST(s.fine_recapito_rendicontazione_silver AS TIMESTAMP)
                        OR CAST(s.demat_arcad_data_rendicontazione AS TIMESTAMP) > CAST(s.fine_recapito_rendicontazione_silver AS TIMESTAMP)
                        OR CAST(s.tentativo_recapito_data_rendicontazione AS TIMESTAMP) > CAST(s.fine_recapito_rendicontazione_silver AS TIMESTAMP)
                        OR CAST(s.messaingiacenza_recapito_data_rendicontazione AS TIMESTAMP) > CAST(s.fine_recapito_rendicontazione_silver AS TIMESTAMP)
                        OR CAST(s.accettazione_23l_recag012_data_rendicontazione AS TIMESTAMP) > CAST(s.fine_recapito_rendicontazione_silver AS TIMESTAMP)
                    THEN 1 ELSE 0
                END AS controllo_rendicontazione_ordinata,
                IF(
                    fine_recapito_stato IN (
                        'RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C',
                        'RECAG005C','RECAG006C','RECAG007C','RECAG008C'
                    )
                    AND tentativo_recapito_stato IS NULL,
                    1, 0
                ) AS assenza_inesito,
                IF(
                    fine_recapito_stato IN (
                        'RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C',
                        'RECAG005C','RECAG006C','RECAG007C','RECAG008C'
                    )
                    AND messaingiacenza_recapito_stato IS NULL,
                    1, 0
                ) AS assenza_messa_in_giacenza,
                IF(certificazione_recapito_stato IS NULL, 1, 0) AS assenza_pre_esito,
                IF(
                        fine_recapito_stato NOT IN ('RECRS001C','RECRS003C', 'RECRSI003C')
                        AND (
                            (
                                fine_recapito_stato IN ('RECAG008C')
                                AND (
                                    demat_23l_ar_stato IS NULL
                                    OR demat_plico_stato IS NULL
                                )
                            )
                            OR (
                                demat_23l_ar_stato IS NULL
                                AND demat_plico_stato IS NULL
                            )
                        ),
                        1,
                        0
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
                ) AS assenza_RECAG012,
                    IF(
                        fine_recapito_stato IN ('RECRSI003C','RECRSI004C','RECRI003C','RECRI004C')
                        AND tentativo_recapito_stato IS NULL,
                        1,
                        0
                    ) AS assenza_avviato_estero,
                    IF(
                        fine_recapito_stato IN ('RECRSI003C','RECRSI004C','RECRI003C','RECRI004C')
                        AND messaingiacenza_recapito_stato IS NULL,
                        1,
                        0
                    ) AS assenza_ingresso_paese_estero
            FROM temp_controlli s
        ),

        temp_assenza_eventi AS (
            SELECT
                *,
                IF(
                    assenza_inesito = 1
                    OR assenza_messa_in_giacenza = 1
                    OR assenza_pre_esito = 1
                    OR assenza_dematerializzazione_23l_ar_plico = 1
                    OR assenza_demat_ARCAD = 1
                    OR assenza_RECAG012 = 1
                    OR assenza_avviato_estero = 1
                    OR assenza_ingresso_paese_estero = 1,
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
                t.codiceoggetto,
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
                t.tms_payment_gold,
                t.tms_payment_silver,
                t.tms_date_payment,
                t.tms_cancelled,
                t.flag_wi7_consolidatore,
                t.flag_wi7_report_postalizzazioni_incomplete,
                t.wi7_cluster,
                t.pcretry_rank,
                t.attempt_rank,
                t.attempt_number,
                t.certificazione_recapito_stato_silver,
                t.certificazione_recapito_data_silver,
                t.certificazione_recapito_rendicontazione_silver,
                t.fine_recapito_stato_silver,
                t.fine_recapito_deliveryfailurecause_silver,
                t.fine_recapito_data_silver,
                t.fine_recapito_rendicontazione_silver,
                t.demat_stato_silver,
                t.demat_recapito_deliveryfailurecause_silver,
                t.demat_data_silver,
                t.demat_rendicontazione_silver,
                t.flag_multi_destinatario,
                t.flag_ultima_postalizzazione,
                t.flag_prodotto_estero,
                t.flag_destinatario_deceduto,
                t.flag_esiti_mancanti,
                t.demat_documenttype_da_controllare,
                t.controllo_causale,
                t.controllo_date_business,
                t.flag_tracker_attivo,
                t.controllo_rendicontazione_ordinata,
                t.controllo_tripletta,
                t.controllo_tempistiche_compiuta_giacenza,
                t.controllo_inesito_casi_giacenza,
                t.controllo_documentType,
                t.assenza_inesito,
                t.assenza_messa_in_giacenza,
                t.assenza_pre_esito,
                t.assenza_dematerializzazione_23l_ar_plico,
                t.assenza_demat_ARCAD,
                t.assenza_RECAG012,
                t.assenza_avviato_estero,
                t.assenza_ingresso_paese_estero,
                CASE
                    WHEN controllo_causale = 1
                    OR controllo_date_business = 1
                    OR controllo_rendicontazione_ordinata = 1
                    OR controllo_tripletta = 1
                    OR controllo_tempistiche_compiuta_giacenza = 1
                    OR controllo_inesito_casi_giacenza = 1
                    OR controllo_documentType = 1
                    THEN 1
                    ELSE 0
                END AS flag_errore_rendicontazione,
                CASE
                    WHEN CAST(t.attempt_number AS INT) = 0 THEN 'Rendicontazione corretta - Bloccati al primo attempt'
                    WHEN CAST(t.attempt_number AS INT) = 1 THEN 'Rendicontazione corretta - Bloccati al secondo attempt'
                    ELSE NULL
                END AS dettaglio_rendicontazione,
                CASE
                    WHEN (flag_wi7_report_postalizzazioni_incomplete = 1 OR flag_wi7_consolidatore = 1)
                        THEN 'Oggetto fuori sla'
                    WHEN (
                        controllo_causale = 1
                        OR controllo_date_business = 1
                        OR controllo_rendicontazione_ordinata = 1
                        OR controllo_tripletta = 1
                        OR controllo_tempistiche_compiuta_giacenza = 1
                        OR controllo_inesito_casi_giacenza = 1
                        OR flag_esiti_mancanti = 1
                        OR controllo_documentType = 1
                    )
                    AND fine_recapito_stato IS NOT NULL
                        THEN 'Errore rendicontazione/assenza eventi intermedi'
                    WHEN (
                        controllo_causale = 0
                        AND controllo_date_business = 0
                        AND controllo_rendicontazione_ordinata = 0
                        AND controllo_tripletta = 0
                        AND controllo_tempistiche_compiuta_giacenza = 0
                        AND controllo_inesito_casi_giacenza = 0
                        AND assenza_inesito = 0
                        AND assenza_RECAG012 = 0
                        AND assenza_pre_esito = 0
                        AND assenza_dematerializzazione_23l_ar_plico = 0
                        AND assenza_messa_in_giacenza = 0
                        AND assenza_demat_arcad = 0
                        AND assenza_avviato_estero = 0
                        AND assenza_ingresso_paese_estero = 0
                        AND controllo_documentType = 0
                    )
                        THEN 'Oggetto con rendicontazione corretta'
                    ELSE 'Oggetto in corso di postalizzazione'
                END AS cluster_mancato_feedback_analogico
            FROM temp_assenza_eventi t
        ),

        finale_filtrato AS (
            SELECT
                CAST(CURRENT_DATE() AS STRING) AS data_estrazione,
                CAST(CURRENT_DATE() AS STRING) AS data_prima_estrazione,
                CONCAT(
                    DATE_FORMAT(DATE_SUB(CURRENT_DATE(), PMOD(DAYOFWEEK(CURRENT_DATE()) + 5, 7)), 'yyyyMMdd'),
                    ' - ',
                    DATE_FORMAT(DATE_ADD(DATE_SUB(CURRENT_DATE(), PMOD(DAYOFWEEK(CURRENT_DATE()) + 5, 7)), 6), 'yyyyMMdd')
                ) AS settimana_prima_estrazione,
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
                tms_cancelled,
                flag_wi7_consolidatore,
                flag_wi7_report_postalizzazioni_incomplete,
                wi7_cluster,
                pcretry_rank,
                attempt_rank,
                attempt_number,
                certificazione_recapito_stato_silver,
                certificazione_recapito_data_silver,
                certificazione_recapito_rendicontazione_silver,
                fine_recapito_stato_silver,
                fine_recapito_deliveryfailurecause_silver,
                fine_recapito_data_silver,
                fine_recapito_rendicontazione_silver,
                demat_stato_silver,
                demat_recapito_deliveryfailurecause_silver,
                demat_data_silver,
                demat_rendicontazione_silver,
                demat_documenttype_da_controllare,
                flag_esiti_mancanti,
                flag_errore_rendicontazione,
                controllo_causale,
                controllo_date_business,
                flag_tracker_attivo,
                controllo_rendicontazione_ordinata,
                controllo_tripletta,
                controllo_tempistiche_compiuta_giacenza,
                controllo_inesito_casi_giacenza,
                controllo_documentType,
                assenza_inesito,
                assenza_messa_in_giacenza,
                assenza_pre_esito,
                assenza_dematerializzazione_23l_ar_plico,
                assenza_demat_ARCAD,
                assenza_RECAG012,
                assenza_avviato_estero,
                assenza_ingresso_paese_estero,
                tms_payment_gold,
                tms_payment_silver,
                tms_date_payment,
                cluster_mancato_feedback_analogico,
                dettaglio_rendicontazione
            FROM finale
            WHERE flag_ultima_postalizzazione = 1
            AND (
                    COALESCE(certificazione_recapito_stato, '') NOT IN (
                        'RECRS006','RECRS013','RECRN006','RECRN013','RECAG004','RECAG013'
                    )
                    OR certificazione_recapito_stato IS NULL
            )
        )

        SELECT *
        FROM finale_filtrato
        WHERE 1=1
        AND cluster_mancato_feedback_analogico = 'Oggetto con rendicontazione corretta'
        ;
    """
    return query


def run_query(spark: SparkSession):
    logging.info("Esecuzione query base...")
    query_sql = build_query_sql()
    return spark.sql(query_sql)


# ---------------- STORICO E RIEPILOGO ----------------
def local_run_date(start_utc: datetime) -> str:
    """Data italiana di riferimento per tutti i tab del medesimo run."""
    return start_utc.astimezone(LOCAL_TZ).date().isoformat()


def week_label(first_date: str) -> str:
    """Intervallo lunedi'-domenica, estremi inclusi: YYYYMMDD - YYYYMMDD."""
    d = datetime.strptime(str(first_date)[:10], "%Y-%m-%d").date()
    monday = d - timedelta(days=d.weekday())
    sunday = monday + timedelta(days=6)
    return f"{monday:%Y%m%d} - {sunday:%Y%m%d}"


def attach_first_detection(
    df_current: pd.DataFrame, df_previous: pd.DataFrame, today: str
):
    """Conserva solo la prima osservazione; non conserva righe uscite dallo stock.

    Se lo storico non ha ancora il nuovo schema, riparte una baseline controllata.
    Il confronto e' interno: non filtra mai lo stock destinato a GO.
    """
    must_have = {"requestid", COL_FIRST_DATE, COL_FIRST_WEEK}
    old_valid_schema = must_have.issubset(df_previous.columns)
    baseline = not old_valid_schema
    result = df_current.copy()
    if "requestid" not in result.columns:
        raise ValueError("Colonna requestid assente dall'estrazione corrente")
    if result["requestid"].duplicated().any():
        raise ValueError(
            "requestid duplicati nell'estrazione: impossibile tracciare lo storico"
        )
    # Il giorno del job sostituisce il campo tecnico della query manuale.
    result["data_estrazione"] = today
    result[COL_FIRST_DATE] = today
    result[COL_FIRST_WEEK] = week_label(today)
    old_ids = set()
    if old_valid_schema and not df_previous.empty:
        old = df_previous.loc[:, ["requestid", COL_FIRST_DATE, COL_FIRST_WEEK]].copy()
        old["requestid"] = old["requestid"].astype(str)
        if old["requestid"].duplicated().any():
            raise ValueError(
                "requestid duplicati nel GSheet precedente: non aggiorno lo storico"
            )
        old_ids = set(old["requestid"])
        old = old.set_index("requestid")
        for idx in result.index:
            requestid = str(result.at[idx, "requestid"])
            if requestid not in old.index:
                continue
            first_date = str(old.at[requestid, COL_FIRST_DATE]).strip()
            first_week = str(old.at[requestid, COL_FIRST_WEEK]).strip()
            # Solo date coerenti. Se lo storico contiene valori incompleti,
            # non inserire valori indeterminati nel nuovo tab.
            try:
                expected_week = week_label(first_date)
            except (ValueError, TypeError):
                continue
            if first_week != expected_week:
                continue
            result.at[idx, COL_FIRST_DATE] = first_date
            result.at[idx, COL_FIRST_WEEK] = first_week
    current_ids = set(result["requestid"].astype(str))
    new = len(current_ids - old_ids)
    exited = len(old_ids - current_ids)
    # Nella prima esecuzione con il nuovo schema non esiste una base storica
    # confrontabile: non presentare 0 come numero certo di oggetti usciti.
    # Nei run successivi il conteggio e' una differenza tra gli stock, non
    # una prova di avvenuta generazione del feedback analogico.
    return result, baseline, new, "N/A - baseline iniziale" if baseline else exited


def weekly_summary(df_current: pd.DataFrame) -> pd.DataFrame:
    """Conta le righe ancora presenti, a granularita' requestid, per prima settimana."""
    if df_current.empty:
        return pd.DataFrame(columns=[COL_FIRST_WEEK, "numero_oggetti"])
    return (
        df_current.groupby(COL_FIRST_WEEK, as_index=False)["requestid"]
        .count()
        .rename(columns={"requestid": "numero_oggetti"})
        .sort_values(COL_FIRST_WEEK)
        .reset_index(drop=True)
    )


def main():
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    start = now_utc_dt()
    today = local_run_date(start)
    start_str = start.strftime("%Y-%m-%d %H:%M:%S")
    webhook = get_slack_webhook_by_name(
        load_slack_webhooks(SLACK_CONFIG_CANDIDATE_PATHS), SLACK_WEBHOOK_NAME
    )
    creds = None
    frames = []
    log = dict(
        max_date_str="-",
        start_run_str=start_str,
        end_run_str="-",
        elapsed_min_str="IN CORSO",
        stato_run="IN CORSO",
        righe_settimana_precedente="N/A",
        righe_settimana_corrente=0,
        oggetti_nuovi_rispetto_precedente=0,
        primo_attempt_corrente=0,
        secondo_attempt_corrente=0,
        tracker_attivo_corrente=0,
        usciti_da_stock=0,
    )
    try:
        creds = load_google_credentials(GOOGLE_SECRET_PATH)
        sheet = Sheet(
            sheet_id=SHEET_ID_OUTPUT, service_credentials=creds, id_mode="key"
        )
        # Lettura PRIMA di qualsiasi scrittura: non perdere mai le prime date
        # se Google Sheets non e' accessibile.
        previous = read_previous_tab(sheet, TAB_OUTPUT)
        log["righe_settimana_precedente"] = len(previous)
        log["max_date_str"] = get_last_update_date(spark)
        logging.info(
            "Avvio estrazione completa %s; prodotti attivi: %s", today, ACTIVE_PRODUCTS
        )
        df_raw = run_query(spark).persist(StorageLevel.MEMORY_AND_DISK)
        frames.append(df_raw)
        # La GOLD e' a granularita' requestid. I join non devono moltiplicare
        # le righe; non si applica alcuna deduplica e non si espongono metriche
        # pre/post DISTINCT, non pertinenti a questo monitoraggio.
        count_post = df_raw.count()
        log["righe_settimana_corrente"] = count_post
        # Un'unica conversione per il dettaglio, il CSV, le metriche e il riepilogo.
        df_pd = df_raw.toPandas()
        required = {
            "requestid",
            "attempt_number",
            "flag_tracker_attivo",
            "controllo_rendicontazione_ordinata",
            "fine_recapito_stato_silver",
            "fine_recapito_data_silver",
            "dettaglio_rendicontazione",
        }
        missing = required - set(df_pd.columns)
        if missing:
            raise ValueError(f"Mancano colonne necessarie: {sorted(missing)}")
        # La SQL attribuisce i nomi descrittivi in modo centralizzato.
        # Qui verifichiamo solo i due attempt operativi e l'etichetta valorizzata.
        attempts = pd.to_numeric(df_pd["attempt_number"], errors="coerce")
        if (
            not attempts.isin([0, 1]).all()
            or df_pd["dettaglio_rendicontazione"].isna().any()
        ):
            raise ValueError(
                "Attempt o dettaglio rendicontazione inattesi nell'estrazione"
            )
        # Tutti gli oggetti restituiti devono superare il nuovo controllo.
        if (
            pd.to_numeric(df_pd["controllo_rendicontazione_ordinata"], errors="coerce")
            .ne(0)
            .any()
        ):
            raise ValueError(
                "Controllo ordine KO in un report di rendicontazione corretta"
            )
        enriched, baseline, new, exited = attach_first_detection(df_pd, previous, today)
        log["oggetti_nuovi_rispetto_precedente"] = (
            "N/A - baseline iniziale" if baseline else new
        )
        log["usciti_da_stock"] = exited
        if baseline:
            logging.info(
                "Baseline iniziale: storico precedente con %d righe ma senza nuove colonne di prima rilevazione; "
                "non e' possibile quantificare gli oggetti usciti o i nuovi rispetto al precedente schema.",
                len(previous),
            )
        log["primo_attempt_corrente"] = int(attempts.eq(0).sum())
        log["secondo_attempt_corrente"] = int(attempts.eq(1).sum())
        log["tracker_attivo_corrente"] = int(
            pd.to_numeric(enriched["flag_tracker_attivo"], errors="coerce").eq(1).sum()
        )
        logging.info(
            "Stock %d (attempt 0=%d, attempt 1=%d); tracker attivo=%d; nuovi=%d; non piu' presenti=%s; baseline=%s",
            count_post,
            log["primo_attempt_corrente"],
            log["secondo_attempt_corrente"],
            log["tracker_attivo_corrente"],
            new,
            exited,
            baseline,
        )
        weekly = weekly_summary(enriched)
        if int(weekly["numero_oggetti"].sum()) != count_post:
            raise ValueError("Riepilogo settimanale non coerente con lo stock")
        # Il CSV non passa MAI dal confronto storico. Solo tre colonne, stock completo.
        csv_three = enriched[
            ["requestid", "fine_recapito_stato_silver", "fine_recapito_data_silver"]
        ]
        filename = f"{start.astimezone(LOCAL_TZ):%Y%m%d}_{CSV_FILE_BASENAME}_senza_feedback.csv"
        # Prima il CSV GO, poi i tab interni: monitoriamo eventuali fallimenti.
        drive_file = upload_csv_to_drive(
            csv_three, creds, DRIVE_FOLDER_ID, filename, CSV_SEPARATOR
        )
        export_to_sheets(enriched, creds, SHEET_ID_OUTPUT, TAB_OUTPUT)
        export_to_sheets(weekly, creds, SHEET_ID_OUTPUT, TAB_WEEKLY)
        end = now_utc_dt()
        log.update(
            end_run_str=end.strftime("%Y-%m-%d %H:%M:%S"),
            elapsed_min_str=elapsed_minutes_str(start, end),
            stato_run="OK - BASELINE" if baseline else "OK",
        )
        write_meta_tab(creds=creds, **log)
        message = [
            build_slack_prefix(),
            "✅ *SUCCESS*",
            f"*Job:* {APP_NAME}",
            f"*Modalità:* {'baseline' if baseline else 'stock completo'}",
            f"*Stock CSV GO:* {count_post}",
            f"*Primo attempt:* {log['primo_attempt_corrente']}",
            f"*Secondo attempt:* {log['secondo_attempt_corrente']}",
            f"*Tracker attivo (post-attivazione):* {log['tracker_attivo_corrente']}",
            f"*Nuovi rispetto alla precedente estrazione:* {'N/A (baseline: ' + str(new) + ' oggetti correnti)' if baseline else new}",
            f"*Non più presenti rispetto alla precedente estrazione:* {exited}",
            f"*CSV Drive:* {drive_file.get('webViewLink', '-')}",
            f"*Aggiornamento dati:* {log['max_date_str']}",
        ]
        send_slack(webhook, "\n".join(message))
        logging.info(
            "Job completato; nessuna logica delta applicata al CSV o al tab corrente"
        )
    except Exception:
        logging.exception("Errore nella generazione del report senza feedback")
        if creds is not None:
            try:
                log.update(
                    end_run_str=now_utc_str(),
                    elapsed_min_str=elapsed_minutes_str(start),
                    stato_run="KO",
                )
                write_meta_tab(creds=creds, **log)
            except Exception:
                logging.exception("Impossibile aggiornare il log di controllo in KO")
        try:
            send_slack(
                webhook,
                "\n".join([build_slack_prefix(), "❌ *FAILURE*", f"*Job:* {APP_NAME}"]),
            )
        finally:
            raise
    finally:
        for frame in reversed(frames):
            try:
                frame.unpersist()
            except Exception:
                logging.exception("Impossibile liberare DataFrame persistito")
        spark.stop()


if __name__ == "__main__":
    main()

# =============================================================================
# QUERY SQL AUTONOMA - IMPALA / DBEAVER
# -- SSDA-1198 | Query manuale completa per Impala / DBeaver
# -- Business logic equivalente a build_query_sql() del job Spark; nessuna dipendenza da Spark SQL.
# -- GOLD = TIMESTAMP in orario italiano (campione Impala 20/11/2026).
# -- SILVER = stringhe UTC ISO-8601 con suffisso Z, convertite una sola volta in Europe/Rome.
# -- L'estrazione autonoma non ricostruisce la prima rilevazione dal GSheet.
# -- ATTENZIONE: CURRENT_DATE() nella sessione Impala dipende dal calendario del server;
# -- per un run vicino alla mezzanotte verificare la data effettiva di esecuzione.
# -- Attuali prodotti: AR, RIR, 890; RS/RIS non abilitati.
# -- Non aggiungere LIMIT: l'estrazione manuale deve comprendere lo stock completo.
#
# WITH gold_base_raw AS (
#             SELECT
#                 s.senderpaid,
#                 s.iun,
#                 s.requestid,
#                 s.requesttimestamp,
#                 s.prodotto,
#                 s.geokey,
#                 s.recapitista_unificato,
#                 s.lotto,
#                 s.codice_oggetto,
#                 s.scarto_consolidatore_stato,
#                 s.scarto_consolidatore_data,
#                 s.affido_recapitista_con016_data,
#                 s.accettazione_recapitista_con018_data,
#                 s.data_deposito,
#                 s.tentativo_recapito_stato,
#                 s.tentativo_recapito_data,
#                 s.tentativo_recapito_data_rendicontazione,
#                 s.messaingiacenza_recapito_stato,
#                 s.messaingiacenza_recapito_data,
#                 s.messaingiacenza_recapito_data_rendicontazione,
#                 s.certificazione_recapito_stato,
#                 s.certificazione_recapito_dettagli,
#                 s.certificazione_recapito_data,
#                 s.certificazione_recapito_data_rendicontazione,
#                 s.demat_23l_ar_stato,
#                 s.demat_23l_ar_data_rendicontazione,
#                 s.demat_plico_stato,
#                 s.demat_plico_data_rendicontazione,
#                 CASE
#                     WHEN s.demat_23l_ar_data_rendicontazione IS NULL
#                      AND s.demat_plico_data_rendicontazione IS NULL THEN NULL
#                     WHEN s.demat_23l_ar_data_rendicontazione IS NOT NULL
#                      AND s.demat_plico_data_rendicontazione IS NULL THEN '23L_AR'
#                     WHEN s.demat_23l_ar_data_rendicontazione IS NULL
#                      AND s.demat_plico_data_rendicontazione IS NOT NULL THEN 'PLICO'
#                     WHEN s.demat_plico_data_rendicontazione
#                          > s.demat_23l_ar_data_rendicontazione THEN 'PLICO'
#                     ELSE '23L_AR'
#                 END AS demat_documenttype_da_controllare,
#                 s.demat_arcad_stato,
#                 s.demat_arcad_data_rendicontazione,
#                 s.fine_recapito_stato,
#                 s.fine_recapito_data,
#                 s.fine_recapito_data_rendicontazione,
#                 s.accettazione_23l_recag012_data,
#                 s.accettazione_23l_recag012_data_rendicontazione,
#                 s.recindex_number,
#                 -- Campi nativi del perfezionamento: informativi e indipendenti
#                 -- dalla verifica sulla presenza di SEND_ANALOG_FEEDBACK.
#                 s.perfezionamento_data,
#                 s.perfezionamento_tipo,
#                 s.perfezionamento_notificationdate,
#                 s.perfezionamento_stato,
#                 s.perfezionamento_stato_dettagli,
#                 s.tms_cancelled,
#                 s.flag_wi7_consolidatore,
#                 s.flag_wi7_report_postalizzazioni_incomplete,
#                 s.wi7_cluster,
#                 s.pcretry_rank,
#                 s.attempt_rank,
#                 s.attempt_number,
#                 s.flag_prodotto_estero
#             FROM send.gold_postalizzazione_analytics s
#             WHERE s.scarto_consolidatore_stato IS NULL
#             AND COALESCE(s.statusrequest, '') NOT IN (
#                     'PN999', 'PN998', 'error', 'internalError', 'syntaxError',
#                     'transformationError', 'semanticError', 'authenticationError', 'duplicatedRequest'
#             )
#             AND s.attempt_rank = 1
#             AND s.pcretry_rank = 1
#             AND s.tms_cancelled IS NULL
#             AND COALESCE(s.certificazione_recapito_stato, '') NOT IN (
#                         'RECRS006','RECRS013','RECRN006','RECRN013','RECAG004','RECAG013'
#                     )
#             AND s.fine_recapito_data_rendicontazione IS NOT NULL
#             AND CAST(s.fine_recapito_data_rendicontazione AS DATE) < DATE_SUB(CURRENT_DATE(), 2)
#             -- RS e RIS temporaneamente esclusi dal perimetro corrente.
#             -- Per riattivarli estendere il perimetro e decommentare gli status dedicati sotto.
#             AND s.prodotto IN ('AR', 'RIR', '890')
#         ),
#         perimetro_iun_raw AS (
#             SELECT DISTINCT
#                 iun
#             FROM gold_base_raw
#         ),
#         returned_to_sender_iun AS (
#             SELECT
#                 n.iun
#             FROM  perimetro_iun_raw p
#             INNER JOIN send.gold_notification_analytics n
#                 ON n.iun = p.iun
#             WHERE n.tms_returned_to_sender IS NOT NULL
#         ),
#         gold_base AS (
#             SELECT
#                 gb.*
#             FROM gold_base_raw gb
#             LEFT ANTI JOIN returned_to_sender_iun rts
#                 ON gb.iun = rts.iun
#         ),
#         perimetro_iun_base AS (
#             SELECT DISTINCT iun FROM gold_base
#         ),
#         send_analog_feedback AS (
#             SELECT
#                 tl.iun,
#                 CAST(REGEXP_EXTRACT(tl.timelineelementid, 'RECINDEX_([0-9]+)', 1) AS INT) AS recindex_number,
#                 CAST(REGEXP_EXTRACT(tl.timelineelementid, 'ATTEMPT_([0-9]+)', 1) AS INT) AS attempt_number
#             FROM send.silver_timeline tl
#             INNER JOIN perimetro_iun_base p ON tl.iun = p.iun
#             WHERE tl.category = 'SEND_ANALOG_FEEDBACK'
#         ),
#         -- Il vincolo sull'assenza di feedback e' del report operativo, non dei
#         -- controlli di correttezza della rendicontazione né del cluster.
#         gold_base_senza_feedback AS (
#             SELECT gb.*
#             FROM gold_base gb
#             LEFT ANTI JOIN send_analog_feedback saf
#                 ON gb.iun = saf.iun
#                 AND CAST(gb.recindex_number AS INT) = saf.recindex_number
#                 AND CAST(gb.attempt_number AS INT) = saf.attempt_number
#             WHERE CAST(gb.attempt_number AS INT) IN (0, 1)
#         ),
#         perimetro_requestid AS (
#             SELECT DISTINCT requestid, iun, CAST(attempt_number AS INT) AS attempt_number
#             FROM gold_base_senza_feedback
#         ),
#         perimetro_iun AS (
#             SELECT DISTINCT iun FROM perimetro_requestid
#         ),
#
#         -- SILVER: statusdatetime/clientrequesttimestamp sono stringhe UTC con Z.
#         -- La conversione a Europe/Rome avviene UNA SOLA VOLTA in temp_silver_postalizzazione.
#         silver_eventi_postalizzazione AS (
#             SELECT
#                 REGEXP_REPLACE(p.requestid, '^pn-cons-000~', '') AS requestid,
#                 ev.item.paperprogrstatus.statuscode AS statuscode,
#                 ev.item.paperprogrstatus.deliveryfailurecause AS deliveryfailurecause_raw,
#                 ev.item.paperprogrstatus.statusdatetime AS statusdatetime_raw,
#                 ev.item.paperprogrstatus.clientrequesttimestamp AS clientrequesttimestamp_raw
#             FROM send.silver_postalizzazione p
#             INNER JOIN p.eventslist ev
#             INNER JOIN perimetro_requestid pr
#                 ON REGEXP_REPLACE(p.requestid, '^pn-cons-000~', '') = pr.requestid
#         ),
#
#         -- Impala: espansione degli ARRAY annidati direttamente dalla tabella,
#         -- senza propagare attachments come campo complesso tra CTE.
#         temp_demat AS (
#             SELECT requestid, documenttype, rn_demat
#             FROM (
#                 SELECT
#                     REGEXP_REPLACE(p.requestid, '^pn-cons-000~', '') AS requestid,
#                     a.item.documenttype AS documenttype,
#                     ROW_NUMBER() OVER (
#                         PARTITION BY REGEXP_REPLACE(p.requestid, '^pn-cons-000~', '')
#                         ORDER BY ev.item.paperprogrstatus.clientrequesttimestamp DESC
#                     ) AS rn_demat
#                 FROM send.silver_postalizzazione p
#                 INNER JOIN p.eventslist ev
#                 INNER JOIN ev.item.paperprogrstatus.attachments a
#                 INNER JOIN perimetro_requestid pr
#                     ON REGEXP_REPLACE(p.requestid, '^pn-cons-000~', '') = pr.requestid
#                 WHERE a.item.documenttype IN ('23L', 'AR')
#             ) x
#         ),
#
#         temp_silver_postalizzazione AS (
#             SELECT
#                 requestid,
#                 statuscode,
#                 CASE WHEN deliveryfailurecause_raw IN
#                     ('M01','M02','M03','M04','M05','M06','M07','M08','M09')
#                     THEN deliveryfailurecause_raw ELSE NULL END AS deliveryfailurecause,
#                 -- SILVER: stringa ISO-8601 UTC (Z) -> timestamp con orario italiano.
#                 -- Si rimuove Z PRIMA del CAST per evitare la doppia conversione.
#                 FROM_UTC_TIMESTAMP(
#                     CAST(REPLACE(REGEXP_REPLACE(
#                         CASE
#                             WHEN LENGTH(statusdatetime_raw) = 17 THEN CONCAT(
#                                 SUBSTR(statusdatetime_raw, 1, 16), ':00Z'
#                             )
#                             ELSE statusdatetime_raw
#                         END, 'Z$', ''), 'T', ' ') AS TIMESTAMP),
#                     'Europe/Rome'
#                 ) AS statusdatetime,
#                 -- SILVER: stringa ISO-8601 UTC (Z) -> timestamp con orario italiano.
#                 -- Si rimuove Z PRIMA del CAST per evitare la doppia conversione.
#                 FROM_UTC_TIMESTAMP(
#                     CAST(REPLACE(REGEXP_REPLACE(
#                         CASE
#                             WHEN LENGTH(clientrequesttimestamp_raw) = 17 THEN CONCAT(
#                                 SUBSTR(clientrequesttimestamp_raw, 1, 16), ':00.000Z'
#                             )
#                             ELSE clientrequesttimestamp_raw
#                         END, 'Z$', ''), 'T', ' ') AS TIMESTAMP),
#                     'Europe/Rome'
#                 ) AS clientrequesttimestamp,
#                 tipo,
#                 ROW_NUMBER() OVER (
#                     PARTITION BY requestid, tipo
#                     ORDER BY clientrequesttimestamp_raw DESC
#                 ) AS rn
#             FROM (
#                 SELECT
#                     se.requestid,
#                     se.statuscode,
#                     se.deliveryfailurecause_raw,
#                     se.statusdatetime_raw,
#                     se.clientrequesttimestamp_raw,
#                     CASE
#                         WHEN se.statuscode IN (
#                             -- RS temporaneamente escluso:
#                             -- 'RECRS001C','RECRS002C','RECRS002F','RECRS003C','RECRS004C','RECRS005C',
#                             'RECRN001C','RECRN002C','RECRN002F','RECRN003C','RECRN004C','RECRN005C',
#                             'RECAG001C','RECAG002C','RECAG003C','RECAG003F','RECAG005C','RECAG006C','RECAG007C','RECAG008C',
#                             -- RIS temporaneamente escluso: 'RECRSI003C','RECRSI004C',
#                             'RECRI003C','RECRI004C'
#                         ) THEN 'FINE_RECAPITO'
#                         WHEN se.statuscode IN (
#                             -- RS temporaneamente escluso:
#                             -- 'RECRS001C','RECRS002A','RECRS002D','RECRS003C','RECRS004A','RECRS005A',
#                             'RECRN001A','RECRN002A','RECRN002D','RECRN003A','RECRN004A','RECRN005A',
#                             'RECAG001A','RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A','RECAG007A','RECAG008A',
#                             -- RIS temporaneamente escluso: 'RECRSI003C','RECRSI004A',
#                             'RECRI003A','RECRI004A'
#                         ) THEN 'CERTIFICAZIONE_RECAPITO'
#                         WHEN se.statuscode IN (
#                             -- RS temporaneamente escluso: 'RECRS002B','RECRS002E','RECRS004B','RECRS005B',
#                             'RECRN001B','RECRN002B','RECRN002E','RECRN003B','RECRN004B','RECRN005B',
#                             'RECAG001B','RECAG002B','RECAG003B','RECAG003E','RECAG005B','RECAG006B','RECAG007B','RECAG008B',
#                             -- RIS temporaneamente escluso: 'RECRSI004B',
#                             'RECRI003B','RECRI004B'
#                         ) THEN 'DEMAT_RECAPITO'
#                         ELSE NULL
#                     END AS tipo
#                 FROM silver_eventi_postalizzazione se
#                 -- I codici di stato sono definiti una sola volta nel CASE sopra.
#                 -- Le famiglie RS/RIS rimangono commentate fino alla riattivazione.
#
#             ) x
#             WHERE tipo IS NOT NULL
#         ),
#
#         temp_ultimi_eventi_silver_postalizzazione AS (
#             SELECT
#                 requestid,
#                 MAX(CASE WHEN tipo = 'FINE_RECAPITO' THEN statuscode END) AS fine_recapito_stato,
#                 MAX(CASE WHEN tipo = 'FINE_RECAPITO' THEN deliveryfailurecause END) AS fine_recapito_deliveryfailurecause,
#                 MAX(CASE WHEN tipo = 'FINE_RECAPITO' THEN statusdatetime END) AS fine_recapito_data,
#                 MAX(CASE WHEN tipo = 'FINE_RECAPITO' THEN clientrequesttimestamp END) AS fine_recapito_rendicontazione,
#                 MAX(CASE WHEN tipo = 'CERTIFICAZIONE_RECAPITO' THEN statuscode END) AS certificazione_recapito_stato,
#                 MAX(CASE WHEN tipo = 'CERTIFICAZIONE_RECAPITO' THEN statusdatetime END) AS certificazione_recapito_data,
#                 MAX(CASE WHEN tipo = 'CERTIFICAZIONE_RECAPITO' THEN clientrequesttimestamp END) AS certificazione_recapito_rendicontazione,
#                 MAX(CASE WHEN tipo = 'DEMAT_RECAPITO' THEN statuscode END) AS demat_recapito_stato,
#                 MAX(CASE WHEN tipo = 'DEMAT_RECAPITO' THEN deliveryfailurecause END) AS demat_recapito_deliveryfailurecause,
#                 MAX(CASE WHEN tipo = 'DEMAT_RECAPITO' THEN statusdatetime END) AS demat_recapito_data,
#                 MAX(CASE WHEN tipo = 'DEMAT_RECAPITO' THEN clientrequesttimestamp END) AS demat_recapito_rendicontazione
#             FROM temp_silver_postalizzazione
#             WHERE rn = 1
#             GROUP BY requestid
#         ),
#
#         temp_silver_notification AS (
#             SELECT
#                 sn.iun,
#                 sn.senderdenomination
#             FROM send.silver_notification sn
#             INNER JOIN perimetro_iun p
#                 ON sn.iun = p.iun
#         ),
#
#         payment_timeline AS (
#             -- Recupera l'evidenza di pagamento dalla timeline SILVER per gli IUN del perimetro.
#             SELECT
#                 tl.iun,
#                 -- SILVER PAYMENT: eventTimestamp e' UTC (Z); per il confronto
#                 -- con GOLD si converte una sola volta in orario Europe/Rome.
#                 -- MAX mantiene la selezione dell'ultimo PAYMENT per IUN.
#                 MAX(
#                     FROM_UTC_TIMESTAMP(
#                         CAST(REPLACE(REGEXP_REPLACE(
#                             get_json_object(tl.details, '$.eventTimestamp'),
#                             'Z$', ''), 'T', ' ') AS TIMESTAMP),
#                         'Europe/Rome'
#                     )
#                 ) AS tms_payment_silver
#             FROM send.silver_timeline tl
#             INNER JOIN perimetro_iun p
#                 ON tl.iun = p.iun
#             WHERE tl.category = 'PAYMENT'
#             GROUP BY tl.iun
#         ),
#
#         temp_gold_notification_analytics AS (
#             SELECT
#                 n.iun,
#                 n.tms_viewed,
#                 n.tms_effective_date,
#                 n.tms_date_payment AS tms_payment_gold,
#                 n.type_notif
#             FROM send.gold_notification_analytics n
#             INNER JOIN perimetro_iun p
#                 ON n.iun = p.iun
#         ),
#
#         temp_controlli AS (
#             SELECT
#                 s.senderpaid,
#                 sn.senderdenomination,
#                 s.iun,
#                 s.requestid,
#                 s.requesttimestamp,
#                 s.prodotto,
#                 s.geokey,
#                 CASE
#                     WHEN s.codice_oggetto LIKE 'R14%' THEN 'Fulmine'
#                     WHEN s.codice_oggetto LIKE '777%' OR s.codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
#                     WHEN s.codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
#                     WHEN s.codice_oggetto LIKE '69%' OR s.codice_oggetto LIKE '381%' OR s.codice_oggetto LIKE 'RB1%' THEN 'Poste'
#                     ELSE s.recapitista_unificato
#                 END AS recapitista_unif,
#                 s.lotto,
#                 s.codice_oggetto,
#                 CONCAT('''', s.codice_oggetto) AS codiceoggetto,
#                 s.scarto_consolidatore_stato,
#                 s.scarto_consolidatore_data,
#                 s.affido_recapitista_con016_data,
#                 s.accettazione_recapitista_con018_data,
#                 CASE
#                     WHEN s.accettazione_recapitista_CON018_data IS NOT NULL THEN s.accettazione_recapitista_CON018_data
#                     WHEN s.affido_recapitista_con016_data IS NOT NULL THEN s.affido_recapitista_con016_data
#                     ELSE s.requesttimestamp
#                 END AS affido_accettazione_rec_data,
#                 s.data_deposito,
#                 s.tentativo_recapito_stato,
#                 s.tentativo_recapito_data,
#                 s.tentativo_recapito_data_rendicontazione,
#                 s.messaingiacenza_recapito_stato,
#                 s.messaingiacenza_recapito_data,
#                 s.messaingiacenza_recapito_data_rendicontazione,
#                 s.certificazione_recapito_stato,
#                 s.certificazione_recapito_dettagli,
#                 s.certificazione_recapito_data,
#                 s.certificazione_recapito_data_rendicontazione,
#                 s.demat_23l_ar_stato,
#                 s.demat_23l_ar_data_rendicontazione,
#                 s.demat_plico_stato,
#                 s.demat_plico_data_rendicontazione,
#                 s.demat_documenttype_da_controllare,
#                 s.demat_arcad_stato,
#                 s.demat_arcad_data_rendicontazione,
#                 s.fine_recapito_stato,
#                 s.fine_recapito_data,
#                 s.fine_recapito_data_rendicontazione,
#                 s.accettazione_23l_recag012_data,
#                 s.accettazione_23l_recag012_data_rendicontazione,
#                 s.recindex_number,
#                 s.perfezionamento_data,
#                 s.perfezionamento_tipo,
#                 s.perfezionamento_notificationdate,
#                 s.perfezionamento_stato,
#                 s.perfezionamento_stato_dettagli,
#                 s.tms_cancelled,
#                 -- GOLD: timestamp di pagamento gia' rappresentato in ora italiana.
#                 -- SILVER: ultimo PAYMENT, originariamente UTC, ora in Europe/Rome.
#                 -- Si espongono entrambi; tms_date_payment conserva il minimo
#                 -- disponibile tra le due evidenze (non incide sul cluster). Per PAYMENT la semantica
#                 -- temporale andra' validata separatamente: non incide sul cluster.
#                 n.tms_payment_gold,
#                 pt.tms_payment_silver,
#                 CASE
#                     WHEN n.tms_payment_gold IS NULL THEN pt.tms_payment_silver
#                     WHEN pt.tms_payment_silver IS NULL THEN n.tms_payment_gold
#                     ELSE LEAST(n.tms_payment_gold, pt.tms_payment_silver)
#                 END AS tms_date_payment,
#                 s.flag_wi7_consolidatore,
#                 s.flag_wi7_report_postalizzazioni_incomplete,
#                 s.wi7_cluster,
#                 s.pcretry_rank,
#                 s.attempt_rank,
#                 s.attempt_number,
#                 -- Confronto tra DATE, senza conversione preliminare in UTC.
#                 CASE
#                     WHEN s.prodotto = 'AR' AND CAST(s.requesttimestamp AS DATE) >= DATE '2026-07-28' THEN 1
#                     WHEN s.prodotto = 'RS' AND CAST(s.requesttimestamp AS DATE) >= DATE '2026-10-15' THEN 1
#                     WHEN s.prodotto IN ('RIR','RIS') AND CAST(s.requesttimestamp AS DATE) >= DATE '2026-10-28' THEN 1
#                     -- 890: non risulta una data di attivazione.
#                     ELSE 0
#                 END AS flag_tracker_attivo,
#                 t.certificazione_recapito_stato AS certificazione_recapito_stato_silver,
#                 t.certificazione_recapito_data AS certificazione_recapito_data_silver,
#                 t.certificazione_recapito_rendicontazione AS certificazione_recapito_rendicontazione_silver,
#                 t.fine_recapito_stato AS fine_recapito_stato_silver,
#                 t.fine_recapito_deliveryfailurecause AS fine_recapito_deliveryfailurecause_silver,
#                 t.fine_recapito_data AS fine_recapito_data_silver,
#                 t.fine_recapito_rendicontazione AS fine_recapito_rendicontazione_silver,
#                 t.demat_recapito_stato AS demat_stato_silver,
#                 t.demat_recapito_deliveryfailurecause AS demat_recapito_deliveryfailurecause_silver,
#                 t.demat_recapito_data AS demat_data_silver,
#                 t.demat_recapito_rendicontazione AS demat_rendicontazione_silver,
#                 d.documenttype,
#                 CASE WHEN n.type_notif = 'MULTI' THEN 1 ELSE 0 END AS flag_multi_destinatario,
#                 CASE
#                     WHEN s.attempt_rank = 1 AND s.pcretry_rank = 1 THEN 1
#                     ELSE 0
#                 END AS flag_ultima_postalizzazione,
#                 s.flag_prodotto_estero,
#                 CASE
#                     WHEN s.certificazione_recapito_dettagli IN ('M02') THEN 1
#                     ELSE 0
#                 END AS flag_destinatario_deceduto,
#                 CASE
#                     WHEN s.flag_wi7_report_postalizzazioni_incomplete = 1 THEN 1
#                     ELSE 0
#                 END AS flag_fuori_sla,
#                 CASE
#                         WHEN s.certificazione_recapito_stato IN ('RECRI004A','RECRSI004A')
#                             AND s.certificazione_recapito_dettagli IS NULL THEN 1
#                         WHEN s.certificazione_recapito_stato NOT IN ('RECRS002A','RECRN002A','RECAG003A','RECRS002D','RECRN002D','RECAG003D','RECRI004A','RECRSI004A')
#                             AND s.certificazione_recapito_dettagli IN ('M01','M02','M03','M04','M05','M06','M07','M08','M09') THEN 1
#                         WHEN s.certificazione_recapito_stato IN ('RECRS002A', 'RECRN002A', 'RECAG003A')
#                             AND (
#                                 s.certificazione_recapito_dettagli NOT IN ('M02', 'M05', 'M06', 'M07', 'M08', 'M09')
#                                 OR s.certificazione_recapito_dettagli IS NULL
#                             ) THEN 1
#                         WHEN s.certificazione_recapito_stato IN ('RECRS002D', 'RECRN002D', 'RECAG003D')
#                             AND (
#                                 s.certificazione_recapito_dettagli NOT IN ('M01', 'M03', 'M04')
#                                 OR s.certificazione_recapito_dettagli IS NULL
#                             ) THEN 1
#                         WHEN s.certificazione_recapito_stato IN (
#                             'RECRS002A','RECRN002A','RECAG003A',
#                             'RECRS002D','RECRN002D','RECAG003D'
#                         ) AND (
#                             s.certificazione_recapito_dettagli IS DISTINCT FROM t.demat_recapito_deliveryfailurecause
#                             OR t.demat_recapito_deliveryfailurecause IS DISTINCT FROM t.fine_recapito_deliveryfailurecause
#                         ) THEN 1
#                         ELSE 0
#                     END AS controllo_causale,
#                 CASE
#                         WHEN s.prodotto IN ('RIR','RIS') THEN 0
#                         WHEN s.certificazione_recapito_stato IN ('RECRN003A', 'RECRN004A', 'RECRN005A')
#                             AND (s.tentativo_recapito_stato = 'RECRN010' OR s.tentativo_recapito_stato IS NULL) THEN 0
#                         WHEN s.certificazione_recapito_stato IN ('RECRS003C', 'RECRS004A', 'RECRS005A')
#                             AND (s.tentativo_recapito_stato = 'RECRS010' OR s.tentativo_recapito_stato IS NULL) THEN 0
#                         WHEN s.certificazione_recapito_stato IN ('RECAG005A','RECAG006A','RECAG007A','RECAG008A')
#                             AND (s.tentativo_recapito_stato = 'RECAG010' OR s.tentativo_recapito_stato IS NULL) THEN 0
#                         WHEN s.certificazione_recapito_stato NOT IN ('RECRN003A','RECRN004A','RECRN005A','RECAG005A','RECRS003C','RECRS004A','RECRS005A','RECAG006A','RECAG007A','RECAG008A')
#                             AND s.tentativo_recapito_stato = s.certificazione_recapito_stato THEN 0
#                         ELSE 1
#                     END AS controllo_inesito_casi_giacenza,
#                 CASE
#                         -- Casi con solo stato finale: non si richiede una tripletta A/B/C.
#                         WHEN s.fine_recapito_stato IN ('RECRS001C','RECRS003C', 'RECRSI003C') THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECRN001A'
#                                 AND (s.demat_23l_ar_stato = 'RECRN001B' OR s.demat_plico_stato = 'RECRN001B' OR s.demat_arcad_stato = 'RECRN001B')
#                                 AND t.fine_recapito_stato = 'RECRN001C' THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECRN002A'
#                                 AND (s.demat_23l_ar_stato = 'RECRN002B' OR s.demat_plico_stato = 'RECRN002B' OR s.demat_arcad_stato = 'RECRN002B')
#                                 AND t.fine_recapito_stato = 'RECRN002C' THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECRN002D'
#                                 AND (s.demat_23l_ar_stato = 'RECRN002E' OR s.demat_plico_stato = 'RECRN002E' OR s.demat_arcad_stato = 'RECRN002E')
#                                 AND t.fine_recapito_stato = 'RECRN002F' THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECRN003A'
#                                 AND (s.demat_23l_ar_stato = 'RECRN003B' OR s.demat_plico_stato = 'RECRN003B' OR s.demat_arcad_stato = 'RECRN003B')
#                                 AND t.fine_recapito_stato = 'RECRN003C' THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECRN004A'
#                                 AND (s.demat_23l_ar_stato = 'RECRN004B' OR s.demat_plico_stato = 'RECRN004B' OR s.demat_arcad_stato = 'RECRN004B')
#                                 AND t.fine_recapito_stato = 'RECRN004C' THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECRN005A'
#                                 AND (s.demat_23l_ar_stato = 'RECRN005B' OR s.demat_plico_stato = 'RECRN005B' OR s.demat_arcad_stato = 'RECRN005B')
#                                 AND t.fine_recapito_stato = 'RECRN005C' THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECAG001A'
#                                 AND (s.demat_23l_ar_stato = 'RECAG001B' OR s.demat_plico_stato = 'RECAG001B' OR s.demat_arcad_stato = 'RECAG001B')
#                                 AND t.fine_recapito_stato = 'RECAG001C' THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECAG002A'
#                                 AND (s.demat_23l_ar_stato = 'RECAG002B' OR s.demat_plico_stato = 'RECAG002B' OR s.demat_arcad_stato = 'RECAG002B')
#                                 AND t.fine_recapito_stato = 'RECAG002C' THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECAG003A'
#                                 AND (s.demat_23l_ar_stato = 'RECAG003B' OR s.demat_plico_stato = 'RECAG003B' OR s.demat_arcad_stato = 'RECAG003B')
#                                 AND t.fine_recapito_stato = 'RECAG003C' THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECAG003D'
#                                 AND (s.demat_23l_ar_stato = 'RECAG003E' OR s.demat_plico_stato = 'RECAG003E' OR s.demat_arcad_stato = 'RECAG003E')
#                                 AND t.fine_recapito_stato = 'RECAG003F' THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECAG005A'
#                                 AND (
#                                     s.demat_23l_ar_stato IN ('RECAG011B', 'RECAG005B')
#                                     OR s.demat_plico_stato IN ('RECAG011B', 'RECAG005B')
#                                     OR s.demat_arcad_stato IN ('RECAG011B', 'RECAG005B')
#                                 )
#                                 AND t.fine_recapito_stato = 'RECAG005C' THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECAG006A'
#                                 AND (
#                                     s.demat_23l_ar_stato IN ('RECAG011B', 'RECAG006B')
#                                     OR s.demat_plico_stato IN ('RECAG011B', 'RECAG006B')
#                                     OR s.demat_arcad_stato IN ('RECAG011B', 'RECAG006B')
#                                 )
#                                 AND t.fine_recapito_stato = 'RECAG006C' THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECAG007A'
#                                 AND (
#                                     s.demat_23l_ar_stato IN ('RECAG011B', 'RECAG007B')
#                                     OR s.demat_plico_stato IN ('RECAG011B', 'RECAG007B')
#                                     OR s.demat_arcad_stato IN ('RECAG011B', 'RECAG007B')
#                                 )
#                                 AND t.fine_recapito_stato = 'RECAG007C' THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECAG008A'
#                                 AND (
#                                     s.demat_23l_ar_stato IN ('RECAG011B', 'RECAG008B')
#                                     OR s.demat_plico_stato IN ('RECAG011B', 'RECAG008B')
#                                     OR s.demat_arcad_stato IN ('RECAG011B', 'RECAG008B')
#                                 )
#                                 AND t.fine_recapito_stato = 'RECAG008C' THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECRS002A'
#                                 AND (s.demat_23l_ar_stato = 'RECRS002B' OR s.demat_plico_stato = 'RECRS002B' OR s.demat_arcad_stato = 'RECRS002B')
#                                 AND t.fine_recapito_stato = 'RECRS002C' THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECRS002D'
#                                 AND (s.demat_23l_ar_stato = 'RECRS002E' OR s.demat_plico_stato = 'RECRS002E' OR s.demat_arcad_stato = 'RECRS002E')
#                                 AND t.fine_recapito_stato = 'RECRS002F' THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECRS004A'
#                                 AND (s.demat_23l_ar_stato = 'RECRS004B' OR s.demat_plico_stato = 'RECRS004B' OR s.demat_arcad_stato = 'RECRS004B')
#                                 AND t.fine_recapito_stato = 'RECRS004C' THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECRS005A'
#                                 AND (s.demat_23l_ar_stato = 'RECRS005B' OR s.demat_plico_stato = 'RECRS005B' OR s.demat_arcad_stato = 'RECRS005B')
#                                 AND t.fine_recapito_stato = 'RECRS005C' THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECRSI004A'
#                                 AND (s.demat_23l_ar_stato = 'RECRSI004B' OR s.demat_plico_stato = 'RECRSI004B' OR s.demat_arcad_stato = 'RECRSI004B')
#                                 AND t.fine_recapito_stato = 'RECRSI004C' THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECRI003A'
#                                 AND (s.demat_23l_ar_stato = 'RECRI003B' OR s.demat_plico_stato = 'RECRI003B' OR s.demat_arcad_stato = 'RECRI003B')
#                                 AND t.fine_recapito_stato = 'RECRI003C' THEN 0
#                         WHEN s.certificazione_recapito_stato = 'RECRI004A'
#                                 AND (s.demat_23l_ar_stato = 'RECRI004B' OR s.demat_plico_stato = 'RECRI004B' OR s.demat_arcad_stato = 'RECRI004B')
#                                 AND t.fine_recapito_stato = 'RECRI004C' THEN 0
#                         ELSE 1
#                     END AS controllo_tripletta,
#                 CASE
#                     -- Casi business in cui è prevista la sola data di fine recapito.
#                     WHEN s.fine_recapito_stato IN ('RECRS001C','RECRS003C', 'RECRSI003C') THEN 0
#                     WHEN t.demat_recapito_data IS NULL
#                          AND CAST(t.certificazione_recapito_data AS TIMESTAMP) = CAST(t.fine_recapito_data AS TIMESTAMP) THEN 0
#                     WHEN CAST(t.certificazione_recapito_data AS TIMESTAMP) = CAST(t.demat_recapito_data AS TIMESTAMP)
#                          AND CAST(t.demat_recapito_data AS TIMESTAMP) = CAST(t.fine_recapito_data AS TIMESTAMP) THEN 0
#                     ELSE 1
#                 END AS controllo_date_business,
#                 CASE
#                         WHEN s.certificazione_recapito_stato IN ('RECRS005A','RECRN005A')
#                         AND DATEDIFF(
#                             CAST(s.certificazione_recapito_data AS DATE),
#                             CAST(s.tentativo_recapito_data AS DATE)
#                         ) < 30 THEN 1
#                         ELSE 0
#                     END AS controllo_tempistiche_compiuta_giacenza,
#                    CASE
#                         -- Controllo solo la famiglia DEMAT con data di rendicontazione più recente.
#                         WHEN (
#                             s.demat_documenttype_da_controllare = '23L_AR'
#                             AND s.demat_23l_ar_stato IS NOT NULL
#                             AND s.demat_23l_ar_stato NOT IN (
#                                 'RECRN001B','RECRN003B',
#                                 'RECAG001B','RECAG002B','RECAG005B','RECAG006B',
#                                 'RECAG007B','RECAG008B','RECAG011B',
#                                 'RECRI003B'
#                             )
#                         ) THEN 1
#                         WHEN (
#                             s.demat_documenttype_da_controllare = 'PLICO'
#                             AND s.demat_plico_stato IS NOT NULL
#                             AND s.demat_plico_stato NOT IN (
#                                 -- RS temporaneamente escluso: 'RECRS002B','RECRS002E','RECRS004B','RECRS005B',
#                                 'RECRN002B','RECRN002E','RECRN004B','RECRN005B',
#                                 'RECAG003B','RECAG003E','RECAG007B','RECAG008B','RECAG011B',
#                                 'RECRI004B'
#                                 -- RIS temporaneamente escluso: 'RECRSI004B'
#                             )
#                         ) THEN 1
#                         WHEN s.prodotto = '890' AND d.documenttype = '23L' THEN 0
#                         WHEN s.prodotto IN ('AR', 'RIR') AND d.documenttype = 'AR' THEN 0
#                         WHEN d.documenttype IS NULL THEN 0
#                         ELSE 1
#                     END AS controllo_documentType
#             FROM gold_base_senza_feedback s
#             LEFT JOIN temp_demat d
#                 ON d.requestid = s.requestid
#             AND d.rn_demat = 1
#             LEFT JOIN temp_silver_notification sn
#                 ON sn.iun = s.iun
#             LEFT JOIN temp_gold_notification_analytics n
#                 ON s.iun = n.iun
#             LEFT JOIN payment_timeline pt
#                 ON s.iun = pt.iun
#             LEFT JOIN temp_ultimi_eventi_silver_postalizzazione t
#                 ON s.requestid = t.requestid
#         ),
#
#         temp_postalizzazione AS (
#             SELECT
#                 s.*,
#                 -- Per il pre-tracker la sequenza non e' vincolante.
#                 -- Per il post-tracker la chiusura SILVER deve essere l'ultima
#                 -- rendicontazione rispetto agli eventi pertinenti, inclusi i GOLD.
#                 -- Entrambe le sorgenti sono ora confrontate come orari Europe/Rome;
#                 -- per GOLD non si applica una seconda conversione.
#                 CASE
#                     WHEN s.flag_tracker_attivo = 0 THEN 0
#                     WHEN s.fine_recapito_rendicontazione_silver IS NULL THEN 1
#                     WHEN
#                         CAST(s.certificazione_recapito_data_rendicontazione AS TIMESTAMP) > CAST(s.fine_recapito_rendicontazione_silver AS TIMESTAMP)
#                         OR CAST(s.demat_23l_ar_data_rendicontazione AS TIMESTAMP) > CAST(s.fine_recapito_rendicontazione_silver AS TIMESTAMP)
#                         OR CAST(s.demat_plico_data_rendicontazione AS TIMESTAMP) > CAST(s.fine_recapito_rendicontazione_silver AS TIMESTAMP)
#                         OR CAST(s.demat_arcad_data_rendicontazione AS TIMESTAMP) > CAST(s.fine_recapito_rendicontazione_silver AS TIMESTAMP)
#                         OR CAST(s.tentativo_recapito_data_rendicontazione AS TIMESTAMP) > CAST(s.fine_recapito_rendicontazione_silver AS TIMESTAMP)
#                         OR CAST(s.messaingiacenza_recapito_data_rendicontazione AS TIMESTAMP) > CAST(s.fine_recapito_rendicontazione_silver AS TIMESTAMP)
#                         OR CAST(s.accettazione_23l_recag012_data_rendicontazione AS TIMESTAMP) > CAST(s.fine_recapito_rendicontazione_silver AS TIMESTAMP)
#                     THEN 1 ELSE 0
#                 END AS controllo_rendicontazione_ordinata,
#                 IF(
#                     fine_recapito_stato IN (
#                         'RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C',
#                         'RECAG005C','RECAG006C','RECAG007C','RECAG008C'
#                     )
#                     AND tentativo_recapito_stato IS NULL,
#                     1, 0
#                 ) AS assenza_inesito,
#                 IF(
#                     fine_recapito_stato IN (
#                         'RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C',
#                         'RECAG005C','RECAG006C','RECAG007C','RECAG008C'
#                     )
#                     AND messaingiacenza_recapito_stato IS NULL,
#                     1, 0
#                 ) AS assenza_messa_in_giacenza,
#                 IF(certificazione_recapito_stato IS NULL, 1, 0) AS assenza_pre_esito,
#                 IF(
#                         fine_recapito_stato NOT IN ('RECRS001C','RECRS003C', 'RECRSI003C')
#                         AND (
#                             (
#                                 fine_recapito_stato IN ('RECAG008C')
#                                 AND (
#                                     demat_23l_ar_stato IS NULL
#                                     OR demat_plico_stato IS NULL
#                                 )
#                             )
#                             OR (
#                                 demat_23l_ar_stato IS NULL
#                                 AND demat_plico_stato IS NULL
#                             )
#                         ),
#                         1,
#                         0
#                     ) AS assenza_dematerializzazione_23l_ar_plico,
#                 IF(
#                     prodotto = '890'
#                     AND fine_recapito_stato IN ('RECAG005C','RECAG006C','RECAG007C','RECAG008C')
#                     AND demat_arcad_data_rendicontazione IS NULL,
#                     1, 0
#                 ) AS assenza_demat_ARCAD,
#                 IF(
#                     prodotto = '890'
#                     AND fine_recapito_stato IN ('RECAG005C','RECAG006C','RECAG007C','RECAG008C')
#                     AND accettazione_23l_recag012_data IS NULL,
#                     1, 0
#                 ) AS assenza_RECAG012,
#                     IF(
#                         fine_recapito_stato IN ('RECRSI003C','RECRSI004C','RECRI003C','RECRI004C')
#                         AND tentativo_recapito_stato IS NULL,
#                         1,
#                         0
#                     ) AS assenza_avviato_estero,
#                     IF(
#                         fine_recapito_stato IN ('RECRSI003C','RECRSI004C','RECRI003C','RECRI004C')
#                         AND messaingiacenza_recapito_stato IS NULL,
#                         1,
#                         0
#                     ) AS assenza_ingresso_paese_estero
#             FROM temp_controlli s
#         ),
#
#         temp_assenza_eventi AS (
#             SELECT
#                 *,
#                 IF(
#                     assenza_inesito = 1
#                     OR assenza_messa_in_giacenza = 1
#                     OR assenza_pre_esito = 1
#                     OR assenza_dematerializzazione_23l_ar_plico = 1
#                     OR assenza_demat_ARCAD = 1
#                     OR assenza_RECAG012 = 1
#                     OR assenza_avviato_estero = 1
#                     OR assenza_ingresso_paese_estero = 1,
#                     1, 0
#                 ) AS flag_esiti_mancanti
#             FROM temp_postalizzazione
#         ),
#
#         finale AS (
#             SELECT
#                 t.senderpaid,
#                 t.senderdenomination,
#                 t.iun,
#                 t.requestid,
#                 t.requesttimestamp,
#                 t.prodotto,
#                 t.geokey,
#                 t.recapitista_unif,
#                 t.lotto,
#                 t.codice_oggetto,
#                 t.codiceoggetto,
#                 t.scarto_consolidatore_stato,
#                 t.scarto_consolidatore_data,
#                 t.affido_recapitista_con016_data,
#                 t.accettazione_recapitista_con018_data,
#                 t.affido_accettazione_rec_data,
#                 t.data_deposito,
#                 t.tentativo_recapito_stato,
#                 t.tentativo_recapito_data,
#                 t.tentativo_recapito_data_rendicontazione,
#                 t.messaingiacenza_recapito_stato,
#                 t.messaingiacenza_recapito_data,
#                 t.messaingiacenza_recapito_data_rendicontazione,
#                 t.certificazione_recapito_stato,
#                 t.certificazione_recapito_dettagli,
#                 t.certificazione_recapito_data,
#                 t.certificazione_recapito_data_rendicontazione,
#                 t.demat_23l_ar_stato,
#                 t.documenttype,
#                 t.demat_23l_ar_data_rendicontazione,
#                 t.demat_plico_stato,
#                 t.demat_plico_data_rendicontazione,
#                 t.demat_arcad_stato,
#                 t.demat_arcad_data_rendicontazione,
#                 t.fine_recapito_stato,
#                 t.fine_recapito_data,
#                 t.fine_recapito_data_rendicontazione,
#                 t.accettazione_23l_recag012_data,
#                 t.accettazione_23l_recag012_data_rendicontazione,
#                 t.recindex_number,
#                 t.perfezionamento_data,
#                 t.perfezionamento_tipo,
#                 t.perfezionamento_notificationdate,
#                 t.perfezionamento_stato,
#                 t.perfezionamento_stato_dettagli,
#                 t.tms_payment_gold,
#                 t.tms_payment_silver,
#                 t.tms_date_payment,
#                 t.tms_cancelled,
#                 t.flag_wi7_consolidatore,
#                 t.flag_wi7_report_postalizzazioni_incomplete,
#                 t.wi7_cluster,
#                 t.pcretry_rank,
#                 t.attempt_rank,
#                 t.attempt_number,
#                 t.certificazione_recapito_stato_silver,
#                 t.certificazione_recapito_data_silver,
#                 t.certificazione_recapito_rendicontazione_silver,
#                 t.fine_recapito_stato_silver,
#                 t.fine_recapito_deliveryfailurecause_silver,
#                 t.fine_recapito_data_silver,
#                 t.fine_recapito_rendicontazione_silver,
#                 t.demat_stato_silver,
#                 t.demat_recapito_deliveryfailurecause_silver,
#                 t.demat_data_silver,
#                 t.demat_rendicontazione_silver,
#                 t.flag_multi_destinatario,
#                 t.flag_ultima_postalizzazione,
#                 t.flag_prodotto_estero,
#                 t.flag_destinatario_deceduto,
#                 t.flag_esiti_mancanti,
#                 t.demat_documenttype_da_controllare,
#                 t.controllo_causale,
#                 t.controllo_date_business,
#                 t.flag_tracker_attivo,
#                 t.controllo_rendicontazione_ordinata,
#                 t.controllo_tripletta,
#                 t.controllo_tempistiche_compiuta_giacenza,
#                 t.controllo_inesito_casi_giacenza,
#                 t.controllo_documentType,
#                 t.assenza_inesito,
#                 t.assenza_messa_in_giacenza,
#                 t.assenza_pre_esito,
#                 t.assenza_dematerializzazione_23l_ar_plico,
#                 t.assenza_demat_ARCAD,
#                 t.assenza_RECAG012,
#                 t.assenza_avviato_estero,
#                 t.assenza_ingresso_paese_estero,
#                 CASE
#                     WHEN controllo_causale = 1
#                     OR controllo_date_business = 1
#                     OR controllo_rendicontazione_ordinata = 1
#                     OR controllo_tripletta = 1
#                     OR controllo_tempistiche_compiuta_giacenza = 1
#                     OR controllo_inesito_casi_giacenza = 1
#                     OR controllo_documentType = 1
#                     THEN 1
#                     ELSE 0
#                 END AS flag_errore_rendicontazione,
#                 CASE
#                     WHEN CAST(t.attempt_number AS INT) = 0 THEN 'Rendicontazione corretta - Bloccati al primo attempt'
#                     WHEN CAST(t.attempt_number AS INT) = 1 THEN 'Rendicontazione corretta - Bloccati al secondo attempt'
#                     ELSE NULL
#                 END AS dettaglio_rendicontazione,
#                 CASE
#                     WHEN (flag_wi7_report_postalizzazioni_incomplete = 1 OR flag_wi7_consolidatore = 1)
#                         THEN 'Oggetto fuori sla'
#                     WHEN (
#                         controllo_causale = 1
#                         OR controllo_date_business = 1
#                         OR controllo_rendicontazione_ordinata = 1
#                         OR controllo_tripletta = 1
#                         OR controllo_tempistiche_compiuta_giacenza = 1
#                         OR controllo_inesito_casi_giacenza = 1
#                         OR flag_esiti_mancanti = 1
#                         OR controllo_documentType = 1
#                     )
#                     AND fine_recapito_stato IS NOT NULL
#                         THEN 'Errore rendicontazione/assenza eventi intermedi'
#                     WHEN (
#                         controllo_causale = 0
#                         AND controllo_date_business = 0
#                         AND controllo_rendicontazione_ordinata = 0
#                         AND controllo_tripletta = 0
#                         AND controllo_tempistiche_compiuta_giacenza = 0
#                         AND controllo_inesito_casi_giacenza = 0
#                         AND assenza_inesito = 0
#                         AND assenza_RECAG012 = 0
#                         AND assenza_pre_esito = 0
#                         AND assenza_dematerializzazione_23l_ar_plico = 0
#                         AND assenza_messa_in_giacenza = 0
#                         AND assenza_demat_arcad = 0
#                         AND assenza_avviato_estero = 0
#                         AND assenza_ingresso_paese_estero = 0
#                         AND controllo_documentType = 0
#                     )
#                         THEN 'Oggetto con rendicontazione corretta'
#                     ELSE 'Oggetto in corso di postalizzazione'
#                 END AS cluster_mancato_feedback_analogico
#             FROM temp_assenza_eventi t
#         ),
#
#         finale_filtrato AS (
#             SELECT
#                 CAST(CURRENT_DATE() AS STRING) AS data_estrazione,
#                 CAST(CURRENT_DATE() AS STRING) AS data_prima_estrazione,
#                 CONCAT(
#                     FROM_TIMESTAMP(CAST(DATE_SUB(CURRENT_DATE(), PMOD(DAYOFWEEK(CURRENT_DATE()) + 5, 7)) AS TIMESTAMP), 'yyyyMMdd'),
#                     ' - ',
#                     FROM_TIMESTAMP(CAST(DATE_ADD(DATE_SUB(CURRENT_DATE(), PMOD(DAYOFWEEK(CURRENT_DATE()) + 5, 7)), 6) AS TIMESTAMP), 'yyyyMMdd')
#                 ) AS settimana_prima_estrazione,
#                 senderpaid,
#                 senderdenomination,
#                 iun,
#                 data_deposito,
#                 requestid,
#                 requesttimestamp,
#                 prodotto,
#                 geokey,
#                 recapitista_unif,
#                 lotto,
#                 codice_oggetto,
#                 codiceoggetto,
#                 affido_accettazione_rec_data,
#                 scarto_consolidatore_stato,
#                 scarto_consolidatore_data,
#                 affido_recapitista_con016_data,
#                 accettazione_recapitista_con018_data,
#                 tentativo_recapito_stato,
#                 tentativo_recapito_data,
#                 tentativo_recapito_data_rendicontazione,
#                 messaingiacenza_recapito_stato,
#                 messaingiacenza_recapito_data,
#                 messaingiacenza_recapito_data_rendicontazione,
#                 certificazione_recapito_stato,
#                 certificazione_recapito_dettagli,
#                 certificazione_recapito_data,
#                 certificazione_recapito_data_rendicontazione,
#                 demat_23l_ar_stato,
#                 documenttype,
#                 demat_23l_ar_data_rendicontazione,
#                 demat_plico_stato,
#                 demat_plico_data_rendicontazione,
#                 demat_arcad_stato,
#                 demat_arcad_data_rendicontazione,
#                 fine_recapito_stato,
#                 fine_recapito_data,
#                 fine_recapito_data_rendicontazione,
#                 accettazione_23l_recag012_data,
#                 accettazione_23l_recag012_data_rendicontazione,
#                 recindex_number,
#                 perfezionamento_data,
#                 perfezionamento_tipo,
#                 perfezionamento_notificationdate,
#                 perfezionamento_stato,
#                 perfezionamento_stato_dettagli,
#                 tms_cancelled,
#                 flag_wi7_consolidatore,
#                 flag_wi7_report_postalizzazioni_incomplete,
#                 wi7_cluster,
#                 pcretry_rank,
#                 attempt_rank,
#                 attempt_number,
#                 certificazione_recapito_stato_silver,
#                 certificazione_recapito_data_silver,
#                 certificazione_recapito_rendicontazione_silver,
#                 fine_recapito_stato_silver,
#                 fine_recapito_deliveryfailurecause_silver,
#                 fine_recapito_data_silver,
#                 fine_recapito_rendicontazione_silver,
#                 demat_stato_silver,
#                 demat_recapito_deliveryfailurecause_silver,
#                 demat_data_silver,
#                 demat_rendicontazione_silver,
#                 demat_documenttype_da_controllare,
#                 flag_esiti_mancanti,
#                 flag_errore_rendicontazione,
#                 controllo_causale,
#                 controllo_date_business,
#                 flag_tracker_attivo,
#                 controllo_rendicontazione_ordinata,
#                 controllo_tripletta,
#                 controllo_tempistiche_compiuta_giacenza,
#                 controllo_inesito_casi_giacenza,
#                 controllo_documentType,
#                 assenza_inesito,
#                 assenza_messa_in_giacenza,
#                 assenza_pre_esito,
#                 assenza_dematerializzazione_23l_ar_plico,
#                 assenza_demat_ARCAD,
#                 assenza_RECAG012,
#                 assenza_avviato_estero,
#                 assenza_ingresso_paese_estero,
#                 tms_payment_gold,
#                 tms_payment_silver,
#                 tms_date_payment,
#                 cluster_mancato_feedback_analogico,
#                 dettaglio_rendicontazione
#             FROM finale
#             WHERE flag_ultima_postalizzazione = 1
#             AND (
#                     COALESCE(certificazione_recapito_stato, '') NOT IN (
#                         'RECRS006','RECRS013','RECRN006','RECRN013','RECAG004','RECAG013'
#                     )
#                     OR certificazione_recapito_stato IS NULL
#             )
#         )
#
#         SELECT *
#         FROM finale_filtrato
#         WHERE 1=1
#         AND cluster_mancato_feedback_analogico = 'Oggetto con rendicontazione corretta'
#         ;
