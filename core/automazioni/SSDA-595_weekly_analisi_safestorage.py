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

APP_NAME = "SSDA-806 Weekly Analisi SafeStorage"

SLACK_CONFIG_CANDIDATE_PATHS = [
    "/Slack/notifications_webhook.txt",
    "/app/mount/Slack/notifications_webhook.txt",
    "/app/mount/notifications_webhook.txt",
]
SLACK_WEBHOOK_NAME = "webhook_1"
SLACK_LABEL = "SSDA-806 Weekly Analisi SafeStorage"

GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"
SHEET_ID = "1RNLSni9hKdE5mSJCLeLtbnkxsXJH5pnoFZhR_WjT-mI"

TAB_OUTPUT = "not_found_att"
TAB_LISTA_CONFRONTO = "lista_confronto"
TAB_LOG = "Log di controllo"

INITIAL_WS_ROWS = 2
GSHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEETS_CELL_LIMIT = 10_000_000


# ---------------- ECCEZIONI ----------------
class CapacityLimitError(RuntimeError):
    """Errore fatale: l'export non è rappresentabile in Google Sheets entro i limiti strutturali."""


# ---------------- SLACK ----------------
def now_utc_dt() -> datetime:
    return datetime.now(timezone.utc)


def elapsed_minutes_str(start_dt: datetime, end_dt: datetime | None = None) -> str:
    if end_dt is None:
        end_dt = now_utc_dt()
    elapsed_sec = (end_dt - start_dt).total_seconds()
    return f"{elapsed_sec / 60:.2f}"


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
    """Restituisce il client Sheet già configurato."""
    return Sheet(sheet_id=SHEET_ID, service_credentials=creds, id_mode="key")


def safe_download_sheet(sheet: Sheet, sheet_name: str) -> pd.DataFrame:
    """
    Legge uno sheet e restituisce un DataFrame Pandas.
    Se il tab non esiste, è vuoto o non è leggibile, restituisce un DataFrame vuoto.
    """
    try:
        logging.info(f"Lettura tab Google Sheet: {sheet_name}")
        df = sheet.download(sheet_name)

        if df is None:
            logging.warning(
                f"Il tab '{sheet_name}' non contiene dati. Restituisco DataFrame vuoto."
            )
            return pd.DataFrame()

        df = pd.DataFrame(df)

        if df.empty:
            logging.warning(f"Il tab '{sheet_name}' è vuoto.")

        logging.info(f"Righe lette da '{sheet_name}': {len(df)}")
        logging.info(f"Colonne lette da '{sheet_name}': {list(df.columns)}")
        return df

    except Exception as e:
        logging.warning(
            f"Impossibile leggere il tab '{sheet_name}': {e}. Restituisco DataFrame vuoto."
        )
        return pd.DataFrame()


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


def spark_to_df_per_gsheet(df_spark, missing: str = "-") -> pd.DataFrame:
    """
    Converte un DataFrame Spark in Pandas pronto per Google Sheets.
    """
    if df_spark is None:
        return pd.DataFrame()

    logging.info("Conversione DataFrame Spark -> Pandas...")
    df = df_spark.toPandas()
    logging.info(f"Conversione completata. Righe convertite: {len(df)}")

    return sanitize_for_sheets(df, missing=missing)


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

    logging.info(
        f"Scrittura completata su tab '{sheet_name}'. "
        f"Righe scritte: {len(df)} | Colonne: {len(df.columns)}"
    )


def get_last_update_date(spark: SparkSession) -> str:
    """Estrae la data più recente (MAX requesttimestamp) dal dataset."""
    logging.info("Estrazione data ultimo aggiornamento dati...")
    max_date_df = spark.sql(
        "SELECT MAX(requesttimestamp) AS max_ts FROM send.gold_postalizzazione_analytics"
    )
    max_date = max_date_df.collect()[0]["max_ts"]

    if isinstance(max_date, datetime):
        return max_date.strftime("%Y-%m-%d %H:%M:%S")
    return str(max_date)


# ---------------- QUERY PRINCIPALE ----------------
def run_query_not_found_att_enhanced(spark: SparkSession) -> pd.DataFrame:
    """
    Estrazione settimanale enhanced:
      - base: send_dev.not_found_att
      - esclude iun perfezionati o cancellati
      - recupera scheduled refinement
      - recupera requestid + ultimo evento postalizzazione
      - recupera stato avanzamento oggetti
      - arricchisce con senderpaid, description, ente_id
    """

    logging.info("Avvio estrazione enhanced not_found_att...")

    # 1) base
    base_df = spark.sql("""
        SELECT DISTINCT
            iun,
            attachment
        FROM send_dev.not_found_att
        WHERE iun IS NOT NULL
    """).cache()
    logging.info(f"Base not_found_att caricata: {base_df.count()} righe")
    base_df.createOrReplaceTempView("base_not_found_att")

    # 2) timeline refined/scheduled
    timeline_agg_df = spark.sql("""
        WITH notification AS (
            SELECT
                iun,
                CASE WHEN category = 'REFINEMENT' THEN 1 ELSE 0 END AS refined,
                CASE WHEN category = 'SCHEDULE_REFINEMENT' THEN 1 ELSE 0 END AS scheduled,
                get_json_object(details, '$.schedulingDate') AS schedulingDate
            FROM send.silver_timeline
            WHERE category IN ('REFINEMENT', 'SCHEDULE_REFINEMENT')
              AND iun IN (SELECT iun FROM base_not_found_att)
        )
        SELECT
            iun,
            SUM(refined) AS refined,
            SUM(scheduled) AS scheduled,
            MAX(schedulingDate) AS schedulingDate
        FROM notification
        GROUP BY iun
        HAVING SUM(refined) = 0
    """)
    logging.info("Calcolata timeline_agg")
    timeline_agg_df.createOrReplaceTempView("timeline_agg")

    timeline_flags_df = spark.sql("""
        SELECT
            iun,
            CASE WHEN scheduled > 0 THEN 1 ELSE 0 END AS scheduled,
            schedulingDate
        FROM timeline_agg
    """)
    logging.info("Calcolata timeline_flags")
    timeline_flags_df.createOrReplaceTempView("timeline_flags")

    # 3) notification analytics: non cancellati e non perfezionati
    notif_status_ok_df = spark.sql("""
        SELECT
            iun
        FROM (
            SELECT
                iun,
                tms_cancelled,
                LEAST(
                    COALESCE(tms_viewed, tms_effective_date),
                    COALESCE(tms_effective_date, tms_viewed)
                ) AS tms_perfezionamento_notification
            FROM send.gold_notification_analytics
            WHERE iun IN (SELECT iun FROM base_not_found_att)
        ) x
        WHERE x.tms_cancelled IS NULL
          AND x.tms_perfezionamento_notification IS NULL
    """)
    logging.info("Calcolata notif_status_ok")
    notif_status_ok_df.createOrReplaceTempView("notif_status_ok")

    # 4) postalizzazione last events
    postalizzazione_last_events_df = spark.sql("""
        SELECT
            iun,
            requestid,
            ultimo_evento_stato,
            ultimo_evento_data_rendicontazione AS tms_ultimo_evento
        FROM send.gold_postalizzazione_analytics
        WHERE pcretry_rank = 1
          AND attempt_rank = 1
          AND iun IN (SELECT iun FROM base_not_found_att)
    """)
    logging.info("Calcolata postalizzazione_last_events")
    postalizzazione_last_events_df.createOrReplaceTempView(
        "postalizzazione_last_events"
    )

    relevant_requestids_df = spark.sql("""
        SELECT DISTINCT
            requestid
        FROM postalizzazione_last_events
        WHERE requestid IS NOT NULL
    """).cache()
    logging.info(
        f"Calcolati relevant_requestids: {relevant_requestids_df.count()} righe"
    )
    relevant_requestids_df.createOrReplaceTempView("relevant_requestids")

    # 5) cluster_mancato_perfezionamento
    CLUSTER_SQL = """
        WITH temp_demat AS (
            SELECT
                REGEXP_REPLACE(p.requestid, '^pn-cons-000~', '') AS requestid,
                att.documenttype AS documenttype,
                ROW_NUMBER() OVER (
                    PARTITION BY REGEXP_REPLACE(p.requestid, '^pn-cons-000~', '')
                    ORDER BY e.paperprogrstatus.clientrequesttimestamp DESC
                ) AS rn_demat
            FROM send.silver_postalizzazione p
            LATERAL VIEW explode(p.eventslist) ev AS e
            LATERAL VIEW explode(e.paperprogrstatus.attachments) atts AS att
            WHERE att.documenttype IN ('23L', 'AR')
              AND REGEXP_REPLACE(p.requestid, '^pn-cons-000~', '') IN (
                  SELECT requestid FROM relevant_requestids
              )
        ),
        base AS (
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
                END AS tipo
            FROM send.silver_postalizzazione t
            LATERAL VIEW EXPLODE(t.eventslist) AS e
            WHERE e.paperprogrstatus.statuscode IN (
                'RECRN001A','RECRN002A','RECRN002D','RECRN003A','RECRN004A','RECRN005A','RECAG001A',
                'RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A','RECAG007A','RECAG008A',
                'RECRN001C','RECRN002C','RECRN002F','RECRN003C','RECRN004C','RECRN005C','RECAG001C',
                'RECAG002C','RECAG003C','RECAG003F','RECAG005C','RECAG006C','RECAG007C','RECAG008C'
            ) AND e.paperprogrstatus.statuscode NOT IN ('PN999','PN998')
              AND REGEXP_REPLACE(t.requestid, '^pn-cons-000~', '') IN (
                  SELECT requestid FROM relevant_requestids
              )
        ),
        temp_silver_postalizzazione AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY requestid, tipo
                    ORDER BY clientrequesttimestamp DESC
                ) AS rn
            FROM base
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
        temp_controlli AS (
            SELECT
                s.senderpaid,
                sn.senderdenomination,
                s.iun,
                s.data_deposito,
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
                    ELSE s.affido_recapitista_con016_data
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
                s.ultimo_evento_data,
                LEAST(
                    COALESCE(n.tms_viewed, n.tms_effective_date),
                    COALESCE(n.tms_effective_date, n.tms_viewed)
                ) AS tms_perfezionamento_notification,
                CASE
                    WHEN tl.category = 'SCHEDULE_REFINEMENT' THEN 1
                    ELSE 0
                END AS flag_schedule_refinement,
                s.tms_cancelled,
                n.tms_date_payment,
                s.flag_wi7_consolidatore,
                s.flag_wi7_report_postalizzazioni_incomplete,
                s.wi7_cluster,
                s.pcretry_rank,
                s.attempt_rank,
                s.statusrequest,
                t.certificazione_recapito_stato AS certificazione_recapito_stato_silver,
                t.certificazione_recapito_data AS certificazione_recapito_data_silver,
                t.certificazione_recapito_rendicontazione AS certificazione_recapito_rendicontazione_silver,
                t.fine_recapito_stato AS fine_recapito_stato_silver,
                t.fine_recapito_data AS fine_recapito_data_silver,
                t.fine_recapito_rendicontazione AS fine_recapito_rendicontazione_silver,
                d.documenttype,
                CASE WHEN w.requestid IS NOT NULL THEN 1 ELSE 0 END AS flag_oggetto_tavoli_poste,
                CASE WHEN w.requestid IS NOT NULL AND w.tavolo_di_gestione = 'duplicati_23l_23i' THEN 1 ELSE 0 END AS flag_tavolo_duplicati_poste,
                CASE WHEN w.requestid IS NOT NULL AND w.tavolo_di_gestione = 'non_rendicontabili_puri' THEN 1 ELSE 0 END AS flag_tavolo_non_rendicontabili_poste,
                CASE WHEN n.type_notif = 'MULTI' THEN 1 ELSE 0 END AS flag_multi_destinatario,
                CASE WHEN s.attempt_rank = 1 AND s.pcretry_rank = 1 THEN 1 ELSE 0 END AS flag_ultima_postalizzazione,
                s.flag_prodotto_estero,
                CASE WHEN s.certificazione_recapito_dettagli IN ('M02') THEN 1 ELSE 0 END AS flag_destinatario_deceduto,
                CASE WHEN s.flag_wi7_report_postalizzazioni_incomplete = 1 THEN 1 ELSE 0 END AS flag_fuori_sla,
                CASE
                    WHEN s.certificazione_recapito_stato NOT IN ('RECRS002A','RECRN002A','RECAG003A','RECRS002D','RECRN002D','RECAG003D')
                        AND s.certificazione_recapito_dettagli IN ('M01','M03','M04','M02','M05','M06','M07','M08','M09') THEN 1
                    WHEN s.certificazione_recapito_stato IN ('RECRS002A','RECRN002A','RECAG003A')
                        AND (
                            s.certificazione_recapito_dettagli NOT IN ('M02','M05','M06','M07','M08','M09')
                            OR s.certificazione_recapito_dettagli IS NULL
                        ) THEN 1
                    WHEN s.certificazione_recapito_stato IN ('RECRS002D','RECRN002D','RECAG003D')
                        AND (
                            s.certificazione_recapito_dettagli NOT IN ('M01','M03','M04')
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
                        AND DATEDIFF(
                            CAST(s.certificazione_recapito_data AS DATE),
                            CAST(s.tentativo_recapito_data AS DATE)
                        ) < 30 THEN 1
                    ELSE 0
                END AS controllo_tempistiche_compiuta_giacenza,
                CASE
                    WHEN s.prodotto = '890' AND d.documenttype = '23L' THEN 0
                    WHEN s.prodotto = 'AR' AND d.documenttype = 'AR' THEN 0
                    WHEN d.documenttype IS NULL THEN 0
                    ELSE 1
                END AS controllo_documentType
            FROM send.gold_postalizzazione_analytics s
            LEFT JOIN temp_demat d
                ON d.requestid = s.requestid
               AND d.rn_demat = 1
            LEFT JOIN send_dev.wi7_poste_da_escludere w
                ON s.requestid = w.requestid
            LEFT JOIN send.gold_notification_analytics n
                ON s.iun = n.iun
            LEFT JOIN send.silver_timeline tl
                ON s.iun = tl.iun
               AND tl.category = 'SCHEDULE_REFINEMENT'
            LEFT JOIN send.silver_notification sn
                ON s.iun = sn.iun
            LEFT JOIN temp_ultimi_eventi_silver_postalizzazione t
                ON s.requestid = t.requestid
            WHERE s.requestid IN (SELECT requestid FROM relevant_requestids)
        ),
        temp_postalizzazione AS (
            SELECT
                s.*,
                IF(
                    fine_recapito_stato IN ('RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C','RECAG005C','RECAG006C','RECAG007C','RECAG008C')
                    AND tentativo_recapito_stato IS NULL,
                    1,
                    0
                ) AS assenza_inesito,
                IF(
                    fine_recapito_stato IN ('RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C','RECAG005C','RECAG006C','RECAG007C','RECAG008C')
                    AND messaingiacenza_recapito_stato IS NULL,
                    1,
                    0
                ) AS assenza_messa_in_giacenza,
                IF(certificazione_recapito_stato IS NULL, 1, 0) AS assenza_pre_esito,
                IF(
                    (
                        fine_recapito_stato IN ('RECAG008C') AND (demat_23l_ar_stato IS NULL OR demat_plico_stato IS NULL)
                    )
                    OR (
                        demat_23l_ar_stato IS NULL AND demat_plico_stato IS NULL
                    ),
                    1,
                    0
                ) AS assenza_dematerializzazione_23l_ar_plico,
                IF(
                    prodotto = '890'
                    AND fine_recapito_stato IN ('RECAG005C','RECAG006C','RECAG007C','RECAG008C')
                    AND demat_arcad_data_rendicontazione IS NULL,
                    1,
                    0
                ) AS assenza_demat_ARCAD,
                IF(
                    prodotto = '890'
                    AND fine_recapito_stato IN ('RECAG005C','RECAG006C','RECAG007C','RECAG008C')
                    AND accettazione_23l_recag012_data IS NULL,
                    1,
                    0
                ) AS assenza_RECAG012
            FROM temp_controlli s
        ),
        temp_assenza_eventi AS (
            SELECT
                *,
                IF(
                    assenza_inesito = 1 OR assenza_messa_in_giacenza = 1 OR assenza_pre_esito = 1
                    OR assenza_dematerializzazione_23l_ar_plico = 1 OR assenza_demat_ARCAD = 1 OR assenza_RECAG012 = 1,
                    1,
                    0
                ) AS flag_esiti_mancanti
            FROM temp_postalizzazione
        ),
        finale AS (
            SELECT
                t.requestid,
                CASE
                    WHEN tms_perfezionamento_notification IS NOT NULL THEN 'Oggetto perfezionato'
                    WHEN tms_cancelled IS NOT NULL THEN 'Oggetto cancellato'
                    WHEN flag_schedule_refinement = 1 THEN 'Oggetto in previsione di perfezionamento'
                    WHEN statusrequest IN ('PN999', 'PN998')
                        OR fine_recapito_stato IN ('PN999', 'PN998')
                        OR tentativo_recapito_stato IN ('PN999', 'PN998')
                        OR certificazione_recapito_stato IN ('PN999', 'PN998')
                        THEN 'Oggetto bloccato con PN998 / PN999'
                    WHEN ultimo_evento_stato IN ('P008', 'P010', 'P011') THEN 'Oggetto scartato con P008 / P010 / P011'
                    WHEN scarto_consolidatore_stato IS NOT NULL THEN 'Oggetto scartato dal Consolidatore'
                    WHEN flag_tavolo_duplicati_poste = 1 THEN 'Oggetto rientrante in tavolo Duplicati 23i/23l Poste'
                    WHEN flag_tavolo_non_rendicontabili_poste = 1 THEN 'Oggetto rientrante in tavolo Non Rendicontabili Poste'
                    WHEN (flag_wi7_report_postalizzazioni_incomplete = 1 OR flag_wi7_consolidatore = 1)
                         AND flag_oggetto_tavoli_poste = 0 THEN 'Oggetto fuori sla'
                    WHEN flag_destinatario_deceduto = 1 THEN 'Oggetto restituito al mittente (esito destinatario deceduto)'
                    WHEN certificazione_recapito_stato IN ('RECRS006','RECRS013','RECRN006','RECRN013','RECAG004','RECAG013')
                      OR fine_recapito_stato IN ('RECRS006','RECRS013','RECRN006','RECRN013','RECAG004','RECAG013')
                        THEN 'Oggetto esitato per Furto / Non Rendicontabile'
                    WHEN (controllo_causale = 1 OR controllo_date_business = 1 OR controllo_tripletta = 1
                          OR controllo_tempistiche_compiuta_giacenza = 1 OR controllo_inesito_casi_giacenza = 1
                          OR controllo_documentType = 1 OR flag_esiti_mancanti = 1)
                         AND fine_recapito_stato IS NOT NULL THEN 'Errore rendicontazione/assenza eventi intermedi'
                    WHEN (controllo_causale = 0 AND controllo_date_business = 0 AND controllo_tripletta = 0
                          AND controllo_tempistiche_compiuta_giacenza = 0 AND controllo_inesito_casi_giacenza = 0
                          AND controllo_documentType = 0 AND assenza_inesito = 0 AND assenza_RECAG012 = 0
                          AND assenza_pre_esito = 0 AND assenza_dematerializzazione_23l_ar_plico = 0
                          AND assenza_messa_in_giacenza = 0 AND assenza_demat_arcad = 0
                          AND flag_destinatario_deceduto = 0)
                         AND fine_recapito_stato IS NOT NULL THEN 'Oggetto con rendicontazione corretta in analisi Team Prodotto'
                    ELSE 'Oggetto in corso di postalizzazione'
                END AS stato_avanzamento_oggetti
            FROM temp_assenza_eventi t
        )
        SELECT DISTINCT requestid, stato_avanzamento_oggetti
        FROM finale
    """

    logging.info("Esecuzione query cluster_mancato_perfezionamento...")
    cluster_df = spark.sql(CLUSTER_SQL)
    cluster_df.createOrReplaceTempView("cluster_by_requestid")
    logging.info("Temp view cluster_by_requestid registrata.")

    # 6) anagrafica ente da gold_notification_analytics + selfcare.gold_contracts
    sender_mapping_df = spark.sql("""
        WITH selfcare_attiva AS (
            SELECT DISTINCT
                internalistitutionid,
                description,
                taxcode
            FROM selfcare.gold_contracts
        ),
        notif_sender AS (
            SELECT DISTINCT
                iun,
                senderpaid
            FROM send.gold_notification_analytics
            WHERE iun IN (SELECT iun FROM base_not_found_att)
              AND senderpaid IS NOT NULL
        )
        SELECT
            n.iun,
            n.senderpaid AS ente_id,
            s.description
        FROM notif_sender n
        LEFT JOIN selfcare_attiva s
            ON n.senderpaid = s.internalistitutionid
    """)
    logging.info("Calcolata anagrafica ente")
    sender_mapping_df.createOrReplaceTempView("sender_mapping")

    # 7) pagamento da gold_notification_analytics e da timeline
    payment_gold_df = spark.sql("""
        SELECT
            iun,
            MAX(tms_date_payment) AS tms_payment_gold
        FROM send.gold_notification_analytics
        WHERE iun IN (SELECT iun FROM base_not_found_att)
        GROUP BY iun
    """)
    logging.info("Calcolato pagamento da gold_notification_analytics")
    payment_gold_df.createOrReplaceTempView("payment_gold")

    payment_timeline_df = spark.sql("""
        SELECT
            iun,
            MAX(CAST(get_json_object(details, '$.eventTimestamp') AS TIMESTAMP)) AS tms_timeline_payment
        FROM send.silver_timeline
        WHERE category = 'PAYMENT'
          AND iun IN (SELECT iun FROM base_not_found_att)
        GROUP BY iun
    """)
    logging.info("Calcolato pagamento da silver_timeline")
    payment_timeline_df.createOrReplaceTempView("payment_timeline")

    # 8) output finale arricchito
    final_spark_df = spark.sql("""
        SELECT
            sm.description,
            sm.ente_id,
            ns.iun,
            b.attachment,
            tf.scheduled,
            pg.tms_payment_gold,
            pt.tms_timeline_payment,
            pl.requestid,
            CASE
                WHEN pl.ultimo_evento_stato NOT LIKE 'CON%' THEN pl.ultimo_evento_stato
            END AS ultimo_evento_stato_REC,
            CASE
                WHEN pl.ultimo_evento_stato NOT LIKE 'CON%' THEN pl.tms_ultimo_evento
            END AS tms_ultimo_evento_REC,
            c.stato_avanzamento_oggetti
        FROM notif_status_ok ns
        LEFT JOIN base_not_found_att b
            ON b.iun = ns.iun
        LEFT JOIN timeline_flags tf
            ON tf.iun = ns.iun
        LEFT JOIN payment_gold pg
            ON pg.iun = ns.iun
        LEFT JOIN payment_timeline pt
            ON pt.iun = ns.iun
        LEFT JOIN postalizzazione_last_events pl
            ON pl.iun = ns.iun
        LEFT JOIN cluster_by_requestid c
            ON c.requestid = pl.requestid
        LEFT JOIN sender_mapping sm
            ON sm.iun = ns.iun
    """)

    logging.info("Output Spark finale generato.")
    return final_spark_df


def enrich_with_lista_confronto(
    extracted_df: pd.DataFrame, lista_confronto_df: pd.DataFrame
) -> tuple[pd.DataFrame, int]:
    """
    Aggiunge il flag match rispetto al tab lista_confronto.
    Il matching avviene su 'iun'.
    """
    logging.info("Avvio arricchimento con tab lista_confronto...")

    extracted_df = extracted_df.copy()
    lista_confronto_df = lista_confronto_df.copy()

    if extracted_df.empty:
        logging.warning(
            "Il DataFrame estratto è vuoto. Nessun arricchimento possibile."
        )
        extracted_df["flag_match_lista_confronto"] = []
        return extracted_df, 0

    # normalizzazione colonne
    extracted_df["iun"] = extracted_df["iun"].astype(str).str.strip()

    if lista_confronto_df.empty:
        logging.warning("Il tab lista_confronto è vuoto o non leggibile.")
        extracted_df["flag_match_lista_confronto"] = "0"
        return extracted_df, 0

    normalized_columns = {c.lower().strip(): c for c in lista_confronto_df.columns}
    if "iun" not in normalized_columns:
        logging.warning(
            f"Nel tab lista_confronto non esiste una colonna 'iun'. Colonne trovate: {list(lista_confronto_df.columns)}"
        )
        extracted_df["flag_match_lista_confronto"] = "0"
        return extracted_df, 0

    iun_col = normalized_columns["iun"]
    lista_confronto_df[iun_col] = lista_confronto_df[iun_col].astype(str).str.strip()

    set_iun_lista = {
        iun
        for iun in lista_confronto_df[iun_col].tolist()
        if iun not in ["", "-", "nan", "None", "NaT"]
    }

    extracted_df["flag_match_lista_confronto"] = extracted_df["iun"].apply(
        lambda x: "1" if x in set_iun_lista else "0"
    )

    matched_count = int((extracted_df["flag_match_lista_confronto"] == "1").sum())

    logging.info(f"Match trovati con lista_confronto: {matched_count}")
    return extracted_df, matched_count


def reorder_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Riordina le colonne:
    """
    desired_order = [
        "description",
        "ente_id",
        "flag_match_lista_confronto",
        "iun",
        "attachment",
        "scheduled",
        "tms_payment_gold",
        "tms_timeline_payment",
        "requestid",
        "ultimo_evento_stato_REC",
        "tms_ultimo_evento_REC",
        "stato_avanzamento_oggetti",
    ]

    for col in desired_order:
        if col not in df.columns:
            df[col] = "-"

    return df[desired_order].copy()


def build_log_df(
    max_date_str: str,
    job_time_utc: str,
    estrazione_df: pd.DataFrame,
    lista_confronto_df: pd.DataFrame,
    matched_count: int,
) -> pd.DataFrame:
    """
    Costruisce il tab Log di controllo.
    """
    log_df = pd.DataFrame(
        {
            "Ultimo aggiornamento dati (MAX requesttimestamp)": [max_date_str],
            "Data esecuzione script (UTC)": [job_time_utc],
            f"Numero righe estratte tab '{TAB_OUTPUT}'": [len(estrazione_df)],
            f"Numero righe lette da tab '{TAB_LISTA_CONFRONTO}'": [
                len(lista_confronto_df)
            ],
            f"Numero righe di '{TAB_OUTPUT}' matchate con '{TAB_LISTA_CONFRONTO}'": [
                matched_count
            ],
        }
    )

    return log_df


# ---------------- MAIN SCRIPT ----------------
def main():
    logging.info("Inizializzazione SparkSession...")
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    job_start_dt = now_utc_dt()
    job_time_utc = job_start_dt.strftime("%Y-%m-%d %H:%M:%S")
    run_id = job_start_dt.strftime("%Y%m%dT%H%M%SZ")
    hostname = socket.gethostname()

    slack_prefix = build_slack_prefix()
    slack_webhooks = load_slack_webhooks(SLACK_CONFIG_CANDIDATE_PATHS)
    slack_webhook_url = get_slack_webhook_by_name(slack_webhooks, SLACK_WEBHOOK_NAME)

    estrazione_df = pd.DataFrame()
    lista_confronto_df = pd.DataFrame()
    matched_count = 0
    max_date_str = "-"

    try:
        logging.info(f"Timestamp esecuzione job (UTC): {job_time_utc}")

        creds = load_google_credentials(GOOGLE_SECRET_PATH)
        sheet = get_sheet_client(creds)

        # 1) Lettura lista confronto
        lista_confronto_df = safe_download_sheet(sheet, TAB_LISTA_CONFRONTO)

        # 2) Estrazione principale
        final_spark_df = run_query_not_found_att_enhanced(spark)
        estrazione_df = spark_to_df_per_gsheet(final_spark_df, missing="-")

        logging.info(f"Righe estratte dalla query principale: {len(estrazione_df)}")

        # 3) Match con lista confronto
        estrazione_df, matched_count = enrich_with_lista_confronto(
            extracted_df=estrazione_df, lista_confronto_df=lista_confronto_df
        )

        # 4) Riordino colonne
        estrazione_df = reorder_output_columns(estrazione_df)

        # 5) Log di controllo
        max_date_str = get_last_update_date(spark)
        logging.info(f"Ultimo aggiornamento dati gold_postalizzazione: {max_date_str}")

        log_df = build_log_df(
            max_date_str=max_date_str,
            job_time_utc=job_time_utc,
            estrazione_df=estrazione_df,
            lista_confronto_df=lista_confronto_df,
            matched_count=matched_count,
        )

        # 6) Export
        export_to_sheets(estrazione_df, creds, TAB_OUTPUT)
        export_to_sheets(log_df, creds, TAB_LOG)

        job_end_dt = now_utc_dt()
        job_elapsed_min = elapsed_minutes_str(job_start_dt, job_end_dt)

        payment_gold_count = (
            int((estrazione_df["tms_payment_gold"] != "-").sum())
            if not estrazione_df.empty and "tms_payment_gold" in estrazione_df.columns
            else 0
        )

        payment_timeline_count = (
            int((estrazione_df["tms_timeline_payment"] != "-").sum())
            if not estrazione_df.empty
            and "tms_timeline_payment" in estrazione_df.columns
            else 0
        )

        slack_msg = (
            f"{slack_prefix}\n"
            f"✅✅✅ *SUCCESS* ✅✅✅\n"
            f"*Job:* {APP_NAME}\n"
            f"*RunId (UTC):* {run_id}\n"
            f"*Host:* {hostname}\n"
            f"*Tab output:* {TAB_OUTPUT}\n"
            f"*Tab lista confronto:* {TAB_LISTA_CONFRONTO}\n"
            f"*Tab log:* {TAB_LOG}\n"
            f"*Ultimo aggiornamento dati:* {max_date_str}\n"
            f"*Righe estratte:* {len(estrazione_df)}\n"
            f"*Colonne estratte:* {len(estrazione_df.columns)}\n"
            f"*Righe lista confronto:* {len(lista_confronto_df)}\n"
            f"*Match lista confronto:* {matched_count}\n"
            f"*Notifiche con pagamento gold:* {payment_gold_count}\n"
            f"*Notifiche con pagamento timeline:* {payment_timeline_count}\n"
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
            f"*Tab output:* {TAB_OUTPUT}\n"
            f"*Tab lista confronto:* {TAB_LISTA_CONFRONTO}\n"
            f"*Tab log:* {TAB_LOG}\n"
            f"*Ultimo aggiornamento dati eventualmente recuperato:* {max_date_str}\n"
            f"*Righe estratte prima dell'errore:* {len(estrazione_df)}\n"
            f"*Righe lista confronto lette prima dell'errore:* {len(lista_confronto_df)}\n"
            f"*Match lista confronto prima dell'errore:* {matched_count}\n"
            f"Il job non è terminato correttamente.\n"
        )

        try:
            send_slack(slack_webhook_url, fail_msg)
        finally:
            raise

    finally:
        spark.stop()
        logging.info("Spark session terminata.")


if __name__ == "__main__":
    main()
