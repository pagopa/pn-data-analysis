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

APP_NAME = "SSDA-819_Weekly_ARCAD_Mancanti_Post_Perfezionamento"
GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"

SHEET_ID = "1gtalIxwHVftk5DQiXmk7MGt8o67t1XGciGlszwWFtxU"

TAB_LOG = "Log di controllo"
TAB_FULMINE = "Fulmine"
TAB_POST_SERVICE = "POST & SERVICE"
TAB_SAILPOST = "RTI Sailpost-Snem"
TAB_POSTE = "Poste"

TAB_BY_RECAPITISTA = {
    "Fulmine": TAB_FULMINE,
    "POST & SERVICE": TAB_POST_SERVICE,
    "RTI Sailpost-Snem": TAB_SAILPOST,
    "Poste": TAB_POSTE,
}

SLACK_CONFIG_CANDIDATE_PATHS = [
    "/Slack/notifications_webhook.txt",
    "/app/mount/Slack/notifications_webhook.txt",
    "/app/mount/notifications_webhook.txt",
]
SLACK_WEBHOOK_NAME = "webhook_1"
SLACK_LABEL = "SSDA-819 Weekly ARCAD Mancanti Post Perfezionamento"

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


# ---------------- FUNZIONI ESPORTAZIONE ----------------


def get_sheet_client(creds: dict) -> Sheet:
    """Restituisce il client Sheet già configurato."""
    return Sheet(sheet_id=SHEET_ID, service_credentials=creds, id_mode="key")


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


def spark_to_sheets(
    df_spark, creds: dict, sheet_name: str, missing: str = "-"
) -> pd.DataFrame:
    """
    Pipeline completa Spark -> Google Sheets:
      1. Converte il DataFrame Spark in Pandas
      2. Delega a export_to_sheets la sanitizzazione, il resize e l'upload

    Restituisce il DataFrame Pandas sanitizzato e scritto.
    """
    if df_spark is None:
        logging.warning(f"DataFrame Spark nullo, skip scrittura su tab '{sheet_name}'.")
        return pd.DataFrame()

    logging.info(f"Conversione DataFrame Spark -> Pandas per tab '{sheet_name}'...")
    df = df_spark.toPandas()
    logging.info(f"Conversione completata. Righe convertite: {len(df)}")

    return export_to_sheets(df, creds, sheet_name, missing=missing)


def export_to_sheets(
    df: pd.DataFrame, creds: dict, sheet_name: str, missing: str = "-"
) -> pd.DataFrame:

    logging.info(f"Scrittura su Google Sheet: tab '{sheet_name}'...")

    df = sanitize_for_sheets(df, missing=missing)

    if df.empty:
        logging.info(
            f"DataFrame vuoto per '{sheet_name}'. Forzo resize a {INITIAL_WS_ROWS} righe."
        )
        target_rows = INITIAL_WS_ROWS
    else:
        target_rows = len(df) + 1  # header + dati

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

    return df


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
    """
    Costruisce il DataFrame base finale, che viene poi riutilizzato
    per i tab dei recapitisti e per i conteggi di log.
    """
    logging.info("Costruzione DataFrame base ARCAD mancanti post perfezionamento...")

    query = """
    WITH dati_gold_corretti AS (
        SELECT
            iun,
            requestid,
            requesttimestamp,
            codice_oggetto,
            geokey,
            lotto,
            affido_recapitista_con016_data,
            accettazione_recapitista_con018_data,
            recapitista_unificato,
            perfezionamento_data,
            certificazione_recapito_stato,
            demat_arcad_stato,
            accettazione_23l_recag012_data,
            rend_23l_stato,
            CASE
                WHEN codice_oggetto LIKE 'R14%' THEN 'Fulmine'
                WHEN codice_oggetto LIKE '777%' OR codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
                WHEN codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
                WHEN codice_oggetto LIKE '69%' OR codice_oggetto LIKE '381%' OR codice_oggetto LIKE 'RB1%' THEN 'Poste'
                ELSE recapitista_unificato
            END AS recapitista_unif
        FROM send.gold_postalizzazione_analytics
    ),
    oggetti_senza_arcad AS (
        SELECT
            iun,
            codice_oggetto,
            requestid,
            geokey,
            lotto,
            COALESCE(
                LEAST(accettazione_recapitista_con018_data, affido_recapitista_con016_data),
                accettazione_recapitista_con018_data,
                affido_recapitista_con016_data,
                requesttimestamp
            ) AS affido_accettazione_rec_data,
            recapitista_unif,
            requesttimestamp
        FROM dati_gold_corretti
        WHERE perfezionamento_data IS NOT NULL
          AND certificazione_recapito_stato IN ('RECAG005A','RECAG006A','RECAG007A','RECAG008A')
          AND demat_arcad_stato IS NULL
          AND accettazione_23l_recag012_data IS NOT NULL
          AND rend_23l_stato IS NOT NULL
    )
    SELECT
        iun,
        codice_oggetto,
        requestid,
        geokey,
        lotto,
        affido_accettazione_rec_data,
        recapitista_unif,
        requesttimestamp
    FROM oggetti_senza_arcad
    """

    df = spark.sql(query)
    logging.info("DataFrame base costruito correttamente.")
    return df


def get_last_update_date(spark: SparkSession) -> str:
    """Estrae MAX(requesttimestamp) dalla gold_postalizzazione_analytics."""
    logging.info("Estrazione data ultimo aggiornamento...")

    max_date_df = spark.sql(
        """
        SELECT MAX(requesttimestamp) AS max_ts
        FROM send.gold_postalizzazione_analytics
        """
    )
    max_date = max_date_df.collect()[0]["max_ts"]

    if isinstance(max_date, datetime):
        return max_date.strftime("%Y-%m-%d %H:%M:%S")
    return str(max_date) if max_date is not None else "-"


def build_log_df(
    max_date_str: str,
    start_run_str: str,
    end_run_str: str = "-",
    elapsed_min_str: str = "IN CORSO",
    stato_run: str = "IN CORSO",
    righe_output: int = 0,
    counts_by_recapitista_pdf: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Costruisce il tab di log con informazioni e metriche sintetiche."""

    counts_map = {}

    if counts_by_recapitista_pdf is not None and not counts_by_recapitista_pdf.empty:
        counts_map = dict(
            zip(
                counts_by_recapitista_pdf["recapitista_unif"],
                counts_by_recapitista_pdf["numero_righe"],
            )
        )

    df = pd.DataFrame(
        {
            "Ultimo aggiornamento dati (MAX requesttimestamp)": [max_date_str],
            "Start run time (UTC)": [start_run_str],
            "End run time (UTC)": [end_run_str],
            "Tempo impiegato (min)": [elapsed_min_str],
            "Stato run": [stato_run],
            "Righe output totali": [int(righe_output)],
            "# Fulmine": [int(counts_map.get("Fulmine", 0))],
            "# POST & SERVICE": [int(counts_map.get("POST & SERVICE", 0))],
            "# RTI Sailpost-Snem": [int(counts_map.get("RTI Sailpost-Snem", 0))],
            "# Poste": [int(counts_map.get("Poste", 0))],
        }
    )

    return sanitize_for_sheets(df)


def write_tab_log(
    creds: dict,
    max_date_str: str,
    start_run_str: str,
    end_run_str: str = "-",
    elapsed_min_str: str = "IN CORSO",
    stato_run: str = "IN CORSO",
    righe_output: int = 0,
    counts_by_recapitista_pdf: pd.DataFrame | None = None,
):
    """Scrive il tab di log con informazioni di run e righe per recapitista."""
    log_df = build_log_df(
        max_date_str=max_date_str,
        start_run_str=start_run_str,
        end_run_str=end_run_str,
        elapsed_min_str=elapsed_min_str,
        stato_run=stato_run,
        righe_output=righe_output,
        counts_by_recapitista_pdf=counts_by_recapitista_pdf,
    )

    export_to_sheets(log_df, creds, TAB_LOG)


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

    max_date_str = "-"
    righe_output = 0
    counts_by_recapitista_pdf = pd.DataFrame()
    base_df = None

    try:
        creds = load_google_credentials(GOOGLE_SECRET_PATH)
        max_date_str = get_last_update_date(spark)

        write_tab_log(
            creds=creds,
            max_date_str=max_date_str,
            start_run_str=job_start_str,
            end_run_str="-",
            elapsed_min_str="IN CORSO",
            stato_run="IN CORSO",
            righe_output=0,
            counts_by_recapitista_pdf=pd.DataFrame(),
        )

        # Base unica riusata per output e metriche di log
        base_df = run_query(spark).persist(StorageLevel.MEMORY_AND_DISK)

        logging.info("Materializzazione DataFrame base persistito...")
        righe_output = base_df.count()
        logging.info(f"Materializzazione completata. Righe base: {righe_output}")

        counts_by_recapitista_df = (
            base_df.groupBy("recapitista_unif")
            .count()
            .withColumnRenamed("count", "numero_righe")
            .orderBy("recapitista_unif")
        )

        counts_by_recapitista_pdf = counts_by_recapitista_df.toPandas()

        write_tab_log(
            creds=creds,
            max_date_str=max_date_str,
            start_run_str=job_start_str,
            end_run_str="-",
            elapsed_min_str="IN CORSO",
            stato_run="IN CORSO",
            righe_output=righe_output,
            counts_by_recapitista_pdf=counts_by_recapitista_pdf,
        )

        logging.info("Tab di log aggiornato correttamente in stato IN CORSO.")

        logging.info(
            f"Recapitisti configurati per export: {list(TAB_BY_RECAPITISTA.keys())}"
        )

        righe_per_tab = {}

        for recapitista, tab_name in TAB_BY_RECAPITISTA.items():

            logging.info(
                f"Elaborazione tab per recapitista '{recapitista}' "
                f"su worksheet '{tab_name}'..."
            )

            rec_df = (
                base_df.filter(base_df["recapitista_unif"] == recapitista)
                .select(
                    "iun",
                    "codice_oggetto",
                    "requestid",
                    "geokey",
                    "lotto",
                    "affido_accettazione_rec_data",
                )
                .orderBy(
                    "affido_accettazione_rec_data",
                    "iun",
                    "requestid",
                )
            )

            rec_count = rec_df.count()
            righe_per_tab[recapitista] = int(rec_count)

            logging.info(
                f"Righe da scrivere per recapitista '{recapitista}': {rec_count}"
            )

            spark_to_sheets(rec_df, creds, tab_name)

        job_end_dt = now_utc_dt()
        job_end_str = job_end_dt.strftime("%Y-%m-%d %H:%M:%S")
        job_elapsed_min = elapsed_minutes_str(job_start_dt, job_end_dt)

        write_tab_log(
            creds=creds,
            max_date_str=max_date_str,
            start_run_str=job_start_str,
            end_run_str=job_end_str,
            elapsed_min_str=job_elapsed_min,
            stato_run="OK",
            righe_output=righe_output,
            counts_by_recapitista_pdf=counts_by_recapitista_pdf,
        )

        righe_per_tab_str = (
            "\n".join(
                [
                    f"- {recapitista}: {num_righe}"
                    for recapitista, num_righe in righe_per_tab.items()
                ]
            )
            if righe_per_tab
            else "- Nessun tab scritto"
        )

        slack_msg = (
            f"{slack_prefix}\n"
            f"✅✅✅ *SUCCESS* ✅✅✅\n"
            f"*Job:* {APP_NAME}\n"
            f"*RunId (UTC):* {run_id}\n"
            f"*Host:* {hostname}\n"
            f"*Ultimo aggiornamento dati:* {max_date_str}\n"
            f"*Righe output totali:* {righe_output}\n"
            f"*Dettaglio righe per recapitista:*\n{righe_per_tab_str}\n"
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
            write_tab_log(
                creds=creds,
                max_date_str=max_date_str,
                start_run_str=job_start_str,
                end_run_str=job_end_str,
                elapsed_min_str=job_elapsed_min,
                stato_run="KO",
                righe_output=righe_output,
                counts_by_recapitista_pdf=counts_by_recapitista_pdf,
            )
        except Exception:
            logging.warning(
                "Aggiornamento del tab di log fallito durante la gestione errore."
            )

        fail_msg = (
            f"{slack_prefix}\n"
            f"❌❌❌ *FAILURE* ❌❌❌\n"
            f"*Job:* {APP_NAME}\n"
            f"*RunId (UTC):* {run_id}\n"
            f"*Host:* {hostname}\n"
            f"*Ultimo aggiornamento dati:* {max_date_str}\n"
            f"Il job non è terminato correttamente.\n"
        )

        try:
            send_slack(slack_webhook_url, fail_msg)
        finally:
            raise

    finally:
        try:
            if base_df is not None:
                base_df.unpersist(blocking=True)
                logging.info("base_df rilasciato dalla cache.")
        except Exception:
            logging.warning("unpersist fallito per base_df.")

        try:
            spark.stop()
            logging.info("Spark session terminata.")
        except Exception:
            logging.warning("spark.stop() fallito in chiusura.")


if __name__ == "__main__":
    main()
