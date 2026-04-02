import json
import logging
import os
import socket
import urllib.request
from datetime import datetime, timedelta, timezone

import gspread
import numpy as np
import pandas as pd
from google.oauth2 import service_account
from pdnd_google_utils import Sheet
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.storagelevel import StorageLevel

# ---------------- CONFIGURAZIONE BASE ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

APP_NAME = "SSDA-740 WI7 Consolidatore"
GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"

# Spreadsheet di destinazione
SHEET_ID = "1spqQRzA-SKYoPj-8gBII_tgOa1EM5-bNA5pdlYCYg3g"

# Tab
TAB_META = "Data aggiornamento"
TAB_DETAIL = "Dettaglio WI7 consolidatore"

SLACK_CONFIG_CANDIDATE_PATHS = [
    "/Slack/notifications_webhook.txt",
    "/app/mount/Slack/notifications_webhook.txt",
    "/app/mount/notifications_webhook.txt",
]
SLACK_WEBHOOK_NAME = "webhook_1"
SLACK_LABEL = "SSDA-740 WI7 Consolidatore"

SHEETS_CHUNK_SIZE = 15000
SHEETS_CELL_LIMIT = 10_000_000
INITIAL_WS_ROWS = 2
COLLAPSE_DELIMITER = ","

GSHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]


# ---------------- ERRORI DEDICATI ----------------
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


# ---------------- FUNZIONI GENERALI ----------------
def load_google_credentials(secret_path: str) -> dict:
    creds = {
        name: open(os.path.join(secret_path, name)).read().strip()
        for name in os.listdir(secret_path)
        if os.path.isfile(os.path.join(secret_path, name))
    }
    logging.info(f"Credenziali caricate: {list(creds.keys())}")
    return creds


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


def export_to_sheets_chunked(
    df: pd.DataFrame,
    creds: dict,
    sheet_id: str,
    sheet_name: str,
    chunk_size: int = SHEETS_CHUNK_SIZE,
    expected_written_cols: int | None = None,
):
    """
    Upload su Google Sheet a chunk:
    - primo chunk: clear + header
    - chunk successivi: append tramite ul_cell
    """
    logging.info(f"Scrittura su Google Sheet (chunked): {sheet_name}")

    if df is None:
        df = pd.DataFrame()

    total_rows = len(df)
    logging.info(f"Totale righe da caricare: {total_rows} (chunk_size={chunk_size})")

    if expected_written_cols is not None:
        would_write = int(total_rows) * int(expected_written_cols)
        if would_write >= SHEETS_CELL_LIMIT:
            raise CapacityLimitError(
                f"Export '{sheet_name}' su sheet_id={sheet_id}: scrittura prevista "
                f"{would_write} celle (rows={total_rows} * cols={expected_written_cols}) "
                f">= {SHEETS_CELL_LIMIT}."
            )

        _resize_worksheet_grid_best_effort(
            creds=creds,
            sheet_id=sheet_id,
            worksheet_name=sheet_name,
            target_rows=INITIAL_WS_ROWS,
            target_cols=expected_written_cols,
        )

    sheet = Sheet(sheet_id=sheet_id, service_credentials=creds, id_mode="key")

    if total_rows == 0:
        logging.info(f"Nessuna riga da caricare per '{sheet_name}'. Pulizia foglio.")
        sheet.upload(
            worksheet_name=sheet_name,
            panda_df=df.astype(str),
            ul_cell="A1",
            header=True,
            clear_on_open=True,
        )
        logging.info("Scrittura completata (chunked, empty).")
        return

    for i, start in enumerate(range(0, total_rows, chunk_size)):
        end = min(start + chunk_size, total_rows)
        chunk = df.iloc[start:end]

        is_first = i == 0
        header = is_first
        clear_on_open = is_first
        ul_cell = "A1" if is_first else f"A{start + 2}"

        logging.info(
            f"Upload righe {start}-{end} su '{sheet_name}' @ {ul_cell} "
            f"(header={header}, clear={clear_on_open})"
        )

        sheet.upload(
            worksheet_name=sheet_name,
            panda_df=chunk.astype(str),
            ul_cell=ul_cell,
            header=header,
            clear_on_open=clear_on_open,
        )

    logging.info("Scrittura completata (chunked).")


def get_last_update_date(spark: SparkSession) -> str:
    logging.info("Estrazione data ultimo aggiornamento...")
    max_date_df = spark.sql(
        "SELECT MAX(requesttimestamp) AS max_ts FROM send.gold_postalizzazione_analytics"
    )
    max_date = max_date_df.collect()[0]["max_ts"]
    if isinstance(max_date, datetime):
        return max_date.strftime("%Y-%m-%d %H:%M:%S")
    return str(max_date)


# ---------------- META ----------------
def build_meta_df(
    max_date_str: str,
    start_run_str: str,
    end_run_str: str = "-",
    elapsed_min_str: str = "IN CORSO",
    stato_run: str = "IN CORSO",
    righe_output: int = 0,
    colonne_output: int = 0,
    celle_output: int = 0,
    collapse_applied: str = "NO",
    collapse_reason: str = "-",
) -> pd.DataFrame:
    return (
        pd.DataFrame(
            {
                "Ultimo aggiornamento dati (MAX requesttimestamp)": [max_date_str],
                "Start run time (UTC)": [start_run_str],
                "End run time (UTC)": [end_run_str],
                "Tempo impiegato (min)": [elapsed_min_str],
                "Stato run": [stato_run],
                "Righe output": [righe_output],
                "Colonne output": [colonne_output],
                "Celle output": [celle_output],
                "Collapse applied": [collapse_applied],
                "Collapse reason": [collapse_reason],
            }
        )
        .replace([np.inf, -np.inf], np.nan)
        .fillna("-")
        .astype(str)
    )


def write_tab_meta(
    creds: dict,
    max_date_str: str,
    start_run_str: str,
    end_run_str: str = "-",
    elapsed_min_str: str = "IN CORSO",
    stato_run: str = "IN CORSO",
    righe_output: int = 0,
    colonne_output: int = 0,
    celle_output: int = 0,
    collapse_applied: str = "NO",
    collapse_reason: str = "-",
):
    update_df = build_meta_df(
        max_date_str=max_date_str,
        start_run_str=start_run_str,
        end_run_str=end_run_str,
        elapsed_min_str=elapsed_min_str,
        stato_run=stato_run,
        righe_output=righe_output,
        colonne_output=colonne_output,
        celle_output=celle_output,
        collapse_applied=collapse_applied,
        collapse_reason=collapse_reason,
    )

    export_to_sheets_chunked(
        update_df,
        creds,
        SHEET_ID,
        TAB_META,
        chunk_size=SHEETS_CHUNK_SIZE,
        expected_written_cols=len(update_df.columns),
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


# ---------------- FESTIVITÀ E WORKING DAYS ----------------
FESTIVITA = [
    ("2023-01-01", "Capodanno"),
    ("2023-01-06", "Epifania"),
    ("2023-04-09", "Pasqua"),
    ("2023-04-10", "Lunedì dell'Angelo"),
    ("2023-04-25", "Festa della Liberazione"),
    ("2023-05-01", "Festa dei Lavoratori"),
    ("2023-06-02", "Festa della Repubblica"),
    ("2023-08-15", "Ferragosto"),
    ("2023-11-01", "Tutti i Santi"),
    ("2023-12-08", "Immacolata Concezione"),
    ("2023-12-25", "Natale"),
    ("2023-12-26", "Santo Stefano"),
    ("2024-01-01", "Capodanno"),
    ("2024-01-06", "Epifania"),
    ("2024-03-31", "Pasqua"),
    ("2024-04-01", "Lunedì dell'Angelo"),
    ("2024-04-25", "Festa della Liberazione"),
    ("2024-05-01", "Festa dei Lavoratori"),
    ("2024-06-02", "Festa della Repubblica"),
    ("2024-08-15", "Ferragosto"),
    ("2024-11-01", "Tutti i Santi"),
    ("2024-12-25", "Natale"),
    ("2024-12-26", "Santo Stefano"),
    ("2025-01-01", "Capodanno"),
    ("2025-01-06", "Epifania"),
    ("2025-04-20", "Pasqua"),
    ("2025-04-21", "Lunedì dell'Angelo"),
    ("2025-04-25", "Festa della Liberazione"),
    ("2025-05-01", "Festa dei Lavoratori"),
    ("2025-06-02", "Festa della Repubblica"),
    ("2025-08-15", "Ferragosto"),
    ("2025-11-01", "Ognissanti"),
    ("2025-12-08", "Immacolata Concezione"),
    ("2025-12-25", "Natale"),
    ("2025-12-26", "Santo Stefano"),
    ("2026-01-01", "Capodanno"),
    ("2026-01-06", "Epifania"),
    ("2026-04-05", "Pasqua"),
    ("2026-04-06", "Lunedì dell'Angelo"),
    ("2026-04-25", "Festa della Liberazione"),
    ("2026-05-01", "Festa dei Lavoratori"),
    ("2026-06-02", "Festa della Repubblica"),
    ("2026-08-15", "Ferragosto"),
    ("2026-10-04", "San Francesco d'Assisi"),
    ("2026-11-01", "Tutti i Santi"),
    ("2026-12-08", "Immacolata Concezione"),
    ("2026-12-25", "Natale"),
    ("2026-12-26", "Santo Stefano"),
]

holiday_dates = {datetime.strptime(d, "%Y-%m-%d").date() for d, _ in FESTIVITA}


def datediff_workdays(start_date, end_date):
    try:
        if start_date is None or end_date is None:
            return None

        if end_date < start_date:
            return 0

        start_date_next_day = start_date.replace(
            hour=0, minute=0, second=0, microsecond=0
        ) + timedelta(days=1)

        total_days = (end_date - start_date_next_day).days
        if total_days < 0:
            return 0

        days_list = [
            start_date_next_day + timedelta(days=i) for i in range(total_days + 1)
        ]

        working_days = [
            d for d in days_list if d.weekday() < 5 and d.date() not in holiday_dates
        ]

        return len(working_days)

    except Exception:
        return None


datediff_workdays_udf = F.udf(datediff_workdays, IntegerType())


# ---------------- COLLAPSE ----------------
def _collassa_in_una_colonna(df_spark, delimiter: str, missing: str = "-"):
    """
    Collassa il DF in un'unica colonna, concatenando i valori riga per riga con un delimitatore.
    Normalizzazione difensiva: sostituisce \r e \n con spazio.
    """
    cols = []
    for c in df_spark.columns:
        col_str = F.coalesce(F.col(c).cast("string"), F.lit(missing))
        col_str = F.regexp_replace(col_str, r"[\r\n]+", " ")
        cols.append(col_str)

    return df_spark.select(F.concat_ws(delimiter, *cols).alias("dati"))


def prepare_output_for_gsheet(df_spark, delimiter: str = COLLAPSE_DELIMITER):
    """
    Prepara il dataframe per l'export su Google Sheets.
    Se il numero stimato di celle supera il limite, applica il collasso in una colonna.
    Restituisce:
      - df_pd pronto per upload
      - meta informazioni utili per log/meta/slack
    """
    if df_spark is None:
        empty_df = pd.DataFrame()
        return empty_df, {
            "righe_output": 0,
            "colonne_output": 0,
            "celle_output": 0,
            "collapse_applied": "NO",
            "collapse_reason": "-",
            "delimiter": "-",
            "chunk_size_used": SHEETS_CHUNK_SIZE,
            "expected_written_cols": 0,
        }

    logging.info("Calcolo righe dataset output (count)...")
    n_rows = df_spark.count()
    n_cols = len(df_spark.columns)
    n_cells = n_rows * n_cols

    logging.info(
        f"Dimensioni output: righe={n_rows}, colonne={n_cols}, celle={n_cells} "
        f"(limite={SHEETS_CELL_LIMIT})"
    )

    if n_rows >= SHEETS_CELL_LIMIT:
        raise CapacityLimitError(
            f"Righe={n_rows} >= {SHEETS_CELL_LIMIT}. "
            "Anche collassando a 1 colonna si raggiungerebbe/supererebbe il limite celle; export impossibile."
        )

    collapse_applied = False
    collapse_reason = "-"
    used_delimiter = "-"

    if n_cells >= SHEETS_CELL_LIMIT:
        collapse_applied = True
        collapse_reason = "LIMITE_CELLE"
        used_delimiter = delimiter

        logging.warning(
            f"Superato limite celle (celle={n_cells} >= {SHEETS_CELL_LIMIT}). "
            f"Applico collasso in 1 colonna con delimitatore='{delimiter}'."
        )

        header_str = delimiter.join(df_spark.columns)
        df_spark_out = _collassa_in_una_colonna(
            df_spark, delimiter=delimiter, missing="-"
        )
        out_cols = 1
        out_cells = n_rows * out_cols

        logging.info("toPandas output collassato...")
        df_pd = spark_to_df_per_gsheet(df_spark_out, missing="-")

        if df_pd.shape[1] != 1:
            raise RuntimeError(
                f"Collasso attivo ma dataframe pandas non è a 1 colonna (colonne={df_pd.shape[1]})."
            )

        df_pd.columns = [header_str]

    else:
        df_spark_out = df_spark
        out_cols = n_cols
        out_cells = n_cells

        logging.info("toPandas output standard...")
        df_pd = spark_to_df_per_gsheet(df_spark_out, missing="-")

    return df_pd, {
        "righe_output": int(n_rows),
        "colonne_output": int(out_cols),
        "celle_output": int(out_cells),
        "collapse_applied": "SI" if collapse_applied else "NO",
        "collapse_reason": collapse_reason,
        "delimiter": used_delimiter,
        "chunk_size_used": (
            int(SHEETS_CHUNK_SIZE * 2) if collapse_applied else int(SHEETS_CHUNK_SIZE)
        ),
        "expected_written_cols": int(out_cols),
    }


# ---------------- QUERY / LOGICA WI7 ----------------
def build_wi7_query(spark: SparkSession):
    logging.info("Esecuzione query base WI7 consolidatore...")

    df_start = spark.sql(
        """
        SELECT DISTINCT
            gpa.senderpaid,
            gpa.requestid,
            gpa.requesttimestamp,
            gpa.prodotto,
            gpa.geokey,
            CASE
                WHEN gpa.affido_consolidatore_data IS NULL THEN gpa.requesttimestamp
                ELSE gpa.affido_consolidatore_data
            END AS affido_consolidatore_data,
            gpa.stampa_imbustamento_con080_data,
            gpa.materialita_pronta_con09a_data,
            gpa.affido_recapitista_con016_data,
            gpa.accettazione_recapitista_con018_data
        FROM send.gold_postalizzazione_analytics AS gpa
        JOIN send.gold_notification_analytics AS gna
            ON gpa.iun = gna.iun
        WHERE gpa.accettazione_recapitista_con018_data IS NULL
          AND gpa.scarto_consolidatore_stato IS NULL
          AND gpa.pcretry_rank = 1
          AND gpa.tentativo_recapito_stato IS NULL
          AND gpa.messaingiacenza_recapito_stato IS NULL
          AND gpa.certificazione_recapito_stato IS NULL
          AND gpa.fine_recapito_stato IS NULL
          AND gpa.demat_23l_ar_stato IS NULL
          AND gpa.demat_plico_stato IS NULL
          AND gpa.iun NOT IN (
                SELECT iun
                FROM send.silver_incident_iun
          )
          AND gna.actual_status <> 'CANCELLED'
          AND gpa.statusrequest NOT IN (
                'PN999',
                'PN998',
                'error',
                'internalError',
                'syntaxError',
                'transformationError',
                'semanticError',
                'authenticationError',
                'duplicatedRequest',
                'booked'
          )
        """
    )

    df_clustered = (
        df_start.withColumn(
            "Cluster",
            F.when(
                F.col("stampa_imbustamento_con080_data").isNull()
                & (
                    datediff_workdays_udf(
                        F.col("affido_consolidatore_data"),
                        F.current_timestamp(),
                    )
                    >= 2
                ),
                "Stampa Imbustamento",
            )
            .when(
                F.col("materialita_pronta_con09a_data").isNull()
                & (
                    datediff_workdays_udf(
                        F.col("stampa_imbustamento_con080_data"),
                        F.current_timestamp(),
                    )
                    >= 2
                ),
                "Materialita Pronta",
            )
            .when(
                F.col("affido_recapitista_con016_data").isNull()
                & (
                    datediff_workdays_udf(
                        F.col("stampa_imbustamento_con080_data"),
                        F.current_timestamp(),
                    )
                    >= 2
                ),
                "Pick Up",
            )
            .when(
                F.col("accettazione_recapitista_con018_data").isNull()
                & (
                    datediff_workdays_udf(
                        F.col("affido_recapitista_con016_data"),
                        F.current_timestamp(),
                    )
                    >= 1
                ),
                "Accettazione Recapitista",
            ),
        )
        .filter(F.col("Cluster").isNotNull())
        .withColumn(
            "Priority",
            F.when(F.col("Cluster") == "Stampa Imbustamento", 4)
            .when(F.col("Cluster") == "Materialita Pronta", 3)
            .when(F.col("Cluster") == "Pick Up", 2)
            .when(F.col("Cluster") == "Accettazione Recapitista", 1),
        )
    )

    df_computed = (
        df_clustered.withColumn(
            "differenza_gg_lavorativi",
            F.when(
                F.col("Cluster") == "Stampa Imbustamento",
                datediff_workdays_udf(
                    F.col("affido_consolidatore_data"), F.current_timestamp()
                ),
            )
            .when(
                F.col("Cluster") == "Materialita Pronta",
                datediff_workdays_udf(
                    F.col("stampa_imbustamento_con080_data"), F.current_timestamp()
                ),
            )
            .when(
                F.col("Cluster") == "Pick Up",
                datediff_workdays_udf(
                    F.col("stampa_imbustamento_con080_data"), F.current_timestamp()
                ),
            )
            .when(
                F.col("Cluster") == "Accettazione Recapitista",
                datediff_workdays_udf(
                    F.col("affido_recapitista_con016_data"), F.current_timestamp()
                ),
            ),
        )
        .withColumn(
            "gg_fuori_sla",
            F.when(
                F.col("Cluster") == "Stampa Imbustamento",
                datediff_workdays_udf(
                    F.col("affido_consolidatore_data"), F.current_timestamp()
                )
                - 2,
            )
            .when(
                F.col("Cluster") == "Materialita Pronta",
                datediff_workdays_udf(
                    F.col("stampa_imbustamento_con080_data"), F.current_timestamp()
                )
                - 2,
            )
            .when(
                F.col("Cluster") == "Pick Up",
                datediff_workdays_udf(
                    F.col("stampa_imbustamento_con080_data"), F.current_timestamp()
                )
                - 2,
            )
            .when(
                F.col("Cluster") == "Accettazione Recapitista",
                datediff_workdays_udf(
                    F.col("affido_recapitista_con016_data"), F.current_timestamp()
                )
                - 1,
            ),
        )
        .filter(F.col("gg_fuori_sla") > 0)
    )

    df_final = (
        df_computed.select(
            "senderpaid",
            "requestid",
            "requesttimestamp",
            "prodotto",
            "geokey",
            "affido_consolidatore_data",
            "stampa_imbustamento_con080_data",
            "materialita_pronta_con09a_data",
            "affido_recapitista_con016_data",
            "accettazione_recapitista_con018_data",
            "Cluster",
            "Priority",
            "differenza_gg_lavorativi",
            "gg_fuori_sla",
        )
        .dropDuplicates()
        .orderBy(
            F.col("Priority").desc(),
            F.col("gg_fuori_sla").desc(),
            F.col("requesttimestamp").asc(),
        )
    )

    return df_final


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
    colonne_output = 0
    celle_output = 0
    collapse_applied = "NO"
    collapse_reason = "-"
    df_output_spark = None

    try:
        creds = load_google_credentials(GOOGLE_SECRET_PATH)
        max_date_str = get_last_update_date(spark)

        write_tab_meta(
            creds=creds,
            max_date_str=max_date_str,
            start_run_str=job_start_str,
            end_run_str="-",
            elapsed_min_str="IN CORSO",
            stato_run="IN CORSO",
            righe_output=0,
            colonne_output=0,
            celle_output=0,
            collapse_applied="NO",
            collapse_reason="-",
        )

        df_output_spark = build_wi7_query(spark).persist(StorageLevel.MEMORY_AND_DISK)

        df_output_pd, output_meta = prepare_output_for_gsheet(
            df_output_spark,
            delimiter=COLLAPSE_DELIMITER,
        )

        righe_output = int(output_meta["righe_output"])
        colonne_output = int(output_meta["colonne_output"])
        celle_output = int(output_meta["celle_output"])
        collapse_applied = output_meta["collapse_applied"]
        collapse_reason = output_meta["collapse_reason"]
        chunk_size_used = int(output_meta["chunk_size_used"])
        expected_written_cols = int(output_meta["expected_written_cols"])

        logging.info(
            f"Output pronto per export: righe={righe_output}, colonne={colonne_output}, "
            f"celle={celle_output}, collapse={collapse_applied}, "
            f"reason={collapse_reason}, chunk_size={chunk_size_used}"
        )

        export_to_sheets_chunked(
            df_output_pd,
            creds,
            SHEET_ID,
            TAB_DETAIL,
            chunk_size=chunk_size_used,
            expected_written_cols=expected_written_cols,
        )

        job_end_dt = now_utc_dt()
        job_end_str = job_end_dt.strftime("%Y-%m-%d %H:%M:%S")
        job_elapsed_min = elapsed_minutes_str(job_start_dt, job_end_dt)

        write_tab_meta(
            creds=creds,
            max_date_str=max_date_str,
            start_run_str=job_start_str,
            end_run_str=job_end_str,
            elapsed_min_str=job_elapsed_min,
            stato_run="OK",
            righe_output=righe_output,
            colonne_output=colonne_output,
            celle_output=celle_output,
            collapse_applied=collapse_applied,
            collapse_reason=collapse_reason,
        )

        if collapse_applied == "SI":
            slack_msg = (
                f"{slack_prefix}\n"
                f"⚠️⚠️⚠️ *SUCCESS WITH COLLAPSE* ⚠️⚠️⚠️\n"
                f"*Job:* {APP_NAME}\n"
                f"*RunId (UTC):* {run_id}\n"
                f"*Host:* {hostname}\n"
                f"*Righe output scritte:* {righe_output}\n"
                f"*Colonne output scritte:* {colonne_output}\n"
                f"*Celle output:* {celle_output}\n"
                f"*Collapse applied:* {collapse_applied}\n"
                f"*Collapse reason:* {collapse_reason}\n"
                f"*Ultimo aggiornamento dati:* {max_date_str}\n"
            )
        else:
            slack_msg = (
                f"{slack_prefix}\n"
                f"✅✅✅ *SUCCESS* ✅✅✅\n"
                f"*Job:* {APP_NAME}\n"
                f"*RunId (UTC):* {run_id}\n"
                f"*Host:* {hostname}\n"
                f"*Righe output scritte:* {righe_output}\n"
                f"*Colonne output scritte:* {colonne_output}\n"
                f"*Celle output:* {celle_output}\n"
                f"*Collapse applied:* {collapse_applied}\n"
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
            write_tab_meta(
                creds=creds,
                max_date_str=max_date_str,
                start_run_str=job_start_str,
                end_run_str=job_end_str,
                elapsed_min_str=job_elapsed_min,
                stato_run="KO",
                righe_output=righe_output,
                colonne_output=colonne_output,
                celle_output=celle_output,
                collapse_applied=collapse_applied,
                collapse_reason=collapse_reason,
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
        try:
            if df_output_spark is not None:
                df_output_spark.unpersist()
                logging.info("df_output_spark rilasciato dalla cache.")
        except Exception:
            logging.warning("unpersist fallito per df_output_spark.")

        try:
            spark.stop()
        except Exception:
            logging.warning("spark.stop() fallito in chiusura.")


if __name__ == "__main__":
    main()
