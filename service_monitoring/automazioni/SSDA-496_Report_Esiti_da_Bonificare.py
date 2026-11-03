import json
import logging
import os
import socket
import time
import urllib.request
from datetime import datetime, timezone

import gspread
import numpy as np
import pandas as pd
from google.oauth2 import service_account
from gspread.exceptions import APIError
from pdnd_google_utils import Sheet
from pyspark import StorageLevel
from pyspark.sql import SparkSession

# ---------------- CONFIGURAZIONE BASE ----------------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

APP_NAME = "SSDA-496 Report Esiti da Bonificare"

GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"

# Versione L3
SHEET_ID_L3 = "1s83mCJAC3Yxye2pBlNaPPEFUkHC8U4eCuhNdZxe1T0I"
# Versione General
SHEET_ID_L4 = "1B_8YkdtruSqCtkrukjHtAqPUnrelNrGySYmhv4t5Myc"

# Versione Recapitisti
SHEET_ID_Fulmine = "1qeNYn3al9iFVlJ7rlR_zO9NXLld313rsx3aFlHL0ekA"
SHEET_ID_Poste = "1idDmew64tfxfI7-KdI4BrYwD-xWm5PxINTsY6rWQoQs"
SHEET_ID_Sailpost = "1JSIum1WhNXJ1CZ-TAuvs8GbeDqeITNqLduVzeLUUInI"
SHEET_ID_Po_Se = "1FDVU_LyK_Ch9DJBsOni0k-4mEKeH7pREc3UiUkSclo8"

TABLE_OUTPUT_REPORT_BONIFICA = "send_dev.report_bonifica_esiti"

TAB_META = "Aggiornamento Job"

SLACK_CONFIG_CANDIDATE_PATHS = [
    "/Slack/notifications_webhook.txt",
    "/app/mount/Slack/notifications_webhook.txt",
    "/app/mount/notifications_webhook.txt",
]
SLACK_WEBHOOK_NAME = "webhook_1"
SLACK_LABEL = "SSDA-496 Report Esiti da Bonificare"

SHEETS_CHUNK_SIZE = 15000
SHEETS_CELL_LIMIT = 10_000_000
INITIAL_WS_ROWS = 2

GSHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]


# ---------------- ERRORI DEDICATI ----------------
class CapacityLimitError(RuntimeError):
    """Errore fatale: l'export non è rappresentabile in Google Sheets entro i limiti strutturali."""


def create_sheet_with_retry(
    sheet_id: str,
    creds: dict,
    max_attempts: int = 5,
):
    """
    Crea l'oggetto Sheet ritentando esclusivamente in caso di rate limit
    Google Sheets (HTTP 429). Gli altri errori vengono propagati subito.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return Sheet(
                sheet_id=sheet_id,
                service_credentials=creds,
                id_mode="key",
            )

        except APIError as e:
            status_code = getattr(
                getattr(e, "response", None),
                "status_code",
                None,
            )

            if status_code != 429:
                raise

            if attempt == max_attempts:
                logging.error(
                    f"Google Sheets rate limit ancora presente dopo "
                    f"{max_attempts} tentativi per sheet_id={sheet_id}."
                )
                raise

            wait_seconds = min(15 * (2 ** (attempt - 1)), 60)

            logging.warning(
                f"Google Sheets rate limit 429 durante apertura sheet_id={sheet_id}. "
                f"Tentativo {attempt}/{max_attempts}. "
                f"Nuovo tentativo tra {wait_seconds} secondi."
            )

            time.sleep(wait_seconds)


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
    """Legge i file di credenziali Google e li restituisce come dizionario."""
    creds = {}
    for name in os.listdir(secret_path):
        file_path = os.path.join(secret_path, name)
        if os.path.isfile(file_path):
            with open(file_path, "r") as f:
                creds[name] = f.read().strip()

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
    - resize iniziale solo per i tab dati, per ridurre la vecchia griglia
    - nessun resize finale: l'upload estende già la griglia quanto necessario
    - nessun resize per TAB_META, che ha struttura fissa e viene sovrascritto integralmente
    """
    logging.info(f"Scrittura su Google Sheet (chunked): {sheet_name}")

    if df is None:
        df = pd.DataFrame()

    total_rows = len(df)
    written_cols = (
        int(expected_written_cols)
        if expected_written_cols is not None
        else int(len(df.columns))
    )
    final_rows = max(INITIAL_WS_ROWS, total_rows + 1)  # +1 per header
    final_cols = max(1, written_cols)

    would_write = final_rows * final_cols
    if would_write >= SHEETS_CELL_LIMIT:
        raise CapacityLimitError(
            f"Export '{sheet_name}' su sheet_id={sheet_id}: scrittura prevista "
            f"{would_write} celle (rows={final_rows} * cols={final_cols}) "
            f">= {SHEETS_CELL_LIMIT}."
        )

    # Per i tab dati riduco prima la vecchia griglia: serve a rimuovere righe
    # residue da esecuzioni precedenti e a contenere le celle allocate.
    # TAB_META viene invece sempre riscritto integralmente ed ha struttura fissa
    # (2 righe x 5 colonne), quindi il resize aggiungerebbe solo chiamate API.
    if sheet_name != TAB_META:
        _resize_worksheet_grid_best_effort(
            creds=creds,
            sheet_id=sheet_id,
            worksheet_name=sheet_name,
            target_rows=INITIAL_WS_ROWS,
            target_cols=final_cols,
        )

    sheet = create_sheet_with_retry(
        sheet_id=sheet_id,
        creds=creds,
    )

    logging.info(f"Totale righe da caricare: {total_rows} (chunk_size={chunk_size})")

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

    # Nessun resize finale: sheet.upload() estende automaticamente la griglia
    # fino alle righe/colonne scritte. Evitiamo quindi una lettura metadata
    # aggiuntiva che, nel run osservato, risultava sistematicamente ridondante.
    logging.info("Scrittura completata (chunked).")


def get_last_update_date(spark: SparkSession) -> str:
    """Estrae la data più recente (MAX(requesttimestamp)) dal dataset."""
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
) -> pd.DataFrame:
    return (
        pd.DataFrame(
            {
                "Ultimo aggiornamento dati (MAX requesttimestamp)": [max_date_str],
                "Start run time (UTC)": [start_run_str],
                "End run time (UTC)": [end_run_str],
                "Tempo impiegato (min)": [elapsed_min_str],
                "Stato run": [stato_run],
            }
        )
        .replace([np.inf, -np.inf], np.nan)
        .fillna("-")
        .astype(str)
    )


def write_tab_meta(
    creds: dict,
    sheet_id: str,
    max_date_str: str,
    start_run_str: str,
    end_run_str: str = "-",
    elapsed_min_str: str = "IN CORSO",
    stato_run: str = "IN CORSO",
):
    update_df = build_meta_df(
        max_date_str=max_date_str,
        start_run_str=start_run_str,
        end_run_str=end_run_str,
        elapsed_min_str=elapsed_min_str,
        stato_run=stato_run,
    )

    export_to_sheets_chunked(
        update_df,
        creds,
        sheet_id,
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


# ---------------- QUERY ----------------
def build_base_query() -> str:
    """Costruisce la query base."""
    return """
        -- =====================================================================
        -- NOTE DI MANUTENZIONE / OTTIMIZZAZIONE
        --
        -- 1) RS e RIS sono temporaneamente esclusi dal perimetro.
        --    Per ripristinarli:
        --      - rimuovere il filtro prodotto NOT IN ('RS','RIS') in gold_perimetro;
        --      - decommentare gli status RECRS* e RECRSI* nella CTE base.
        --    Le logiche downstream RS/RIS restano intenzionalmente presenti.
        --
        -- 2) Il perimetro GOLD viene costruito subito con i filtri che sarebbero
        --    comunque applicati a valle (ultima postalizzazione, non cancellata,
        --    stato valido, fine recapito presente, ecc.).
        --
        -- 3) Le letture di silver_postalizzazione vengono filtrate direttamente
        --    sui requestid del perimetro GOLD e gli array vengono esplosi subito
        --    nella stessa CTE: nessun array viene trascinato nelle CTE successive.
        --
        -- 4) L'assenza di SEND_ANALOG_FEEDBACK viene verificata sulla singola
        --    postalizzazione tramite IUN + RECINDEX + ATTEMPT.
        --
        -- 5) SCHEDULE_REFINEMENT viene mantenuto esclusivamente come informazione
        --    diagnostica e ridotto a un solo record per IUN prima della join.
        --
        -- 6) Le tabelle notification vengono limitate agli IUN del perimetro e
        --    alle sole colonne utilizzate. Non vengono deduplicate forzatamente,
        --    per non modificare la semantica qualora esistano più record diversi.
        -- =====================================================================
        WITH gold_perimetro AS (
            SELECT
                s.*,
                -- Recapitista normalizzato calcolato una sola volta sul perimetro GOLD.
                CASE
                    WHEN s.codice_oggetto LIKE 'R14%' THEN 'Fulmine'
                    WHEN s.codice_oggetto LIKE '777%'
                      OR s.codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
                    WHEN s.codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
                    WHEN s.codice_oggetto LIKE '69%'
                      OR s.codice_oggetto LIKE '381%'
                      OR s.codice_oggetto LIKE 'RB1%' THEN 'Poste'
                    ELSE s.recapitista_unificato
                END AS recapitista_unif,
                -- Famiglia DEMAT da controllare in base alla rendicontazione più recente.
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
                END AS demat_documenttype_da_controllare
            FROM send.gold_postalizzazione_analytics s
            WHERE 1=1
                -- Perimetro GOLD anticipato: queste condizioni erano già parte
                -- del risultato finale; applicarle qui evita join, explode e controlli
                -- su righe che verrebbero comunque scartate a valle.
                AND s.prodotto NOT IN ('RS', 'RIS')
                AND s.scarto_consolidatore_stato IS NULL
                AND s.fine_recapito_stato IS NOT NULL
                AND s.statusrequest NOT IN (
                    'PN999', 'PN998', 'error', 'internalError', 'syntaxError',
                    'transformationError', 'semanticError', 'authenticationError',
                    'duplicatedRequest'
                )
                AND s.attempt_rank = 1
                AND s.pcretry_rank = 1
                AND s.tms_cancelled IS NULL
            ),
            temp_demat AS (
                SELECT
                    REGEXP_REPLACE(sp.requestid, '^pn-cons-000~', '') AS requestid,
                    att.documenttype AS documenttype,
                    ROW_NUMBER() OVER (
                        PARTITION BY REGEXP_REPLACE(sp.requestid, '^pn-cons-000~', '')
                        ORDER BY e.paperprogrstatus.clientrequesttimestamp DESC
                    ) AS rn_demat
                FROM send.silver_postalizzazione sp
                INNER JOIN gold_perimetro p
                    ON REGEXP_REPLACE(sp.requestid, '^pn-cons-000~', '') = p.requestid

                -- L'array eventslist viene esploso immediatamente dopo il filtro
                -- sul perimetro, senza essere trasportato in CTE intermedie.
                LATERAL VIEW explode(sp.eventslist) ev AS e
                LATERAL VIEW explode(e.paperprogrstatus.attachments) atts AS att

                WHERE att.documenttype IN ('23L', 'AR')
            ),
            base AS (
                SELECT
                    REGEXP_REPLACE (t.requestid, '^pn-cons-000~', '') AS requestid,
                    e.paperprogrstatus.statuscode AS statuscode,
                    CASE
                        WHEN LENGTH (e.paperprogrstatus.statusdatetime) = 17 THEN CONCAT (
                            SUBSTR (e.paperprogrstatus.statusdatetime, 1, 16),
                            ':00Z'
                        )
                        ELSE e.paperprogrstatus.statusdatetime
                    END AS statusdatetime,
                    CASE
                        WHEN LENGTH (e.paperprogrstatus.clientrequesttimestamp) = 17 THEN CONCAT (
                            SUBSTR (e.paperprogrstatus.clientrequesttimestamp, 1, 16),
                            ':00.000Z'
                        )
                        ELSE e.paperprogrstatus.clientrequesttimestamp
                    END AS clientrequesttimestamp,
                    CASE
                        WHEN e.paperprogrstatus.statuscode IN (
                            -- RS temporaneamente escluso. Per ripristinarlo decommentare:
                            -- 'RECRS001C','RECRS002C','RECRS002F','RECRS003C','RECRS004C','RECRS005C',
                            'RECRN001C','RECRN002C','RECRN002F','RECRN003C','RECRN004C','RECRN005C',
                            'RECAG001C','RECAG002C','RECAG003C','RECAG003F','RECAG005C','RECAG006C','RECAG007C','RECAG008C',
                            -- RIS temporaneamente escluso. Per ripristinarlo decommentare:
                            -- 'RECRSI003C','RECRSI004C',
                            'RECRI003C','RECRI004C'
                        ) THEN 'FINE_RECAPITO'
                        WHEN e.paperprogrstatus.statuscode IN (
                            -- RS temporaneamente escluso. Per ripristinarlo decommentare:
                            -- 'RECRS001C','RECRS002A','RECRS002D','RECRS003C','RECRS004A','RECRS005A',
                            'RECRN001A','RECRN002A','RECRN002D','RECRN003A','RECRN004A','RECRN005A',
                            'RECAG001A','RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A','RECAG007A','RECAG008A',
                            -- RIS temporaneamente escluso. Per ripristinarlo decommentare:
                            -- 'RECRSI003C','RECRSI004A',
                            'RECRI003A','RECRI004A'
                        ) THEN 'CERTIFICAZIONE_RECAPITO'
                        -- Eventi DEMAT B/E utilizzati per il controllo della tripletta e delle date business.
                        WHEN e.paperprogrstatus.statuscode IN (
                            -- RS temporaneamente escluso. Per ripristinarlo decommentare:
                            -- 'RECRS002B', 'RECRS002E', 'RECRS004B', 'RECRS005B',
                            'RECRN001B', 'RECRN002B', 'RECRN002E', 'RECRN003B', 'RECRN004B', 'RECRN005B',
                            'RECAG001B', 'RECAG002B', 'RECAG003B', 'RECAG003E', 'RECAG005B', 'RECAG006B',
                            'RECAG007B', 'RECAG008B',
                            -- RIS temporaneamente escluso. Per ripristinarlo decommentare:
                            -- 'RECRSI004B',
                            'RECRI003B', 'RECRI004B'
                        ) THEN 'DEMAT_RECAPITO'
                        ELSE NULL
                    END AS tipo
                FROM send.silver_postalizzazione t
                INNER JOIN gold_perimetro p
                    ON REGEXP_REPLACE(t.requestid, '^pn-cons-000~', '') = p.requestid
                LATERAL VIEW EXPLODE(t.eventslist) AS e
                WHERE e.paperprogrstatus.statuscode IN (
                    -- RS temporaneamente escluso. Per ripristinarlo decommentare:
                    -- 'RECRS002A','RECRS002D','RECRS004A','RECRS005A',
                    'RECRN001A','RECRN002A','RECRN002D','RECRN003A','RECRN004A','RECRN005A',
                    'RECAG001A','RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A','RECAG007A','RECAG008A',
                    -- RIS temporaneamente escluso. Per ripristinarlo decommentare:
                    -- 'RECRSI004A',
                    'RECRI003A','RECRI004A',

                    -- RS temporaneamente escluso. Per ripristinarlo decommentare:
                    -- 'RECRS001C','RECRS002C','RECRS002F','RECRS003C','RECRS004C','RECRS005C',
                    'RECRN001C','RECRN002C','RECRN002F','RECRN003C','RECRN004C','RECRN005C',
                    'RECAG001C','RECAG002C','RECAG003C','RECAG003F','RECAG005C','RECAG006C','RECAG007C','RECAG008C',
                    -- RIS temporaneamente escluso. Per ripristinarlo decommentare:
                    -- 'RECRSI003C','RECRSI004C',
                    'RECRI003C','RECRI004C',

                    -- RS temporaneamente escluso. Per ripristinarlo decommentare:
                    -- 'RECRS002B', 'RECRS002E', 'RECRS004B', 'RECRS005B',
                    'RECRN001B', 'RECRN002B', 'RECRN002E', 'RECRN003B', 'RECRN004B', 'RECRN005B',
                    'RECAG001B', 'RECAG002B', 'RECAG003B', 'RECAG003E', 'RECAG005B', 'RECAG006B',
                    'RECAG007B', 'RECAG008B',
                    -- RIS temporaneamente escluso. Per ripristinarlo decommentare:
                    -- 'RECRSI004B',
                    'RECRI003B', 'RECRI004B'
                ) AND e.paperprogrstatus.statuscode NOT IN ('PN999','PN998')
            ),
            vista_silver_postalizzazione AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY requestid, tipo
                        ORDER BY clientrequesttimestamp DESC
                    ) AS rn
                FROM base
            ),
            max_events_silver_postalizzazione AS (
                SELECT
                    requestid,
                    MAX(CASE WHEN tipo = 'FINE_RECAPITO' THEN statuscode END) AS fine_recapito_stato,
                    MAX(CASE WHEN tipo = 'FINE_RECAPITO' THEN statusdatetime END) AS fine_recapito_data,
                    MAX(CASE WHEN tipo = 'FINE_RECAPITO' THEN clientrequesttimestamp END) AS fine_recapito_rendicontazione,
                    MAX(CASE WHEN tipo = 'CERTIFICAZIONE_RECAPITO' THEN statuscode END) AS certificazione_recapito_stato,
                    MAX(CASE WHEN tipo = 'CERTIFICAZIONE_RECAPITO' THEN statusdatetime END) AS certificazione_recapito_data,
                    MAX(CASE WHEN tipo = 'CERTIFICAZIONE_RECAPITO' THEN clientrequesttimestamp END) AS certificazione_recapito_rendicontazione,
                    MAX(CASE WHEN tipo = 'DEMAT_RECAPITO' THEN statuscode END) AS demat_recapito_stato,
                    MAX(CASE WHEN tipo = 'DEMAT_RECAPITO' THEN statusdatetime END) AS demat_recapito_data,
                    MAX(CASE WHEN tipo = 'DEMAT_RECAPITO' THEN clientrequesttimestamp END) AS demat_recapito_rendicontazione
                FROM vista_silver_postalizzazione
                WHERE rn = 1
                GROUP BY requestid
            ),
            send_analog_feedback AS (
                SELECT
                    tl.iun,
                    CAST(REGEXP_EXTRACT(tl.timelineelementid, 'RECINDEX_([0-9]+)', 1) AS INT) AS recindex_number,
                    CAST(REGEXP_EXTRACT(tl.timelineelementid, 'ATTEMPT_([0-9]+)', 1) AS INT) AS attempt_number
                FROM send.silver_timeline tl
                INNER JOIN gold_perimetro p
                    ON tl.iun = p.iun
                WHERE tl.category = 'SEND_ANALOG_FEEDBACK'
            ),

            -- Serve solo sapere se esiste almeno uno SCHEDULE_REFINEMENT per IUN.
            -- Il DISTINCT evita che più eventi timeline moltiplichino la riga GOLD.
            schedule_refinement AS (
                SELECT DISTINCT
                    tl.iun
                FROM send.silver_timeline tl
                INNER JOIN gold_perimetro p
                    ON tl.iun = p.iun
                WHERE tl.category = 'SCHEDULE_REFINEMENT'
            ),

            -- Limito silver_notification agli IUN realmente presenti nel perimetro
            -- e porto avanti solo le colonne effettivamente utilizzate.
            silver_notification_perimetro AS (
                SELECT
                    sn.iun,
                    sn.senderdenomination
                FROM send.silver_notification sn
                INNER JOIN gold_perimetro p
                    ON sn.iun = p.iun
            ),

            -- Stesso principio per gold_notification_analytics:
            -- solo IUN utili e sole colonne necessarie alla query.
            gold_notification_perimetro AS (
                SELECT
                    n.iun,
                    n.tms_date_payment,
                    n.type_notif
                FROM send.gold_notification_analytics n
                INNER JOIN gold_perimetro p
                    ON n.iun = p.iun
            ),

            temp_postalizzazione_e_controlli AS (
                SELECT
                    s.senderpaid,
                    sn.senderdenomination,
                    s.iun,
                    s.requestid,
                    s.requesttimestamp,
                    s.prodotto,
                    s.geokey,
                    s.recapitista_unif,
                    s.lotto,
                    s.codice_oggetto,
                    CONCAT ("'", s.codice_oggetto) AS codiceOggetto,
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
                    CASE
                        WHEN sr.iun IS NOT NULL THEN 1
                        ELSE 0
                    END AS flag_schedule_refinement,
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
                    t.demat_recapito_stato AS demat_recapito_stato_silver,
                    t.demat_recapito_data AS demat_recapito_data_silver,
                    t.demat_recapito_rendicontazione AS demat_recapito_rendicontazione_silver,
                    d.documenttype,
                    CASE
                        WHEN n.type_notif = 'MULTI' THEN 1
                        ELSE 0
                    END AS flag_multi_destinatario,
                    CASE
                        WHEN s.attempt_rank = 1
                        AND s.pcretry_rank = 1 THEN 1
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
                        -- RECRS001C / RECRS003C / RECRSI003C sono fascicoli chiusi senza A/B precedenti.
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
                        -- Per RECRS001C / RECRS003C / RECRSI003C è corretto avere la sola data di fine recapito.
                        WHEN s.fine_recapito_stato IN ('RECRS001C','RECRS003C', 'RECRSI003C') THEN 0
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
                                'RECRS002B','RECRS002E','RECRS004B','RECRS005B',
                                'RECRN002B','RECRN002E','RECRN004B','RECRN005B',
                                'RECAG003B','RECAG003E','RECAG007B','RECAG008B','RECAG011B',
                                'RECRI004B','RECRSI004B'
                            )
                        ) THEN 1
                        WHEN s.prodotto = '890' AND d.documenttype = '23L' THEN 0
                        WHEN s.prodotto IN ('AR', 'RIR')  AND d.documenttype = 'AR'  THEN 0
                        WHEN d.documenttype IS NULL  THEN 0
                        ELSE 1
                    END AS controllo_documentType
                FROM gold_perimetro s
                    LEFT JOIN temp_demat d ON d.requestid = s.requestid AND d.rn_demat = 1
                    LEFT JOIN silver_notification_perimetro sn ON sn.iun = s.iun
                    LEFT JOIN gold_notification_perimetro n ON n.iun = s.iun
                    -- Join 1:1 sul lato timeline: la CTE contiene un solo IUN.
                    LEFT JOIN schedule_refinement sr ON sr.iun = s.iun
                    LEFT JOIN max_events_silver_postalizzazione t ON s.requestid = t.requestid
                    LEFT ANTI JOIN send_analog_feedback saf
                        ON s.iun = saf.iun
                        AND CAST(s.recindex_number AS INT) = saf.recindex_number
                        AND CAST(s.attempt_number AS INT) = saf.attempt_number
            ),
            temp_postalizzazione AS (
                SELECT
                    s.*,
                    IF(
                        fine_recapito_stato IN (
                            'RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C',
                            'RECAG005C','RECAG006C','RECAG007C','RECAG008C'
                        )
                        AND tentativo_recapito_stato IS NULL,
                        1,
                        0
                    ) AS assenza_inesito,
                    IF(
                        fine_recapito_stato IN (
                            'RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C',
                            'RECAG005C','RECAG006C','RECAG007C','RECAG008C'
                        )
                        AND messaingiacenza_recapito_stato IS NULL,
                        1,
                        0
                    ) AS assenza_messa_in_giacenza,
                    IF(
                        certificazione_recapito_stato IS NULL,
                        1,
                        0
                    ) AS assenza_pre_esito,
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
                        AND fine_recapito_stato IN (
                            'RECAG005C','RECAG006C','RECAG007C','RECAG008C'
                        )
                        AND demat_arcad_data_rendicontazione IS NULL,
                        1,
                        0
                    ) AS assenza_demat_arcad,
                    IF(
                        prodotto = '890'
                        AND fine_recapito_stato IN (
                            'RECAG005C','RECAG006C','RECAG007C','RECAG008C'
                        )
                        AND accettazione_23l_recag012_data IS NULL,
                        1,
                        0
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
                FROM temp_postalizzazione_e_controlli s
            ),
            temp_assenza_eventi AS (
                SELECT
                    *,
                    IF(
                        assenza_inesito = 1
                        OR assenza_messa_in_giacenza = 1
                        OR assenza_pre_esito = 1
                        OR assenza_dematerializzazione_23l_ar_plico = 1
                        OR assenza_demat_arcad = 1
                        OR assenza_RECAG012 = 1
                        OR assenza_avviato_estero = 1
                        OR assenza_ingresso_paese_estero = 1,
                        1,
                        0
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
                    t.tms_date_payment,
                    t.flag_schedule_refinement,
                    t.tms_cancelled,
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
                    t.demat_recapito_stato_silver,
                    t.demat_recapito_data_silver,
                    t.demat_recapito_rendicontazione_silver,
                    t.flag_multi_destinatario,
                    t.flag_ultima_postalizzazione,
                    t.flag_prodotto_estero,
                    t.flag_destinatario_deceduto,
                    t.flag_esiti_mancanti,
                    CASE
                        WHEN controllo_causale = 1
                        OR controllo_date_business = 1
                        OR controllo_tripletta = 1
                        OR controllo_tempistiche_compiuta_giacenza = 1
                        OR controllo_inesito_casi_giacenza = 1
                        OR controllo_documenttype = 1 THEN 1
                        ELSE 0
                    END AS flag_errore_rendicontazione,
                    CONCAT_WS(
                        ', ',
                        CASE WHEN controllo_documenttype = 1 THEN 'errore documenttype' END,
                        CASE WHEN controllo_causale = 1 THEN 'errore rend. causale' END,
                        CASE WHEN controllo_date_business = 1 THEN 'errore rend. date business' END,
                        CASE WHEN controllo_tripletta = 1 THEN 'errore rend. tripletta' END,
                        CASE WHEN controllo_tempistiche_compiuta_giacenza = 1 THEN 'errore rend. tempistiche compiuta giacenza' END,
                        CASE WHEN controllo_inesito_casi_giacenza = 1 THEN 'errore rend. inesito casi giacenza' END,
                        CASE WHEN assenza_inesito = 1 THEN 'assenza inesito' END,
                        CASE WHEN assenza_messa_in_giacenza = 1 THEN 'assenza messa in giacenza' END,
                        CASE WHEN assenza_pre_esito = 1 THEN 'assenza pre-esito' END,
                        CASE WHEN assenza_dematerializzazione_23l_ar_plico = 1 THEN 'assenza demat 23l_ar / plico' END,
                        CASE WHEN assenza_demat_arcad = 1 THEN 'assenza demat ARCAD' END,
                        CASE WHEN assenza_RECAG012 = 1 THEN 'assenza RECAG012' END,
                        CASE WHEN assenza_avviato_estero = 1 THEN 'assenza avviato estero' END,
                        CASE WHEN assenza_ingresso_paese_estero = 1 THEN 'assenza ingresso paese estero' END
                    ) AS causa_mancato_perfezionamento,
                    t.controllo_date_business,
                    t.controllo_tripletta,
                    t.controllo_tempistiche_compiuta_giacenza,
                    t.controllo_inesito_casi_giacenza,
                    t.controllo_documenttype,
                    t.assenza_inesito,
                    t.assenza_messa_in_giacenza,
                    t.assenza_pre_esito,
                    t.assenza_dematerializzazione_23l_ar_plico,
                    t.assenza_demat_arcad,
                    t.assenza_RECAG012,
                    t.assenza_avviato_estero,
                    t.assenza_ingresso_paese_estero
                FROM temp_assenza_eventi t
            ),
            finale_filtrato AS (
                SELECT *
                FROM finale f
                WHERE
                    flag_ultima_postalizzazione = 1
                    AND tms_cancelled IS NULL
                    AND (
                        certificazione_recapito_stato NOT IN (
                            'RECRS006','RECRS013','RECRN006','RECRN013','RECAG004','RECAG013'
                        )
                        OR certificazione_recapito_stato IS NULL
                    )
                    AND (
                        tentativo_recapito_stato NOT IN ('PN998', 'PN999')
                        OR certificazione_recapito_stato NOT IN ('PN998', 'PN999')
                        OR fine_recapito_stato NOT IN ('PN998', 'PN999')
                    )
                    AND (
                        flag_errore_rendicontazione = 1
                        OR flag_esiti_mancanti = 1
                    )
            )
        SELECT * FROM finale_filtrato
    """


def build_queries() -> dict:
    """Costruisce le versioni filtrate per ciascun recapitista."""
    base_view = "vw_finale_filtrato"

    query_general = f"""
    SELECT *
    FROM {base_view}
    """

    query_base_poste = f"""
    SELECT
        requestid,
        requesttimestamp,
        prodotto,
        geokey,
        recapitista_unif,
        lotto,
        codice_oggetto,
        codiceOggetto,
        affido_accettazione_rec_data,
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
        flag_esiti_mancanti,
        flag_errore_rendicontazione,
        causa_mancato_perfezionamento
    FROM {base_view}
    """

    query_base_altri_recapitisti = f"""
    SELECT
        requestid,
        requesttimestamp,
        prodotto,
        geokey,
        recapitista_unif,
        lotto,
        codice_oggetto,
        codiceOggetto,
        affido_accettazione_rec_data,
        flag_esiti_mancanti,
        flag_errore_rendicontazione,
        causa_mancato_perfezionamento
    FROM {base_view}
    """

    query_fulmine = query_base_altri_recapitisti + """
    WHERE recapitista_unif = 'Fulmine'
    """

    query_poste = query_base_poste + """
    WHERE recapitista_unif = 'Poste'
    """

    query_post_service = query_base_altri_recapitisti + """
    WHERE recapitista_unif = 'POST & SERVICE'
    """

    query_sailpost = query_base_altri_recapitisti + """
    WHERE recapitista_unif = 'RTI Sailpost-Snem'
    """

    query_l3 = f"""
    SELECT
        senderpaid,
        senderdenomination,
        iun,
        requestid,
        requesttimestamp,
        prodotto,
        geokey,
        recapitista_unif,
        lotto,
        codice_oggetto,
        codiceOggetto,
        affido_accettazione_rec_data,
        tms_date_payment,
        flag_esiti_mancanti,
        flag_errore_rendicontazione,
        causa_mancato_perfezionamento
    FROM {base_view}
    """

    return {
        "Poste": query_poste,
        "Fulmine": query_fulmine,
        "POST & SERVICE": query_post_service,
        "RTI Sailpost-Snem": query_sailpost,
        "L3": query_l3,
        "general": query_general,
    }


def overwrite_iceberg_table_from_df(df_spark, target_table: str) -> None:
    """
    Sovrascrive la tabella Iceberg target a partire da un DataFrame Spark
    già materializzato, senza rieseguire la query.
    """
    logging.info(f"Inizio scrittura su tabella Iceberg: {target_table}")
    logging.info(f"Colonne dataset da scrivere: {df_spark.columns}")

    df_spark.createOrReplaceTempView("DF_OUTPUT_REPORT_BONIFICA")

    logging.info(
        f"Sovrascrittura della tabella {target_table} tramite createOrReplace..."
    )
    df_spark.sparkSession.sql("""SELECT * FROM DF_OUTPUT_REPORT_BONIFICA""").writeTo(
        target_table
    ).using("iceberg").tableProperty("format-version", "2").tableProperty(
        "engine.hive.enabled", "true"
    ).createOrReplace()

    final_count = df_spark.sparkSession.table(target_table).count()
    logging.info(
        f"Scrittura completata su {target_table}. "
        f"Righe presenti dopo la sovrascrittura: {final_count}"
    )


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
    base_df = None

    SHEET_MAP = {
        "Poste": {
            "sheet_id": SHEET_ID_Poste,
            "sheet_name": "Esiti da Bonificare - Poste",
        },
        "Fulmine": {
            "sheet_id": SHEET_ID_Fulmine,
            "sheet_name": "Esiti da Bonificare - Fulmine",
        },
        "POST & SERVICE": {
            "sheet_id": SHEET_ID_Po_Se,
            "sheet_name": "Esiti da Bonificare - Post & Service",
        },
        "RTI Sailpost-Snem": {
            "sheet_id": SHEET_ID_Sailpost,
            "sheet_name": "Esiti da Bonificare - Sailpost-Snem",
        },
        "L3": {
            "sheet_id": SHEET_ID_L3,
            "sheet_name": "Estrazione Esiti da Bonificare",
        },
        "general": {
            "sheet_id": SHEET_ID_L4,
            "sheet_name": "Estrazione Esiti da Bonificare",
        },
    }

    try:
        spark_conf = spark.sparkContext.getConf().getAll()
        for k, v in spark_conf:
            logging.info(f"{k}: {v}")

        creds = load_google_credentials(GOOGLE_SECRET_PATH)

        logging.info("Costruzione base comune finale_filtrato...")
        base_query = build_base_query()

        base_df = spark.sql(base_query).persist(StorageLevel.MEMORY_AND_DISK)

        logging.info(
            "Materializzazione persist della base comune condivisa tra tutte le query..."
        )
        base_count = base_df.count()
        logging.info(f"Base comune materializzata con {base_count} righe.")

        base_df.createOrReplaceTempView("vw_finale_filtrato")

        queries = build_queries()

        max_date_str = get_last_update_date(spark)

        write_tab_meta(
            creds=creds,
            sheet_id=SHEET_ID_L4,
            max_date_str=max_date_str,
            start_run_str=job_start_str,
            end_run_str="-",
            elapsed_min_str="IN CORSO",
            stato_run="IN CORSO",
        )

        for nome_query, query_sql in queries.items():
            try:
                logging.info(f"Esecuzione query per: {nome_query}")

                # Persist temporaneo della singola query di output:
                # evita di ricalcolare filtro/proiezione/DISTINCT tra count() e toPandas().
                df_spark = spark.sql(query_sql).persist(StorageLevel.MEMORY_AND_DISK)

                try:
                    row_count = df_spark.count()
                    logging.info(f"Query {nome_query} eseguita con {row_count} righe.")

                    if row_count == 0:
                        logging.warning(f"Nessun dato trovato per {nome_query}. Salto.")
                        continue

                    if nome_query == "general":
                        logging.info(
                            f"Avvio sovrascrittura tabella Iceberg {TABLE_OUTPUT_REPORT_BONIFICA} "
                            f"con i risultati dell'estrazione '{nome_query}'."
                        )
                        overwrite_iceberg_table_from_df(
                            df_spark=df_spark,
                            target_table=TABLE_OUTPUT_REPORT_BONIFICA,
                        )
                        logging.info(
                            f"Sovrascrittura completata per {TABLE_OUTPUT_REPORT_BONIFICA}."
                        )

                    if nome_query not in SHEET_MAP:
                        logging.error(
                            f"Nessun mapping sheet per '{nome_query}'. Skipping."
                        )
                        continue

                    logging.info(
                        f"Trasformazione in Pandas DataFrame per export sheet: {nome_query}"
                    )
                    df = spark_to_df_per_gsheet(df_spark)

                    sheet_id_target = SHEET_MAP[nome_query]["sheet_id"]
                    sheet_name = SHEET_MAP[nome_query]["sheet_name"]

                    logging.info(f"Scrittura del dataset su sheet '{sheet_name}'...")
                    export_to_sheets_chunked(
                        df,
                        creds,
                        sheet_id_target,
                        sheet_name,
                        chunk_size=SHEETS_CHUNK_SIZE,
                        expected_written_cols=len(df.columns),
                    )

                    logging.info(
                        f"Aggiornamento tab Data Aggiornamento per {nome_query}..."
                    )
                    write_tab_meta(
                        creds=creds,
                        sheet_id=sheet_id_target,
                        max_date_str=max_date_str,
                        start_run_str=job_start_str,
                        end_run_str="-",
                        elapsed_min_str=elapsed_minutes_str(job_start_dt),
                        stato_run="IN CORSO",
                    )

                finally:
                    # Il risultato specifico non serve più dopo l'export.
                    df_spark.unpersist()
                    logging.info(f"df_spark rilasciato dalla cache per: {nome_query}")

            except Exception as e:
                logging.error(
                    f"Errore durante elaborazione di {nome_query}: {e}",
                    exc_info=True,
                )
                raise

        job_end_dt = now_utc_dt()
        job_end_str = job_end_dt.strftime("%Y-%m-%d %H:%M:%S")
        job_elapsed_min = elapsed_minutes_str(job_start_dt, job_end_dt)

        for sheet_cfg in SHEET_MAP.values():
            try:
                write_tab_meta(
                    creds=creds,
                    sheet_id=sheet_cfg["sheet_id"],
                    max_date_str=max_date_str,
                    start_run_str=job_start_str,
                    end_run_str=job_end_str,
                    elapsed_min_str=job_elapsed_min,
                    stato_run="OK",
                )
            except Exception:
                logging.warning(
                    f"Aggiornamento tab meta fallito per sheet_id={sheet_cfg['sheet_id']}"
                )

        success_msg = (
            f"{slack_prefix}\n"
            f"✅✅✅ *SUCCESS* ✅✅✅\n"
            f"*Job:* {APP_NAME}\n"
            f"*RunId (UTC):* {run_id}\n"
            f"*Host:* {hostname}\n"
            f"*Ultimo aggiornamento dati:* {max_date_str}\n"
        )

        logging.info("Job completato con successo.")
        send_slack(slack_webhook_url, success_msg)

    except Exception:
        logging.error("Errore durante l'esecuzione del job.", exc_info=True)

        try:
            job_end_dt = now_utc_dt()
            job_end_str = job_end_dt.strftime("%Y-%m-%d %H:%M:%S")
            job_elapsed_min = elapsed_minutes_str(job_start_dt, job_end_dt)

            creds = load_google_credentials(GOOGLE_SECRET_PATH)

            for sheet_cfg in SHEET_MAP.values():
                try:
                    write_tab_meta(
                        creds=creds,
                        sheet_id=sheet_cfg["sheet_id"],
                        max_date_str=max_date_str,
                        start_run_str=job_start_str,
                        end_run_str=job_end_str,
                        elapsed_min_str=job_elapsed_min,
                        stato_run="KO",
                    )
                except Exception:
                    logging.warning(
                        f"Aggiornamento tab meta fallito in errore per sheet_id={sheet_cfg['sheet_id']}"
                    )
        except Exception:
            logging.warning(
                "Aggiornamento dei tab meta fallito durante la gestione errore."
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
            if base_df is not None:
                base_df.unpersist()
                logging.info("base_df rilasciato dalla cache.")
        except Exception:
            logging.warning("unpersist fallito per base_df.")

        try:
            spark.stop()
        except Exception:
            logging.warning("spark.stop() fallito in chiusura.")


if __name__ == "__main__":
    main()
