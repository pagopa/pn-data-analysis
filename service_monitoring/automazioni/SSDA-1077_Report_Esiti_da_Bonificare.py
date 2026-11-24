import csv
import json
import logging
import math
import os
import random
import socket
import tempfile
import time
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import gspread
import numpy as np
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from gspread.exceptions import WorksheetNotFound
from pdnd_google_utils import Sheet
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from requests.exceptions import RequestException

# ---------------- CONFIGURAZIONE BASE ----------------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

APP_NAME = "SSDA-1077 Report Esiti da Bonificare"

GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"

# Versione L3
SHEET_ID_L3 = "1s83mCJAC3Yxye2pBlNaPPEFUkHC8U4eCuhNdZxe1T0I"
# Versione General
SHEET_ID_L4 = "1B_8YkdtruSqCtkrukjHtAqPUnrelNrGySYmhv4t5Myc"

# SSDA-1077: L3 e L4 su GSheet; i 4 recapitisti su CSV in cartella Drive.
DRIVE_FOLDER_ID = "1XijKY661Pb5gpWUmWs70czoYTef5Sz6I"
CSV_SEPARATOR = ";"  # Per cambiare il separatore di tutti i CSV.
CSV_MAX_DATA_ROWS = 800_000  # L'intestazione non conta nel limite.
CSV_PROGRESS_EVERY = 25_000  # Avanzamento durante lo streaming Spark sul driver.
LOCAL_TZ = ZoneInfo("Europe/Rome")
CSV_NAMES = {
    "Poste": "Poste Italiane",
    "Fulmine": "Fulmine",
    "POST & SERVICE": "Spike",  # Alias SOLO per i nomi dei CSV.
    "RTI Sailpost-Snem": "Sailpost",
}
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]
GOOGLE_MAX_ATTEMPTS = 7
GOOGLE_RETRY_BASE_SEC = 2.0
GOOGLE_RETRY_CAP_SEC = 60.0

TABLE_OUTPUT_REPORT_BONIFICA = "send_dev.report_bonifica_esiti"

TAB_META = "Log di controllo"
# Nel foglio L3 mostriamo solo le informazioni essenziali dell'ultima run.
# Il log L4 resta completo di conteggi, nomi e link CSV.
META_L3_COLUMNS = [
    "Ultimo aggiornamento dati (MAX requesttimestamp)",
    "Start run time (UTC)",
    "End run time (UTC)",
    "Tempo impiegato (min)",
    "Stato run",
]

SLACK_CONFIG_CANDIDATE_PATHS = [
    "/Slack/notifications_webhook.txt",
    "/app/mount/Slack/notifications_webhook.txt",
    "/app/mount/notifications_webhook.txt",
]
SLACK_WEBHOOK_NAME = "webhook_1"
SLACK_LABEL = "SSDA-1077 Report Esiti da Bonificare"

SHEETS_CHUNK_SIZE = 15000
SHEETS_CELL_LIMIT = 10_000_000
INITIAL_WS_ROWS = 2

GSHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]


# ---------------- ERRORI DEDICATI ----------------
class CapacityLimitError(RuntimeError):
    """Errore fatale: l'export non è rappresentabile in Google Sheets entro i limiti strutturali."""


def create_sheet_with_retry(sheet_id: str, creds: dict):
    """Apre lo Sheet; retry gestiti centralmente da google_retry."""
    return Sheet(sheet_id=sheet_id, service_credentials=creds, id_mode="key")


# ---------------- GOOGLE API: RETRY TRANSITORI ----------------
def _is_retryable_google_error(exc: Exception) -> bool:
    """Ritenta rate limit, errori temporanei server e problemi di rete; non auth/permessi."""
    status = getattr(getattr(exc, "resp", None), "status", None)
    if status is None:
        status = getattr(getattr(exc, "response", None), "status_code", None)
    if status in (408, 429, 500, 502, 503, 504):
        return True
    if status == 403:
        payload = str(exc).lower()
        return any(
            x in payload
            for x in (
                "ratelimitexceeded",
                "userratelimitexceeded",
                "quotaexceeded",
                "rate limit",
            )
        )
    return isinstance(exc, (TimeoutError, ConnectionError, OSError, RequestException))


def google_retry(operation, description: str):
    """Backoff esponenziale con jitter; nessun OK fittizio dopo retry esauriti."""
    for attempt in range(1, GOOGLE_MAX_ATTEMPTS + 1):
        try:
            return operation()
        except Exception as exc:
            if attempt == GOOGLE_MAX_ATTEMPTS or not _is_retryable_google_error(exc):
                logging.error(
                    "Google API %s: errore definitivo (tentativo %d): %s",
                    description,
                    attempt,
                    exc,
                )
                raise
            delay = min(
                GOOGLE_RETRY_CAP_SEC, GOOGLE_RETRY_BASE_SEC * (2 ** (attempt - 1))
            )
            delay *= 0.8 + 0.4 * random.random()
            logging.warning(
                "Google API %s: retry %d/%d tra %.1fs: %s",
                description,
                attempt,
                GOOGLE_MAX_ATTEMPTS,
                delay,
                exc,
            )
            time.sleep(delay)


@contextmanager
def log_phase(name: str):
    """Fase misurata: in caso di KO conserva nome, tempo ed eccezione."""
    tick = time.perf_counter()
    logging.info("[FASE START] %s", name)
    try:
        yield
    except Exception:
        logging.exception("[FASE KO] %s dopo %.1f s", name, time.perf_counter() - tick)
        raise
    else:
        logging.info("[FASE OK] %s in %.1f s", name, time.perf_counter() - tick)


def _drive_service(creds: dict):
    """Google Drive API v3; usare i nomi argomento della discovery API (camelCase)."""
    credentials = service_account.Credentials.from_service_account_info(
        creds, scopes=DRIVE_SCOPES
    )
    return build("drive", "v3", credentials=credentials, cache_discovery=False)


def check_drive_folder_access(creds: dict) -> None:
    """Verifica una sola volta che la cartella Drive sia visibile all'account del job.

    Non cerca CSV esistenti e non crea file. Un controllo di lettura non puo'
    garantire l'upload, ma intercetta in anticipo ID errati o permessi mancanti.
    """
    with log_phase("Verifica cartella destinazione Google Drive"):
        try:
            folder = google_retry(
                lambda: _drive_service(creds)
                .files()
                .get(
                    fileId=DRIVE_FOLDER_ID,
                    fields="id,name,mimeType,capabilities(canAddChildren)",
                    supportsAllDrives=True,
                )
                .execute(),
                "lettura cartella Drive",
            )
        except HttpError as exc:
            if exc.resp.status in (403, 404):
                raise RuntimeError(
                    "Cartella Google Drive non accessibile (HTTP "
                    f"{exc.resp.status}): verifica DRIVE_FOLDER_ID e i permessi "
                    "dell'account di servizio usato da CDE."
                ) from exc
            raise
        if folder.get("mimeType") != "application/vnd.google-apps.folder":
            raise RuntimeError(
                "DRIVE_FOLDER_ID non identifica una cartella Google Drive"
            )
        if folder.get("capabilities", {}).get("canAddChildren") is False:
            raise RuntimeError(
                "L'account di servizio legge la cartella Drive ma non ha "
                "il permesso di creare file al suo interno."
            )
        logging.info(
            "Cartella Drive accessibile: %s (id=%s)",
            folder.get("name", "-"),
            folder["id"],
        )


def upload_drive_csv(local_path: str, filename: str, creds: dict) -> dict:
    """Carica SEMPRE un nuovo CSV della run corrente nella cartella Drive.

    Non interroga i file gia' presenti, non aggiorna e non elimina CSV storici.
    La sessione resumable recupera automaticamente errori HTTP transitori
    durante i chunk. Non ritentiamo un'intera CREATE dopo un esito ambiguo,
    per non duplicare involontariamente un upload gia' completato.
    """
    with log_phase(f"Upload Drive {filename}"):
        service = google_retry(lambda: _drive_service(creds), "connessione Drive")
        size = os.path.getsize(local_path)
        logging.info("Drive CREATE nuovo CSV: %s, bytes=%d", filename, size)
        media = MediaFileUpload(
            local_path,
            mimetype="text/csv",
            resumable=True,
            chunksize=5 * 1024 * 1024,
        )
        request = service.files().create(
            body={
                "name": filename,
                "parents": [DRIVE_FOLDER_ID],
                "mimeType": "text/csv",
            },
            media_body=media,
            fields="id,name,webViewLink",
            supportsAllDrives=True,
        )
        response = None
        while response is None:
            status, response = request.next_chunk(num_retries=5)
            if status is not None:
                logging.info(
                    "Drive %s: upload %.1f%%", filename, 100 * status.progress()
                )
        if not response or not response.get("id"):
            raise RuntimeError(f"Drive non ha confermato ID file per {filename}")
        logging.info("Drive CREATE OK: %s, id=%s", filename, response["id"])
        return response


def export_csv_streaming(
    df_spark, row_count: int, label: str, local_date: str, creds: dict
):
    """Stream Spark -> CSV UTF-8 BOM, header per file e split max 800k righe.

    Nessuna toPandas/collect, un solo iteratore Spark per recapitista.
    Il temporary file viene sempre eliminato, anche su errore di scrittura.
    Il conteggio atteso serve anche come controllo di integrità.
    """
    if CSV_SEPARATOR not in (";", ",", "|", "\t"):
        raise ValueError(f"Separatore CSV non ammesso: {CSV_SEPARATOR!r}")
    if label not in CSV_NAMES:
        raise ValueError(f"Recapitista CSV sconosciuto: {label}")
    if row_count < 0:
        raise ValueError(f"Numero righe CSV negativo: {row_count}")
    fields = df_spark.columns
    if not fields:
        raise ValueError("CSV privo di colonne")
    if "requestid" not in fields:
        raise ValueError("CSV privo di requestid per l'ordinamento")

    num_parts = max(1, math.ceil(row_count / CSV_MAX_DATA_ROWS))
    logging.info(
        "CSV START recapitista=%s attese=%d colonne=%d parti=%d max_righe=%d",
        label,
        row_count,
        len(fields),
        num_parts,
        CSV_MAX_DATA_ROWS,
    )
    with log_phase(f"CSV completo {label}"):
        # Mantiene l'ordinamento storico: NON introdurre repartition/coalesce
        # o uno streaming non deterministico che cambierebbe l'ordine nei file.
        ordered = df_spark.orderBy(F.col("requestid"))
        row_iter = iter(ordered.toLocalIterator())
        csv_results = []
        emitted = 0
        for idx in range(1, num_parts + 1):
            part_rows = min(CSV_MAX_DATA_ROWS, row_count - emitted)
            name = f"Report Esiti da Bonificare_{CSV_NAMES[label]}_{local_date}"
            if num_parts > 1:
                name += f"_parte_{idx}_di_{num_parts}"
            name += ".csv"
            pathname = None
            written = 0
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    suffix=".csv",
                    prefix="ssda1077_",
                    encoding="utf-8-sig",
                    newline="",
                    delete=False,
                ) as tmp:
                    pathname = tmp.name
                    writer = csv.writer(
                        tmp, delimiter=CSV_SEPARATOR, quoting=csv.QUOTE_MINIMAL
                    )
                    writer.writerow(fields)
                    for _ in range(part_rows):
                        try:
                            record = next(row_iter)
                        except StopIteration as exc:
                            raise RuntimeError(
                                f"CSV {label}: Spark ha restituito meno righe "
                                f"del conteggio atteso {row_count}"
                            ) from exc
                        writer.writerow(["" if v is None else str(v) for v in record])
                        emitted += 1
                        written += 1
                        if emitted % CSV_PROGRESS_EVERY == 0:
                            logging.info(
                                "CSV %s avanzamento: %d/%d (%.1f%%)",
                                label,
                                emitted,
                                row_count,
                                100.0 * emitted / row_count if row_count else 100.0,
                            )
                logging.info(
                    "CSV locale completato: %s, righe=%d, dimensione=%d bytes",
                    name,
                    written,
                    os.path.getsize(pathname),
                )
                uploaded = upload_drive_csv(pathname, name, creds)
                csv_results.append(
                    {
                        "filename": name,
                        "file_id": uploaded["id"],
                        "url": uploaded.get("webViewLink")
                        or f"https://drive.google.com/file/d/{uploaded['id']}/view",
                        "rows": written,
                    }
                )
                logging.info("CSV parte %d/%d pubblicata: %s", idx, num_parts, name)
            finally:
                if pathname and os.path.exists(pathname):
                    os.unlink(pathname)

        if emitted != row_count:
            raise RuntimeError(
                f"CSV {label}: righe emesse={emitted}, attese={row_count}"
            )
        try:
            next(row_iter)
        except StopIteration:
            pass
        else:
            raise RuntimeError(
                f"CSV {label}: Spark restituisce righe aggiuntive rispetto al count"
            )
        logging.info(
            "CSV FINE recapitista=%s righe=%d file=%d", label, emitted, len(csv_results)
        )
        return csv_results


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
        google_retry(
            lambda: ws.resize(rows=final_rows, cols=final_cols),
            f"resize {worksheet_name}",
        )

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
    # (header + singola riga), quindi il resize aggiungerebbe chiamate API.
    if sheet_name != TAB_META:
        _resize_worksheet_grid_best_effort(
            creds=creds,
            sheet_id=sheet_id,
            worksheet_name=sheet_name,
            target_rows=INITIAL_WS_ROWS,
            target_cols=final_cols,
        )

    sheet = google_retry(
        lambda: create_sheet_with_retry(sheet_id=sheet_id, creds=creds),
        f"apertura Sheet {sheet_name}",
    )

    logging.info(f"Totale righe da caricare: {total_rows} (chunk_size={chunk_size})")

    if total_rows == 0:
        logging.info(f"Nessuna riga da caricare per '{sheet_name}'. Pulizia foglio.")
        google_retry(
            lambda: sheet.upload(
                worksheet_name=sheet_name,
                panda_df=df.astype(str),
                ul_cell="A1",
                header=True,
                clear_on_open=True,
            ),
            f"upload vuoto {sheet_name}",
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

        google_retry(
            lambda: sheet.upload(
                worksheet_name=sheet_name,
                panda_df=chunk.astype(str),
                ul_cell=ul_cell,
                header=header,
                clear_on_open=clear_on_open,
            ),
            f"upload {sheet_name} righe {start}:{end}",
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
    end_run_str: str,
    elapsed_min_str: str,
    stato_run: str,
    counts: dict,
    csv_files: dict,
) -> pd.DataFrame:
    """Una sola riga di monitoraggio su L4 per l'intero job."""
    row = {
        "Ultimo aggiornamento dati (MAX requesttimestamp)": max_date_str,
        "Start run time (UTC)": start_run_str,
        "End run time (UTC)": end_run_str,
        "Tempo impiegato (min)": elapsed_min_str,
        "Stato run": stato_run,
    }
    for rec in ("Poste", "Fulmine", "POST & SERVICE", "RTI Sailpost-Snem"):
        files = csv_files.get(rec, [])
        row[f"Righe {rec}"] = counts.get(rec, "N/D")
        row[f"File CSV {rec}"] = len(files)
        row[f"Link CSV {rec}"] = " | ".join(f["url"] for f in files)
    if all(isinstance(counts.get(rec), int) for rec in CSV_NAMES):
        row["Totale righe recapitisti"] = sum(counts[x] for x in CSV_NAMES)
    else:
        row["Totale righe recapitisti"] = "N/D"
    row["Righe non attribuite ai 4 recapitisti"] = counts.get("non_attribuite", "N/D")
    return pd.DataFrame([row]).fillna("-").astype(str)


def ensure_meta_l3_worksheet(creds: dict) -> None:
    """Crea il tab Log di controllo L3 soltanto quando non esiste."""
    credentials = service_account.Credentials.from_service_account_info(
        creds, scopes=GSHEETS_SCOPES
    )
    spreadsheet = gspread.authorize(credentials).open_by_key(SHEET_ID_L3)

    try:
        spreadsheet.worksheet(TAB_META)
    except WorksheetNotFound:
        spreadsheet.add_worksheet(
            title=TAB_META,
            rows=2,
            cols=len(META_L3_COLUMNS),
        )
        logging.info("Tab '%s' creato nello Sheet L3", TAB_META)


def write_tab_meta(
    creds: dict,
    max_date_str: str,
    start_run_str: str,
    end_run_str: str,
    elapsed_min_str: str,
    stato_run: str,
    counts: dict,
    csv_files: dict,
):
    """Aggiorna il monitoraggio a ogni transizione (IN CORSO / OK / KO).

    L4 e' il monitoraggio completo, con conteggi e link CSV.
    L3 contiene soltanto le cinque colonne operative richieste.
    Un eventuale problema del solo tab L3 viene segnalato, ma non annulla
    l'esecuzione: gli output e il log completo L4 restano prioritari.
    """
    result = build_meta_df(
        max_date_str,
        start_run_str,
        end_run_str,
        elapsed_min_str,
        stato_run,
        counts,
        csv_files,
    )

    # Registro principale già esistente: invariato.
    export_to_sheets_chunked(
        result, creds, SHEET_ID_L4, TAB_META, expected_written_cols=len(result.columns)
    )

    # Riepilogo sintetico nello Sheet L3, senza i dettagli per recapitista.
    try:
        ensure_meta_l3_worksheet(creds)
        l3_result = result.loc[:, META_L3_COLUMNS].copy()
        export_to_sheets_chunked(
            l3_result,
            creds,
            SHEET_ID_L3,
            TAB_META,
            expected_written_cols=len(META_L3_COLUMNS),
        )
        logging.info("Log di controllo L3 aggiornato: stato=%s", stato_run)
    except Exception:
        logging.warning(
            "Impossibile aggiornare il log sintetico L3; il log completo L4 "
            "rimane aggiornato e la run prosegue.",
            exc_info=True,
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
def build_gold_without_feedback_query() -> str:
    """Costruisce una sola volta il perimetro GOLD eleggibile senza feedback.

    L'anti-join usa la stessa tripletta e gli stessi CAST della query originale.
    Si esegue PRIMA di ogni arricchimento e controllo, senza cambiare le regole.
    """
    return """
        WITH gold_perimetro AS (
            SELECT
                -- Proiezione esplicita: 51 colonne anziche' 176 prima dello shuffle.
                -- Aggiungere qui eventuali nuovi campi usati dai controlli.
                s.accettazione_23l_recag012_data,
                s.accettazione_23l_recag012_data_rendicontazione,
                s.accettazione_recapitista_con018_data,
                s.affido_recapitista_con016_data,
                s.attempt_number,
                s.attempt_rank,
                s.certificazione_recapito_data,
                s.certificazione_recapito_data_rendicontazione,
                s.certificazione_recapito_dettagli,
                s.certificazione_recapito_stato,
                s.codice_oggetto,
                s.data_deposito,
                s.demat_23l_ar_data_rendicontazione,
                s.demat_23l_ar_stato,
                s.demat_arcad_data_rendicontazione,
                s.demat_arcad_stato,
                s.demat_plico_data_rendicontazione,
                s.demat_plico_stato,
                s.fine_recapito_data,
                s.fine_recapito_data_rendicontazione,
                s.fine_recapito_stato,
                s.flag_prodotto_estero,
                s.flag_wi7_consolidatore,
                s.flag_wi7_report_postalizzazioni_incomplete,
                s.geokey,
                s.iun,
                s.lotto,
                s.messaingiacenza_recapito_data,
                s.messaingiacenza_recapito_data_rendicontazione,
                s.messaingiacenza_recapito_stato,
                s.pcretry_rank,
                s.perfezionamento_data,
                s.perfezionamento_notificationdate,
                s.perfezionamento_stato,
                s.perfezionamento_stato_dettagli,
                s.perfezionamento_tipo,
                s.prodotto,
                s.recapitista_unificato,
                s.recindex_number,
                s.requestid,
                s.requesttimestamp,
                s.scarto_consolidatore_data,
                s.scarto_consolidatore_stato,
                s.senderpaid,
                s.statusrequest,
                s.tentativo_recapito_data,
                s.tentativo_recapito_data_rendicontazione,
                s.tentativo_recapito_stato,
                s.tms_cancelled,
                s.ultimo_evento_stato,
                s.wi7_cluster,
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
            -- Serve soltanto a limitare la scansione della timeline agli IUN candidati.
            perimetro_iun AS (
                SELECT DISTINCT iun FROM gold_perimetro
            ),
            send_analog_feedback AS (
                SELECT
                    tl.iun,
                    CAST(REGEXP_EXTRACT(tl.timelineelementid, 'RECINDEX_([0-9]+)', 1) AS INT) AS recindex_number,
                    CAST(REGEXP_EXTRACT(tl.timelineelementid, 'ATTEMPT_([0-9]+)', 1) AS INT) AS attempt_number
                FROM send.silver_timeline tl
                LEFT SEMI JOIN perimetro_iun p
                    ON tl.iun = p.iun
                WHERE tl.category = 'SEND_ANALOG_FEEDBACK'
            )
        SELECT g.*
        FROM gold_perimetro g
        LEFT ANTI JOIN send_analog_feedback saf
            ON g.iun = saf.iun
            AND CAST(g.recindex_number AS INT) = saf.recindex_number
            AND CAST(g.attempt_number AS INT) = saf.attempt_number
    """


def build_base_query() -> str:
    """Seconda e ultima query Spark: arricchisce il perimetro e applica i controlli.

    Ordine delle CTE, identico alla lettura della query storica:
      1) gold_perimetro: legge la GOLD gia' filtrata e cached (vista del main).
      2) perimetro_requestid / perimetro_iun: chiavi per filtrare altre sorgenti.
      3) temp_demat: ultimo allegato 23L/AR (explode seguito da row_number).
      4) base (subquery classificata) -> vista_silver_postalizzazione ->
         max_events_silver_postalizzazione: eventi, ultimi status e pivot per requestid.
      5) institution, gold_notification_perimetro: arricchimenti necessari.
      6) temp_postalizzazione_e_controlli -> temp_postalizzazione ->
         temp_assenza_eventi -> finale -> finale_filtrato: controlli invariati.

    Le CTE SILVER NON sono persistite separatamente: ciascun risultato e'
    usato una sola volta; materializzarle comporterebbe altre azioni Spark.
    """
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
        -- 4) L'assenza di SEND_ANALOG_FEEDBACK e' gia' verificata nella prima
        --    fase (build_gold_without_feedback_query) alla granularita'
        --    IUN + RECINDEX + ATTEMPT.
        --
        -- 5) La timeline e' interrogata solo per il controllo del feedback
        --    analogico, gia' eseguito nel perimetro GOLD iniziale.
        --
        -- 6) La cache temporanea riguarda solo il perimetro GOLD senza feedback.
        --    Le CTE SILVER rimangono qui, nell'ordine della query originaria.
        --    Il dataset finale verra' materializzato separatamente una volta.
        --    LEFT SEMI JOIN evita duplicazioni; DISTINCT resta solo sugli IUN.
        --    Il nome ente proviene da selfcare.gold_contracts (regola SSDA-1024).
        -- =====================================================================
        WITH gold_perimetro AS (
            -- Il perimetro e' gia' filtrato per SEND_ANALOG_FEEDBACK e persistito
            -- dal main: tutte le CTE successive leggono la stessa vista in cache.
            SELECT * FROM vw_gold_senza_feedback
        ),
            -- La semi-join sui requestid non richiede DISTINCT: anche se la
            -- chiave si ripetesse, il lato sinistro non verrebbe moltiplicato.
            perimetro_requestid AS (
                SELECT requestid FROM gold_perimetro
            ),
            perimetro_iun AS (
                SELECT DISTINCT iun FROM gold_perimetro
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
                -- OTTIMIZZAZIONE: solo filtro di esistenza, nessuna moltiplicazione.
                LEFT SEMI JOIN perimetro_requestid p
                    ON REGEXP_REPLACE(sp.requestid, '^pn-cons-000~', '') = p.requestid

                -- L'array eventslist viene esploso immediatamente dopo il filtro
                -- sul perimetro, senza essere trasportato in CTE intermedie.
                LATERAL VIEW explode(sp.eventslist) ev AS e
                LATERAL VIEW explode(e.paperprogrstatus.attachments) atts AS att

                WHERE att.documenttype IN ('23L', 'AR')
            ),
            base AS (
                -- Unico CASE di classificazione: filtro dopo la subquery,
                -- prima della finestra ROW_NUMBER; Filtro degli eventi classificati prima delle funzioni finestra.
                SELECT *
                FROM (
                    SELECT
                        REGEXP_REPLACE (t.requestid, '^pn-cons-000~', '') AS requestid,
                        e.paperprogrstatus.statuscode AS statuscode,
                        -- Normalizzazione della causale SILVER per i controlli di coerenza
                        CASE WHEN e.paperprogrstatus.deliveryfailurecause IN
                            ('M01','M02','M03','M04','M05','M06','M07','M08','M09')
                            THEN e.paperprogrstatus.deliveryfailurecause ELSE NULL END AS deliveryfailurecause,
                        CASE
                            WHEN LENGTH (e.paperprogrstatus.statusdatetime) = 17 THEN CONCAT (
                                SUBSTR (e.paperprogrstatus.statusdatetime, 1, 16),
                                ':00Z'
                            )
                            ELSE e.paperprogrstatus.statusdatetime
                        END AS statusdatetime_raw,
                        CASE
                            WHEN LENGTH (e.paperprogrstatus.clientrequesttimestamp) = 17 THEN CONCAT (
                                SUBSTR (e.paperprogrstatus.clientrequesttimestamp, 1, 16),
                                ':00.000Z'
                            )
                            ELSE e.paperprogrstatus.clientrequesttimestamp
                        END AS clientrequesttimestamp_raw,
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
                            -- Eventi DEMAT B/E utilizzati per i confronti delle date business.
                            -- RECAG011B resta valido nei controlli GOLD (tripletta/documenttype),
                            -- ma NON risale come DEMAT_RECAPITO SILVER (allineamento SSDA-1198).
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
                    -- OTTIMIZZAZIONE: preserva la stessa normalizzazione della SILVER.
                    LEFT SEMI JOIN perimetro_requestid p
                        ON REGEXP_REPLACE(t.requestid, '^pn-cons-000~', '') = p.requestid
                    LATERAL VIEW EXPLODE(t.eventslist) AS e
                    -- La classificazione CASE e' l'unica fonte dei codici validi.
                ) eventi_classificati
                WHERE tipo IS NOT NULL
            ),
            vista_silver_postalizzazione AS (
                SELECT
                    base.*,
                    -- SILVER UTC ISO Z -> orario italiano una sola volta; sessione Spark UTC.
                    FROM_UTC_TIMESTAMP(
                        CAST(REPLACE(REGEXP_REPLACE(statusdatetime_raw, 'Z$', ''), 'T', ' ') AS TIMESTAMP),
                        'Europe/Rome') AS statusdatetime,
                    FROM_UTC_TIMESTAMP(
                        CAST(REPLACE(REGEXP_REPLACE(clientrequesttimestamp_raw, 'Z$', ''), 'T', ' ') AS TIMESTAMP),
                        'Europe/Rome') AS clientrequesttimestamp,
                    ROW_NUMBER() OVER (
                        PARTITION BY requestid, tipo
                        ORDER BY clientrequesttimestamp_raw DESC
                    ) AS rn
                FROM base
            ),
            max_events_silver_postalizzazione AS (
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
                FROM vista_silver_postalizzazione
                WHERE rn = 1
                GROUP BY requestid
            ),
            -- SEND_ANALOG_FEEDBACK escluso in build_gold_without_feedback_query.


            -- OTTIMIZZAZIONE (SSDA-1024): senderdenomination da Selfcare;
            -- selezione dell'ultimo contratto prod-pn per internalistitutionid.
            -- A parita' di updatedat la stessa regola SQL mantiene la medesima
            -- assenza di tie-break del riferimento originale.
            institution AS (
                SELECT internalistitutionid, denomination
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
                ) t
                WHERE rn = 1
            ),

            -- Stesso principio per gold_notification_analytics:
            -- solo IUN utili e sole colonne necessarie alla query.
            gold_notification_perimetro AS (
                SELECT
                    n.iun,
                    n.tms_date_payment,
                    n.type_notif
                FROM send.gold_notification_analytics n
                -- OTTIMIZZAZIONE: conserva tutte le righe notification valide
                -- senza moltiplicarle per il numero di postalizzazioni GOLD.
                LEFT SEMI JOIN perimetro_iun p
                    ON n.iun = p.iun
            ),

            temp_postalizzazione_e_controlli AS (
                SELECT
                    s.senderpaid,
                    sn.denomination AS senderdenomination,
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
                    s.tms_cancelled,
                    n.tms_date_payment,
                    s.flag_wi7_consolidatore,
                    s.flag_wi7_report_postalizzazioni_incomplete,
                    s.wi7_cluster,
                    s.pcretry_rank,
                    s.attempt_rank,
                    s.attempt_number,
                    -- Tracker attivo solo dopo le date per prodotto: 890 senza data nota.
                    CASE
                        WHEN s.prodotto = 'AR' AND CAST(s.requesttimestamp AS DATE) >= DATE '2026-07-28' THEN 1
                        WHEN s.prodotto IN ('RIR','RIS') AND CAST(s.requesttimestamp AS DATE) >= DATE '2026-10-28' THEN 1
                        WHEN s.prodotto = 'RS' AND CAST(s.requesttimestamp AS DATE) >= DATE '2026-10-15' THEN 1
                        ELSE 0
                    END AS flag_tracker_attivo,
                    t.certificazione_recapito_stato AS certificazione_recapito_stato_silver,
                    t.certificazione_recapito_data AS certificazione_recapito_data_silver,
                    t.certificazione_recapito_rendicontazione AS certificazione_recapito_rendicontazione_silver,
                    t.fine_recapito_stato AS fine_recapito_stato_silver,
                    t.fine_recapito_deliveryfailurecause AS fine_recapito_deliveryfailurecause_silver,
                    t.fine_recapito_data AS fine_recapito_data_silver,
                    t.fine_recapito_rendicontazione AS fine_recapito_rendicontazione_silver,
                    t.demat_recapito_stato AS demat_recapito_stato_silver,
                    t.demat_recapito_deliveryfailurecause AS demat_recapito_deliveryfailurecause_silver,
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
                    LEFT JOIN institution sn ON s.senderpaid = sn.internalistitutionid
                    LEFT JOIN gold_notification_perimetro n ON n.iun = s.iun
                    LEFT JOIN max_events_silver_postalizzazione t ON s.requestid = t.requestid
            ),
            temp_postalizzazione AS (
                SELECT
                    s.*,
                    -- SSDA-1198: post-tracker fine recapito SILVER deve essere l'ultima rendicontazione.
                    CASE
                        WHEN s.flag_tracker_attivo = 0 THEN 0
                        WHEN s.fine_recapito_rendicontazione_silver IS NULL THEN 1
                        WHEN CAST(s.certificazione_recapito_data_rendicontazione AS TIMESTAMP) > s.fine_recapito_rendicontazione_silver
                          OR CAST(s.demat_23l_ar_data_rendicontazione AS TIMESTAMP) > s.fine_recapito_rendicontazione_silver
                          OR CAST(s.demat_plico_data_rendicontazione AS TIMESTAMP) > s.fine_recapito_rendicontazione_silver
                          OR CAST(s.demat_arcad_data_rendicontazione AS TIMESTAMP) > s.fine_recapito_rendicontazione_silver
                          OR CAST(s.tentativo_recapito_data_rendicontazione AS TIMESTAMP) > s.fine_recapito_rendicontazione_silver
                          OR CAST(s.messaingiacenza_recapito_data_rendicontazione AS TIMESTAMP) > s.fine_recapito_rendicontazione_silver
                          OR CAST(s.accettazione_23l_recag012_data_rendicontazione AS TIMESTAMP) > s.fine_recapito_rendicontazione_silver
                        THEN 1 ELSE 0
                    END AS controllo_rendicontazione_ordinata,
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
                    t.tms_cancelled,
                    t.flag_wi7_report_postalizzazioni_incomplete,
                    t.wi7_cluster,
                    t.pcretry_rank,
                    t.attempt_rank,
                    t.certificazione_recapito_stato_silver,
                    t.certificazione_recapito_data_silver,
                    t.certificazione_recapito_rendicontazione_silver,
                    t.fine_recapito_stato_silver,
                    t.fine_recapito_deliveryfailurecause_silver,
                    t.fine_recapito_data_silver,
                    t.fine_recapito_rendicontazione_silver,
                    t.demat_recapito_stato_silver,
                    t.demat_recapito_deliveryfailurecause_silver,
                    t.demat_recapito_data_silver,
                    t.demat_recapito_rendicontazione_silver,
                    t.flag_multi_destinatario,
                    t.flag_ultima_postalizzazione,
                    t.flag_prodotto_estero,
                    t.flag_destinatario_deceduto,
                    t.flag_esiti_mancanti,
                    t.flag_tracker_attivo,
                    t.attempt_number,
                    t.controllo_causale,
                    t.controllo_rendicontazione_ordinata,
                    CASE
                        WHEN controllo_causale = 1
                        OR controllo_rendicontazione_ordinata = 1
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
                        CASE WHEN controllo_rendicontazione_ordinata = 1 THEN 'errore ordine rendicontazione' END,
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
                    ) AS causa_mancato_feedback_analogico,
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
        causa_mancato_feedback_analogico
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
        causa_mancato_feedback_analogico
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
        causa_mancato_feedback_analogico
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
    """Un solo perimetro SQL; output L3/L4, CSV e log completo L4 / sintetico L3."""
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    start = now_utc_dt()
    date_it = start.astimezone(LOCAL_TZ).strftime("%Y%m%d")
    run_id = start.strftime("%Y%m%dT%H%M%SZ")
    start_str = start.strftime("%Y-%m-%d %H:%M:%S")
    max_date_str = "-"
    counts, csv_files = {}, {}
    base_df = None
    gold_df = None
    creds = None
    slack_webhook_url = get_slack_webhook_by_name(
        load_slack_webhooks(SLACK_CONFIG_CANDIDATE_PATHS), SLACK_WEBHOOK_NAME
    )
    try:
        creds = load_google_credentials(GOOGLE_SECRET_PATH)
        # Stati provvisori: in caso di failure, il log non dichiara OK.
        write_tab_meta(
            creds, "-", start_str, "-", "IN CORSO", "IN CORSO", counts, csv_files
        )
        logging.info("Drive: modalita CREATE-ONLY; nessuna ricerca di CSV precedenti")
        check_drive_folder_access(creds)
        max_date_str = get_last_update_date(spark)
        # FASE 1 - GOLD candidati e filtro SEND_ANALOG_FEEDBACK.
        # Una sola vista temporanea e una sola cache intermedia. Il COUNT serve
        # a rendere effettiva la persistenza e a misurare il perimetro.
        # MEMORY_ONLY evita di aggiungere una seconda cache su disco locale CDE:
        # se qualche blocco non entra in RAM, Spark potra' ricalcolarlo.
        # Questo NON impedisce gli spill temporanei degli shuffle sul disco.
        logging.info("[FASE START] GOLD senza feedback")
        gold_timer = time.perf_counter()
        logging.info(
            "Costruzione perimetro GOLD senza SEND_ANALOG_FEEDBACK; "
            "51 colonne sorgente per ridurre shuffle/cache"
        )
        gold_df = spark.sql(build_gold_without_feedback_query()).persist(
            StorageLevel.MEMORY_ONLY
        )
        gold_df.createOrReplaceTempView("vw_gold_senza_feedback")
        gold_count = gold_df.count()
        logging.info(
            "[FASE OK] GOLD senza feedback: %d righe, %.1f s",
            gold_count,
            time.perf_counter() - gold_timer,
        )

        # FASE 2 - Elaborazione dei controlli sul perimetro GOLD filtrato.
        # Non creiamo cache DEMAT/SILVER: la loro elaborazione e' gia' compresa
        # nella query e viene consumata una sola volta da questo risultato.
        # La vista GOLD, invece, viene riutilizzata nei sotto-perimetri di join.
        with log_phase("FASE 2/3 - Controlli e materializzazione report finale"):
            base_df = spark.sql(build_base_query()).persist(
                StorageLevel.MEMORY_AND_DISK
            )
            base_count = base_df.count()
            logging.info("Report finale materializzato: %d righe", base_count)

        # FASE 3 - A questo punto la costruzione del report e' conclusa.
        # Si rilascia la cache GOLD intermedia: resta solo base_df per le uscite.
        # In caso di perdita di blocchi finali Spark puo' ricalcolare la lineage;
        # rilasciare la GOLD e' quindi una scelta di contenimento memoria,
        # non un checkpoint o un taglio definitivo delle dipendenze.
        with log_phase("FASE 3/3 - Rilascio cache GOLD intermedia"):
            gold_df.unpersist(blocking=True)
            gold_df = None

        logging.info("Stock da bonificare: %d", base_count)
        if base_df.filter(F.col("requestid").isNull()).limit(1).count():
            raise ValueError("requestid nullo nel report generale")
        # Una riga per requestid; nessuna deduplica implicita.
        duplicates = (
            base_df.groupBy("requestid").count().filter("count > 1").limit(5).collect()
        )
        if duplicates:
            raise ValueError(f"Duplicazioni requestid nelle join: {duplicates}")
        # Anomalie sempre motivate: i motivi devono riflettere i nuovi controlli.
        if (
            base_df.filter(
                F.col("causa_mancato_feedback_analogico").isNull()
                | (F.length(F.trim(F.col("causa_mancato_feedback_analogico"))) == 0)
            )
            .limit(1)
            .count()
        ):
            raise ValueError("Oggetti da bonificare privi di motivazione")
        required_cols = {
            "controllo_causale",
            "controllo_rendicontazione_ordinata",
            "flag_tracker_attivo",
            "fine_recapito_deliveryfailurecause_silver",
            "demat_recapito_deliveryfailurecause_silver",
            "causa_mancato_feedback_analogico",
        }
        missing = required_cols - set(base_df.columns)
        if missing:
            raise ValueError(
                f"Campi diagnostici mancanti nel report: {sorted(missing)}"
            )
        base_df.createOrReplaceTempView("vw_finale_filtrato")
        queries = build_queries()

        # OUTPUT - riutilizziamo base_df gia' persistito per tutti i report.
        # L3/L4 invariati nei tracciati; L4 dettaglio completo + Iceberg.
        for label, sheet_id, tab_name in (
            ("L3", SHEET_ID_L3, "Estrazione Esiti da Bonificare"),
            ("general", SHEET_ID_L4, "Estrazione Esiti da Bonificare"),
        ):
            # Nessuna nuova cache: la proiezione L3 usa base_df come sorgente.
            # Il numero di righe non cambia perche' L3 seleziona solo colonne.
            with log_phase(f"Pubblicazione Google Sheet {label}"):
                df = spark.sql(queries[label])
                n = base_count
                counts[label] = n
                if label == "general":
                    overwrite_iceberg_table_from_df(df, TABLE_OUTPUT_REPORT_BONIFICA)
                pd_data = spark_to_df_per_gsheet(df)
                export_to_sheets_chunked(
                    pd_data,
                    creds,
                    sheet_id,
                    tab_name,
                    expected_written_cols=len(pd_data.columns),
                )
                logging.info("GSheet %s pubblicato: %d righe", label, n)

        # Una sola aggregazione leggera per conteggiare i recapitisti.
        # Evitiamo quattro persist addizionali: il dataset base e' gia' in cache.
        # Il filtro CSV viene letto una sola volta durante lo streaming.
        with log_phase("Conteggi recapitisti CSV"):
            rec_counts = {
                row["recapitista_unif"]: row["count"]
                for row in base_df.groupBy("recapitista_unif").count().collect()
            }
        for rec in CSV_NAMES:
            with log_phase(f"Esportazione recapitista {rec}"):
                df = spark.sql(queries[rec])
                n = int(rec_counts.get(rec, 0))
                counts[rec] = n
                logging.info("CSV %s: righe attese=%d", rec, n)
                csv_files[rec] = export_csv_streaming(df, n, rec, date_it, creds)
                logging.info(
                    "CSV %s: pubblicate %d righe in %d parti",
                    rec,
                    n,
                    len(csv_files[rec]),
                )
        # Eventuali recapitisti non previsti vengono evidenziati nel log, non eliminati da L3/L4.
        counts["non_attribuite"] = base_count - sum(
            counts.get(rec, 0) for rec in CSV_NAMES
        )
        if counts["non_attribuite"] < 0:
            raise ValueError(
                "Conteggi CSV superiori allo stock: verificare duplicazioni"
            )
        if counts["non_attribuite"]:
            logging.warning(
                "ATTENZIONE: %d oggetti appartengono a recapitisti diversi dai quattro CSV",
                counts["non_attribuite"],
            )
        stop = now_utc_dt()
        write_tab_meta(
            creds,
            max_date_str,
            start_str,
            stop.strftime("%Y-%m-%d %H:%M:%S"),
            elapsed_minutes_str(start, stop),
            "OK",
            counts,
            csv_files,
        )
        try:
            send_slack(
                slack_webhook_url,
                f"{build_slack_prefix()}\n✅ SUCCESS\nRunId: {run_id}\nStock: {base_count}\nCSV: {sum(len(v) for v in csv_files.values())}",
            )
        except Exception:
            logging.warning(
                "Notifica non riuscita: output e stato OK già pubblicati", exc_info=True
            )
        logging.info("SSDA-1077 completato")
    except Exception:
        logging.exception("SSDA-1077: errore esecuzione")
        if creds is not None:
            try:
                stop = now_utc_dt()
                write_tab_meta(
                    creds,
                    max_date_str,
                    start_str,
                    stop.strftime("%Y-%m-%d %H:%M:%S"),
                    elapsed_minutes_str(start, stop),
                    "KO",
                    counts,
                    csv_files,
                )
            except Exception:
                logging.exception("Impossibile scrivere KO sul log di controllo")
        try:
            send_slack(
                slack_webhook_url,
                f"{build_slack_prefix()}\n❌ FAILURE\nRunId: {run_id}",
            )
        except Exception:
            logging.warning("Notifica di errore non riuscita", exc_info=True)
        raise
    finally:
        if gold_df is not None:
            try:
                gold_df.unpersist(blocking=True)
            except Exception:
                logging.warning("Impossibile rilasciare gold_df", exc_info=True)
        if base_df is not None:
            try:
                base_df.unpersist()
            except Exception:
                logging.warning("Impossibile rilasciare base_df")
        try:
            spark.catalog.dropTempView("vw_gold_senza_feedback")
        except Exception:
            logging.debug("Vista temporanea GOLD gia' assente", exc_info=True)
        spark.stop()


if __name__ == "__main__":
    main()

"""
===============================================================================
SQL PER IMPALA — SSDA-1077
===============================================================================
COPIARE SOLTANTO LA QUERY CHE INIZIA DA WITH E TERMINA CON IL ;
NON e' codice Python eseguibile: il blocco e' una stringa multilinea isolata.
Questa e' la versione LOGICA completa, senza dipendenze dalle TEMP VIEW di CDE.
Le espansioni ARRAY sono adattate alla sintassi Impala (JOIN su colonne ARRAY).
ATTENZIONE: verificare in DBeaver con EXPLAIN che il catalogo Impala esponga
le nested collections come ev.item e atts.item per queste tabelle specifiche.
Non e' stata eseguita contro un cluster Impala; eventuali differenze di
versione/tipi nelle strutture annidate richiedono validazione SQL.
I controlli business sono ripresi dalla versione SQL originale.
===============================================================================


        WITH gold_candidati AS (
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
            -- Serve soltanto a limitare la scansione della timeline agli IUN candidati.
            perimetro_iun_candidati AS (
                SELECT DISTINCT iun FROM gold_candidati
            ),
            send_analog_feedback AS (
                SELECT
                    tl.iun,
                    CAST(REGEXP_EXTRACT(tl.timelineelementid, 'RECINDEX_([0-9]+)', 1) AS INT) AS recindex_number,
                    CAST(REGEXP_EXTRACT(tl.timelineelementid, 'ATTEMPT_([0-9]+)', 1) AS INT) AS attempt_number
                FROM send.silver_timeline tl
                LEFT SEMI JOIN perimetro_iun_candidati p
                    ON tl.iun = p.iun
                WHERE tl.category = 'SEND_ANALOG_FEEDBACK'
            ),
        gold_perimetro AS (
            SELECT g.*
            FROM gold_candidati g
            LEFT ANTI JOIN send_analog_feedback saf
                ON g.iun = saf.iun
                AND CAST(g.recindex_number AS INT) = saf.recindex_number
                AND CAST(g.attempt_number AS INT) = saf.attempt_number
        )
        ,
            perimetro_requestid AS (
                SELECT requestid FROM gold_perimetro
            ),
            perimetro_iun AS (
                SELECT DISTINCT iun FROM gold_perimetro
            ),
            temp_demat AS (
                SELECT
                    REGEXP_REPLACE(sp.requestid, '^pn-cons-000~', '') AS requestid,
                    atts.item.documenttype AS documenttype,
                    ROW_NUMBER() OVER (
                        PARTITION BY REGEXP_REPLACE(sp.requestid, '^pn-cons-000~', '')
                        ORDER BY ev.item.paperprogrstatus.clientrequesttimestamp DESC
                    ) AS rn_demat
                FROM send.silver_postalizzazione sp,
                     sp.eventslist ev,
                     ev.item.paperprogrstatus.attachments atts
                LEFT SEMI JOIN perimetro_requestid p
                    ON REGEXP_REPLACE(sp.requestid, '^pn-cons-000~', '') = p.requestid

                WHERE atts.item.documenttype IN ('23L', 'AR')
            ),
            base AS (
                -- Unico CASE di classificazione: filtro dopo la subquery,
                -- prima della finestra ROW_NUMBER; nessuna tabella intermedia.
                SELECT *
                FROM (
                    SELECT
                        REGEXP_REPLACE (t.requestid, '^pn-cons-000~', '') AS requestid,
                        ev.item.paperprogrstatus.statuscode AS statuscode,
                        -- SSDA-1198: causale SILVER necessaria ai nuovi controlli.
                        CASE WHEN ev.item.paperprogrstatus.deliveryfailurecause IN
                            ('M01','M02','M03','M04','M05','M06','M07','M08','M09')
                            THEN ev.item.paperprogrstatus.deliveryfailurecause ELSE NULL END AS deliveryfailurecause,
                        CASE
                            WHEN LENGTH (ev.item.paperprogrstatus.statusdatetime) = 17 THEN CONCAT (
                                SUBSTR (ev.item.paperprogrstatus.statusdatetime, 1, 16),
                                ':00Z'
                            )
                            ELSE ev.item.paperprogrstatus.statusdatetime
                        END AS statusdatetime_raw,
                        CASE
                            WHEN LENGTH (ev.item.paperprogrstatus.clientrequesttimestamp) = 17 THEN CONCAT (
                                SUBSTR (ev.item.paperprogrstatus.clientrequesttimestamp, 1, 16),
                                ':00.000Z'
                            )
                            ELSE ev.item.paperprogrstatus.clientrequesttimestamp
                        END AS clientrequesttimestamp_raw,
                        CASE
                            WHEN ev.item.paperprogrstatus.statuscode IN (
                                -- RS temporaneamente escluso. Per ripristinarlo decommentare:
                                -- 'RECRS001C','RECRS002C','RECRS002F','RECRS003C','RECRS004C','RECRS005C',
                                'RECRN001C','RECRN002C','RECRN002F','RECRN003C','RECRN004C','RECRN005C',
                                'RECAG001C','RECAG002C','RECAG003C','RECAG003F','RECAG005C','RECAG006C','RECAG007C','RECAG008C',
                                -- RIS temporaneamente escluso. Per ripristinarlo decommentare:
                                -- 'RECRSI003C','RECRSI004C',
                                'RECRI003C','RECRI004C'
                            ) THEN 'FINE_RECAPITO'
                            WHEN ev.item.paperprogrstatus.statuscode IN (
                                -- RS temporaneamente escluso. Per ripristinarlo decommentare:
                                -- 'RECRS001C','RECRS002A','RECRS002D','RECRS003C','RECRS004A','RECRS005A',
                                'RECRN001A','RECRN002A','RECRN002D','RECRN003A','RECRN004A','RECRN005A',
                                'RECAG001A','RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A','RECAG007A','RECAG008A',
                                -- RIS temporaneamente escluso. Per ripristinarlo decommentare:
                                -- 'RECRSI003C','RECRSI004A',
                                'RECRI003A','RECRI004A'
                            ) THEN 'CERTIFICAZIONE_RECAPITO'
                            -- Eventi DEMAT B/E utilizzati per i confronti delle date business.
                            -- RECAG011B resta valido nei controlli GOLD (tripletta/documenttype),
                            -- ma NON risale come DEMAT_RECAPITO SILVER (allineamento SSDA-1198).
                            WHEN ev.item.paperprogrstatus.statuscode IN (
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
                    FROM send.silver_postalizzazione t,
                         t.eventslist ev
                    LEFT SEMI JOIN perimetro_requestid p
                        ON REGEXP_REPLACE(t.requestid, '^pn-cons-000~', '') = p.requestid
                    -- La classificazione CASE e' l'unica fonte dei codici validi.
                ) eventi_classificati
                WHERE tipo IS NOT NULL
            ),
            vista_silver_postalizzazione AS (
                SELECT
                    base.*,
                    -- SILVER UTC ISO Z -> orario italiano una sola volta; sessione Spark UTC.
                    FROM_UTC_TIMESTAMP(
                        CAST(REPLACE(REGEXP_REPLACE(statusdatetime_raw, 'Z$', ''), 'T', ' ') AS TIMESTAMP),
                        'Europe/Rome') AS statusdatetime,
                    FROM_UTC_TIMESTAMP(
                        CAST(REPLACE(REGEXP_REPLACE(clientrequesttimestamp_raw, 'Z$', ''), 'T', ' ') AS TIMESTAMP),
                        'Europe/Rome') AS clientrequesttimestamp,
                    ROW_NUMBER() OVER (
                        PARTITION BY requestid, tipo
                        ORDER BY clientrequesttimestamp_raw DESC
                    ) AS rn
                FROM base
            ),
            max_events_silver_postalizzazione AS (
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
                FROM vista_silver_postalizzazione
                WHERE rn = 1
                GROUP BY requestid
            ),
            -- SEND_ANALOG_FEEDBACK escluso in build_gold_without_feedback_query.


            -- OTTIMIZZAZIONE (SSDA-1024): senderdenomination da Selfcare;
            -- selezione dell'ultimo contratto prod-pn per internalistitutionid.
            -- A parita' di updatedat la stessa regola SQL mantiene la medesima
            -- assenza di tie-break del riferimento originale.
            institution AS (
                SELECT internalistitutionid, denomination
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
                ) t
                WHERE rn = 1
            ),

            -- Stesso principio per gold_notification_analytics:
            -- solo IUN utili e sole colonne necessarie alla query.
            gold_notification_perimetro AS (
                SELECT
                    n.iun,
                    n.tms_date_payment,
                    n.type_notif
                FROM send.gold_notification_analytics n
                -- OTTIMIZZAZIONE: conserva tutte le righe notification valide
                -- senza moltiplicarle per il numero di postalizzazioni GOLD.
                LEFT SEMI JOIN perimetro_iun p
                    ON n.iun = p.iun
            ),

            temp_postalizzazione_e_controlli AS (
                SELECT
                    s.senderpaid,
                    sn.denomination AS senderdenomination,
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
                    s.tms_cancelled,
                    n.tms_date_payment,
                    s.flag_wi7_consolidatore,
                    s.flag_wi7_report_postalizzazioni_incomplete,
                    s.wi7_cluster,
                    s.pcretry_rank,
                    s.attempt_rank,
                    s.attempt_number,
                    -- Tracker attivo solo dopo le date per prodotto: 890 senza data nota.
                    CASE
                        WHEN s.prodotto = 'AR' AND CAST(s.requesttimestamp AS DATE) >= DATE '2026-07-28' THEN 1
                        WHEN s.prodotto IN ('RIR','RIS') AND CAST(s.requesttimestamp AS DATE) >= DATE '2026-10-28' THEN 1
                        WHEN s.prodotto = 'RS' AND CAST(s.requesttimestamp AS DATE) >= DATE '2026-10-15' THEN 1
                        ELSE 0
                    END AS flag_tracker_attivo,
                    t.certificazione_recapito_stato AS certificazione_recapito_stato_silver,
                    t.certificazione_recapito_data AS certificazione_recapito_data_silver,
                    t.certificazione_recapito_rendicontazione AS certificazione_recapito_rendicontazione_silver,
                    t.fine_recapito_stato AS fine_recapito_stato_silver,
                    t.fine_recapito_deliveryfailurecause AS fine_recapito_deliveryfailurecause_silver,
                    t.fine_recapito_data AS fine_recapito_data_silver,
                    t.fine_recapito_rendicontazione AS fine_recapito_rendicontazione_silver,
                    t.demat_recapito_stato AS demat_recapito_stato_silver,
                    t.demat_recapito_deliveryfailurecause AS demat_recapito_deliveryfailurecause_silver,
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
                    LEFT JOIN institution sn ON s.senderpaid = sn.internalistitutionid
                    LEFT JOIN gold_notification_perimetro n ON n.iun = s.iun
                    LEFT JOIN max_events_silver_postalizzazione t ON s.requestid = t.requestid
            ),
            temp_postalizzazione AS (
                SELECT
                    s.*,
                    -- SSDA-1198: post-tracker fine recapito SILVER deve essere l'ultima rendicontazione.
                    CASE
                        WHEN s.flag_tracker_attivo = 0 THEN 0
                        WHEN s.fine_recapito_rendicontazione_silver IS NULL THEN 1
                        WHEN CAST(s.certificazione_recapito_data_rendicontazione AS TIMESTAMP) > s.fine_recapito_rendicontazione_silver
                          OR CAST(s.demat_23l_ar_data_rendicontazione AS TIMESTAMP) > s.fine_recapito_rendicontazione_silver
                          OR CAST(s.demat_plico_data_rendicontazione AS TIMESTAMP) > s.fine_recapito_rendicontazione_silver
                          OR CAST(s.demat_arcad_data_rendicontazione AS TIMESTAMP) > s.fine_recapito_rendicontazione_silver
                          OR CAST(s.tentativo_recapito_data_rendicontazione AS TIMESTAMP) > s.fine_recapito_rendicontazione_silver
                          OR CAST(s.messaingiacenza_recapito_data_rendicontazione AS TIMESTAMP) > s.fine_recapito_rendicontazione_silver
                          OR CAST(s.accettazione_23l_recag012_data_rendicontazione AS TIMESTAMP) > s.fine_recapito_rendicontazione_silver
                        THEN 1 ELSE 0
                    END AS controllo_rendicontazione_ordinata,
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
                    t.tms_cancelled,
                    t.flag_wi7_report_postalizzazioni_incomplete,
                    t.wi7_cluster,
                    t.pcretry_rank,
                    t.attempt_rank,
                    t.certificazione_recapito_stato_silver,
                    t.certificazione_recapito_data_silver,
                    t.certificazione_recapito_rendicontazione_silver,
                    t.fine_recapito_stato_silver,
                    t.fine_recapito_deliveryfailurecause_silver,
                    t.fine_recapito_data_silver,
                    t.fine_recapito_rendicontazione_silver,
                    t.demat_recapito_stato_silver,
                    t.demat_recapito_deliveryfailurecause_silver,
                    t.demat_recapito_data_silver,
                    t.demat_recapito_rendicontazione_silver,
                    t.flag_multi_destinatario,
                    t.flag_ultima_postalizzazione,
                    t.flag_prodotto_estero,
                    t.flag_destinatario_deceduto,
                    t.flag_esiti_mancanti,
                    t.flag_tracker_attivo,
                    t.attempt_number,
                    t.controllo_causale,
                    t.controllo_rendicontazione_ordinata,
                    CASE
                        WHEN controllo_causale = 1
                        OR controllo_rendicontazione_ordinata = 1
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
                        CASE WHEN controllo_rendicontazione_ordinata = 1 THEN 'errore ordine rendicontazione' END,
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
                    ) AS causa_mancato_feedback_analogico,
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
        SELECT * FROM finale_filtrato;
"""
