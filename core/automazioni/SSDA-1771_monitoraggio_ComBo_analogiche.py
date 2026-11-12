#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SSDA-1771 - Monitoraggio giornaliero delle Comunicazioni Bonarie Analogiche.

Il perimetro include PREPARE_ANALOG_DELIVERY% e PREPARE_ANALOG_MESSAGE% (c'è stato un cambio denominazione il 26 ottobre 2026).

Il job produce tre tab nel Google Sheet: dettaglio, pivot stati e log di controllo.
Perimetro di richieste dal 19/10/2026 incluso, filtrato su requesttimestamp.
Per ogni requestid seleziona l'ultimo evento rendicontato diverso da CON020 e
REC090; in assenza di eventi validi mantiene P000 o IN_ATTESA_EVENTI.
Gli eventi CON/RS/RIS sono mappati secondo "Integrazione con Consolidatore"
(PagoPA Confluence, consultata il 28/10/2026):
https://pagopa.atlassian.net/wiki/spaces/PN/pages/643368025/Integrazione+con+Consolidatore
"""

import json
import logging
import os
import time
import urllib.request
from datetime import datetime, timezone

import gspread
import pandas as pd
from google.oauth2 import service_account
from pdnd_google_utils import Sheet
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

APP_NAME = "SSDA-1771 Monitoraggio ComBo"
SHEET_ID = "1-HxVMX_78xQNdbO0OzVWIgBfdvVVS_vWDg00C_67VOY"
GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"
TAB_DETTAGLIO = "Dettaglio ComBo"
TAB_RIEPILOGO = "Pivot Stati"
TAB_LOG = "Log Controllo"
SLACK_CONFIG_CANDIDATE_PATHS = [
    "/Slack/notifications_webhook.txt",
    "/app/mount/Slack/notifications_webhook.txt",
    "/app/mount/notifications_webhook.txt",
]
SLACK_CHANNEL_WEBHOOK_NAME = "webhook_monitoraggio_ComBo_Analogiche"  # Webhook per comunicazioni del monitoraggio
SLACK_OPERATIONS_WEBHOOK_NAME = "webhook_1"  # Webhook per esito esecuzione
SLACK_TIMEOUT_SEC = 20
MAX_SHEET_CELLS = 10_000_000
UPLOAD_CHUNK = 1000
MAX_EXPORT_ROWS = 150000
SLACK_LINK = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit"
LOG_COLS = [
    "run_id_utc",
    "inizio_utc",
    "fine_utc",
    "durata_min",
    "esito",
    "oggetti",
    "rendicontati",
    "in_attesa",
    "stati_distinti",
    "max_data_rendicontazione",
    "dettaglio_errore",
]
DETAIL_COLS = [
    "requestid",
    "iun",
    "statuscode",
    "stato_monitoraggio",
    "statusdatetime",
    "clientrequesttimestamp",
]
PIVOT_COLS = ["stato_monitoraggio", "statuscode", "numero_oggetti"]
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Ordine funzionale delle righe della pivot, basato sulla sequenza degli
# stati nell'estratto "Integrazione con Consolidatore" (CON, RS, RIS).
# I codici CON020 e REC090 sono esclusi dalla selezione e non vanno mappati.
# Non includiamo stati AR, 890 o RIR: non appartengono ai prodotti richiesti.
STATUS_MAPPING = [
    ("P000", "AFFIDO_CONSOLIDATORE"),
    ("CON998", "ERRORE_RECUPERO_DOCUMENTO"),
    ("CON997", "ERRORE_SMISTAMENTO"),
    ("CON996", "ERRORE_VERIFICA_PDF"),
    ("CON995", "ERRORE_STAMPA"),
    ("CON993", "ERRORE_STAMPA_PARZIALE"),
    ("CON080", "STAMPATO_IMBUSTATO"),
    ("CON010", "DISTINTA_ELETTRONICA_INVIATA"),
    ("CON011", "DISTINTA_ELETTRONICA_SIGILLATA"),
    ("CON012", "DISTINTA_ELETTRONICA_ACCETTATA"),
    ("CON992", "DISTINTA_ELETTRONICA_RIFIUTATA"),
    ("CON09A", "MATERIALITA_PRONTA_PER_RITIRO"),
    ("CON016", "PICKUP_RECAPITISTA"),
    ("CON018", "ACCETTAZIONE_RECAPITISTA"),
    ("CON991", "MANCATA_ACCETTAZIONE_RECAPITISTA"),
    ("RECRS010", "INESITO_RECAPITO"),
    ("RECRS011", "IN_GIACENZA"),
    ("RECRS001C", "CONSEGNATO_FASCICOLO_CHIUSO"),
    ("RECRS002A", "MANCATA_CONSEGNA_PRE_ESITO"),
    ("RECRS002B", "MANCATA_CONSEGNA_IN_DEMATERIALIZZAZIONE"),
    ("RECRS002C", "MANCATA_CONSEGNA_FASCICOLO_CHIUSO"),
    ("RECRS002D", "IRREPERIBILITA_ASSOLUTA_PRE_ESITO"),
    ("RECRS002E", "IRREPERIBILITA_IN_DEMATERIALIZZAZIONE"),
    ("RECRS002F", "IRREPERIBILITA_FASCICOLO_CHIUSO"),
    ("RECRS003C", "CONSEGNATO_GIACENZA_FASCICOLO_CHIUSO"),
    ("RECRS004A", "MANCATA_CONSEGNA_GIACENZA_PRE_ESITO"),
    ("RECRS004B", "MANCATA_CONSEGNA_GIACENZA_IN_DEMATERIALIZZAZIONE"),
    ("RECRS004C", "MANCATA_CONSEGNA_GIACENZA_FASCICOLO_CHIUSO"),
    ("RECRS005A", "COMPIUTA_GIACENZA_PRE_ESITO"),
    ("RECRS005B", "COMPIUTA_GIACENZA_IN_DEMATERIALIZZAZIONE"),
    ("RECRS005C", "COMPIUTA_GIACENZA_FASCICOLO_CHIUSO"),
    ("RECRS006", "FURTO_SMARRIMENTO_DETERIORAMENTO"),
    ("RECRS013", "NON_RENDICONTABILE"),
    ("RECRS015", "CAUSA_FORZA_MAGGIORE"),
    ("RECRSI001", "AVVIATO_ALL_ESTERO"),
    ("RECRSI002", "INGRESSO_NEL_PAESE_ESTERO"),
    ("RECRSI003C", "CONSEGNATO_RIS_FASCICOLO_CHIUSO"),
    ("RECRSI004A", "NON_CONSEGNATO_RIS_PRE_ESITO"),
    ("RECRSI004B", "NON_CONSEGNATO_RIS_IN_DEMATERIALIZZAZIONE"),
    ("RECRSI004C", "NON_CONSEGNATO_RIS_FASCICOLO_CHIUSO"),
    ("RECRSI005", "FURTO_SMARRIMENTO_DETERIORAMENTO_RIS"),
]
STATUS_ORDER = {code: i for i, (code, label) in enumerate(STATUS_MAPPING)}
# Gli stati di fallback sono mostrati dopo gli eventi noti.
STATUS_ORDER[""] = len(STATUS_ORDER)


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(APP_NAME)

# Spark esegue una sola lettura della SILVER con OUTER EXPLODE.
# I valori di mapping sono costanti definite nel codice e non provengono da input esterni.
# La CASE SQL e l'ordine della pivot condividono STATUS_MAPPING: un'unica fonte.
STATUS_CASE = "\n".join(
    f"            WHEN x.statuscode = '{code}' THEN '{label}'"
    for code, label in STATUS_MAPPING
)
QUERY = r"""
-- SSDA-1771 | Monitoraggio ComBo - Spark SQL
-- Applichiamo il perimetro PRIMA dell'esplosione di eventslist.
-- Il campo requesttimestamp e' stringa con data ISO all'inizio (YYYY-MM-DD).
-- La comparazione lessicografica e' corretta finche' il formato e' ISO.
WITH combo_eventi AS (
    SELECT
        t.requestid,
        NULLIF(TRIM(e.paperprogrstatus.statuscode), '') AS statuscode,
        CASE
            WHEN LENGTH(e.paperprogrstatus.statusdatetime) = 17
                THEN CONCAT(SUBSTR(e.paperprogrstatus.statusdatetime, 1, 16), ':00Z')
            ELSE e.paperprogrstatus.statusdatetime
        END AS statusdatetime,
        CASE
            WHEN LENGTH(e.paperprogrstatus.clientrequesttimestamp) = 17
                THEN CONCAT(SUBSTR(e.paperprogrstatus.clientrequesttimestamp, 1, 16), ':00.000Z')
            ELSE e.paperprogrstatus.clientrequesttimestamp
        END AS clientrequesttimestamp
    FROM send.silver_postalizzazione t
    LATERAL VIEW OUTER EXPLODE(t.eventslist) ev AS e
    WHERE (
        requestid LIKE 'pn-cons-000~PREPARE_ANALOG_DELIVERY%'
        OR requestid LIKE 'pn-cons-000~PREPARE_ANALOG_MESSAGE%'
    )
      AND requesttimestamp >= '2026-10-19'
),
prioritizzati AS (
    SELECT
        requestid, statuscode, statusdatetime, clientrequesttimestamp,
        CASE
            -- Escludiamo questi eventi dalla scelta, SENZA perdere i requestid.
            -- Il fallback permette di recuperare l'evento utile precedente.
            WHEN statuscode IN ('CON020', 'REC090') THEN 2
            WHEN statuscode = 'P000' THEN 1
            WHEN statuscode IS NOT NULL
             AND NULLIF(TRIM(clientrequesttimestamp), '') IS NOT NULL THEN 0
            ELSE 2
        END AS priorita
    FROM combo_eventi
),
ranking AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY requestid
            ORDER BY
                priorita ASC,
                clientrequesttimestamp DESC NULLS LAST,
                statusdatetime DESC NULLS LAST,
                statuscode DESC NULLS LAST
        ) AS rn
    FROM prioritizzati
),
ultimo_evento AS (
    SELECT
        requestid,
        CASE WHEN priorita < 2 THEN statuscode END AS statuscode,
        CASE WHEN priorita < 2 THEN statusdatetime END AS statusdatetime,
        CASE WHEN priorita < 2 THEN clientrequesttimestamp END AS clientrequesttimestamp
    FROM ranking
    WHERE rn = 1
)
SELECT
    x.requestid,
    -- Lo IUN viene estratto dal requestid senza join aggiuntive.
    NULLIF(REGEXP_EXTRACT(x.requestid, 'IUN_([^.]+)', 1), '') AS iun,
    x.statuscode,
    CASE
        WHEN x.statuscode IS NULL THEN 'IN_ATTESA_EVENTI'
__STATUS_CASE__
        ELSE CONCAT('NON_MAPPATO_', x.statuscode)
    END AS stato_monitoraggio,
    x.statusdatetime,
    x.clientrequesttimestamp
FROM ultimo_evento x
""".replace("__STATUS_CASE__", STATUS_CASE)


# --- Funzioni di utilità ---
def utc_now():
    return datetime.now(timezone.utc)


def utc_fmt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


# --- Google Sheets: credenziali, tab e upload ---
def get_credentials():
    creds = {}
    for filename in os.listdir(GOOGLE_SECRET_PATH):
        fp = os.path.join(GOOGLE_SECRET_PATH, filename)
        if os.path.isfile(fp):
            with open(fp, encoding="utf-8") as file:
                creds[filename] = file.read().strip()
    if "client_email" not in creds or "private_key" not in creds:
        raise RuntimeError("Secret Google non valido/incompleto")
    log.info(
        "Credenziali Google caricate (nomi file, mai valori): %s", sorted(creds.keys())
    )
    return creds


def google_client(creds):
    auth = service_account.Credentials.from_service_account_info(creds, scopes=SCOPES)
    return gspread.authorize(auth)


def retry_gsheet(fn, label, attempts=5):
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except gspread.exceptions.APIError as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status not in (429, 500, 502, 503, 504) or attempt == attempts:
                raise
            delay = min(10 * 2 ** (attempt - 1), 60)
            log.warning(
                "Google API %s per %s, tentativo %s/%s, retry in %ss",
                status,
                label,
                attempt,
                attempts,
                delay,
            )
            time.sleep(delay)


def ensure_tabs(spreadsheet):
    existing = {ws.title: ws for ws in spreadsheet.worksheets()}
    for name, columns in (
        (TAB_DETTAGLIO, len(DETAIL_COLS)),
        (TAB_RIEPILOGO, len(PIVOT_COLS)),
        (TAB_LOG, len(LOG_COLS)),
    ):
        if name not in existing:
            existing[name] = retry_gsheet(
                lambda name=name, columns=columns: spreadsheet.add_worksheet(
                    title=name, rows=100, cols=columns
                ),
                f"create tab {name}",
            )
    return existing


def update_log(ws, record):
    """Sovrascrive il log con intestazione e un solo record dell'esecuzione corrente."""
    values = [str(record.get(col, "")) for col in LOG_COLS]
    # Due righe fisse: non vengono mantenute informazioni delle esecuzioni precedenti.
    retry_gsheet(lambda: ws.resize(rows=2, cols=len(LOG_COLS)), "log resize")
    retry_gsheet(
        lambda: ws.update(
            range_name="A1", values=[LOG_COLS, values], value_input_option="RAW"
        ),
        "log overwrite",
    )


def check_spreadsheet_capacity(spreadsheet, detail_rows, pivot_rows):
    """Controlla il limite complessivo di celle prima di modificare i tab dati."""
    planned = {
        TAB_DETTAGLIO: (max(2, detail_rows + 1), len(DETAIL_COLS)),
        TAB_RIEPILOGO: (max(2, pivot_rows + 1), len(PIVOT_COLS)),
    }
    cell_count = 0
    for ws in retry_gsheet(lambda: spreadsheet.worksheets(), "spreadsheet capacity"):
        rows, cols = planned.get(ws.title, (ws.row_count, ws.col_count))
        cell_count += rows * cols
    if cell_count >= MAX_SHEET_CELLS:
        raise RuntimeError(f"Capienza GSheet superata: {cell_count} celle previste")
    log.info("Capienza GSheet prevista dopo export: %s celle", cell_count)


def overwrite_tab(creds, ws, name, df):
    rows = len(df)
    columns = len(df.columns)
    if rows > MAX_EXPORT_ROWS:
        raise RuntimeError(f"Export {name} troppo grande: {rows}")
    # Ridimensionamento per eliminare righe residue e contenere la griglia.
    # Verifica della capienza complessiva dello spreadsheet (10M celle).
    target_rows = max(2, rows + 1)
    retry_gsheet(lambda: ws.resize(rows=target_rows, cols=columns), f"resize {name}")
    sheet = Sheet(sheet_id=SHEET_ID, service_credentials=creds, id_mode="key")
    if rows == 0:
        retry_gsheet(
            lambda: sheet.upload(
                worksheet_name=name,
                panda_df=df,
                ul_cell="A1",
                header=True,
                clear_on_open=True,
            ),
            f"empty upload {name}",
        )
    else:
        for offset in range(0, rows, UPLOAD_CHUNK):
            chunk = df.iloc[offset : offset + UPLOAD_CHUNK]
            first = offset == 0
            # Non ripetere automaticamente upload parziali in caso di timeout ambiguo:
            # il write e' su range deterministico, ma il controllo post-scrittura e' obbligatorio.
            sheet.upload(
                worksheet_name=name,
                panda_df=chunk,
                ul_cell="A1" if first else f"A{offset + 2}",
                header=first,
                clear_on_open=first,
            )
            log.info("Upload %s: %s/%s righe", name, offset + len(chunk), rows)
    header = retry_gsheet(lambda: ws.row_values(1), f"header check {name}")
    if header[:columns] != list(df.columns):
        raise RuntimeError(f"Header non coerente nel tab {name}: {header}")
    # Verifica numero righe non vuote nella prima colonna (requestid/stato sempre popolati).
    actual = len(retry_gsheet(lambda: ws.col_values(1), f"rows check {name}")) - 1
    if actual != rows:
        raise RuntimeError(
            f"Righe non coerenti su {name}: attese={rows}, lette={actual}"
        )
    log.info("Tab %s verificato: %s righe", name, actual)


# --- Slack: configurazione e notifiche ---
def load_webhooks():
    for filename in SLACK_CONFIG_CANDIDATE_PATHS:
        if not os.path.isfile(filename):
            continue
        out = {}
        with open(filename, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    if key.strip() and val.strip():
                        out[key.strip()] = val.strip()
        if out:
            log.info("Webhook config rilevata da %s, nomi=%s", filename, sorted(out))
            return out
    return {}


def send_slack(url, message):
    if not url:
        raise RuntimeError("Webhook Slack non disponibile: messaggio non inviato")
    request = urllib.request.Request(
        url,
        data=json.dumps({"text": message}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=SLACK_TIMEOUT_SEC) as response:
        body = response.read().decode("utf-8", errors="replace").strip()
        if response.status < 200 or response.status >= 300 or body != "ok":
            raise RuntimeError(
                f"Slack risposta HTTP={response.status}, body={body[:100]}"
            )


def safe_notify(url, message):
    try:
        send_slack(url, message)
        return True
    except Exception:
        log.exception("Invio Slack fallito")
        return False


# --- Elaborazione Spark e coordinamento del job ---
def main():
    started = utc_now()
    run_id = started.strftime("%Y%m%dT%H%M%SZ")
    summary = {
        "oggetti": 0,
        "rendicontati": 0,
        "in_attesa": 0,
        "stati_distinti": 0,
        "max_data_rendicontazione": "-",
    }
    record = {
        **summary,
        "run_id_utc": run_id,
        "inizio_utc": utc_fmt(started),
        "fine_utc": "",
        "durata_min": "",
        "esito": "IN_CORSO",
        "dettaglio_errore": "",
    }
    spark = None
    df = None
    log_ws = None
    hooks = load_webhooks()
    ops_hook = hooks.get(SLACK_OPERATIONS_WEBHOOK_NAME)
    channel_hook = hooks.get(SLACK_CHANNEL_WEBHOOK_NAME)
    log.info("Avvio %s - run=%s - sheet=%s", APP_NAME, run_id, SLACK_LINK)
    try:
        if not ops_hook or not channel_hook:
            raise RuntimeError("Configurazione Slack mancante: verificare webhook_1")
        creds = get_credentials()
        spreadsheet = retry_gsheet(
            lambda: google_client(creds).open_by_key(SHEET_ID), "open spreadsheet"
        )
        tabs = ensure_tabs(spreadsheet)
        log_ws = tabs[TAB_LOG]
        update_log(log_ws, record)

        spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
        log.info(
            "Esecuzione SQL ComBo: perimetro unificato, una scansione con OUTER EXPLODE"
        )
        df = spark.sql(QUERY).persist(StorageLevel.MEMORY_AND_DISK)
        summary["oggetti"] = df.count()
        if summary["oggetti"] == 0:
            raise RuntimeError(
                "Perimetro ComBo vuoto: interrompo export per non cancellare dati validi"
            )
        if summary["oggetti"] > MAX_EXPORT_ROWS:
            raise RuntimeError("Superata soglia di sicurezza prima di toPandas")
        if df.select("requestid").distinct().count() != summary["oggetti"]:
            raise RuntimeError("Duplicazioni requestid nel dataset di dettaglio")
        if df.filter(F.col("requestid").isNull()).limit(1).count():
            raise RuntimeError("Presenza di requestid NULL")
        # Lo IUN viene ricavato dal requestid
        if df.filter(F.col("iun").isNull()).limit(1).count():
            raise RuntimeError("Presenza di IUN NULL estratto dal requestid")
        # L'evento scelto non puo' mai essere uno dei due codici esclusi.
        if df.filter(F.col("statuscode").isin("CON020", "REC090")).limit(1).count():
            raise RuntimeError("Selezionato un evento escluso (CON020/REC090)")
        # La pivot raggruppa per stato_monitoraggio (stesso valore del dettaglio)
        # e codice: colonna A stato_monitoraggio, B statuscode, C numero_oggetti.
        # Un codice sconosciuto resta riconoscibile e non viene aggregato ad altri.
        pivot_rows = (
            df.groupBy("stato_monitoraggio", "statuscode")
            .agg(F.count("*").alias("numero_oggetti"))
            .collect()
        )
        pivot_data = sorted(
            [
                (
                    r["stato_monitoraggio"],
                    r["statuscode"] or "",
                    int(r["numero_oggetti"]),
                )
                for r in pivot_rows
            ],
            key=lambda item: (
                STATUS_ORDER.get(item[1], len(STATUS_ORDER) + 1),
                item[1],
            ),
        )
        if any(not stato for stato, _, _ in pivot_data):
            raise RuntimeError("stato_monitoraggio NULL o vuoto nella pivot")
        if sum(n for _, _, n in pivot_data) != summary["oggetti"]:
            raise RuntimeError("Quadratura pivot/dettaglio fallita")
        # P000 rimane visibile nel dettaglio anche senza data di rendicontazione.
        summary["rendicontati"] = df.filter(
            F.col("statuscode").isNotNull()
            & (F.col("statuscode") != "P000")
            & (F.trim(F.coalesce(F.col("clientrequesttimestamp"), F.lit(""))) != "")
        ).count()
        summary["in_attesa"] = summary["oggetti"] - summary["rendicontati"]
        summary["stati_distinti"] = len(pivot_data)
        max_ts = (
            df.filter(F.col("statuscode") != "P000")
            .agg(F.max("clientrequesttimestamp").alias("max_ts"))
            .first()["max_ts"]
        )
        summary["max_data_rendicontazione"] = str(max_ts) if max_ts else "-"
        # P000 e IN_ATTESA_EVENTI non sono conteggiati come eventi rendicontati.
        if (
            df.filter(
                (F.col("stato_monitoraggio") == "AFFIDO_CONSOLIDATORE")
                & ((F.col("statuscode") != "P000") | F.col("statuscode").isNull())
            )
            .limit(1)
            .count()
        ):
            raise RuntimeError("Incoerenza fra P000 e AFFIDO_CONSOLIDATORE")
        if (
            df.filter(
                (F.col("stato_monitoraggio") == "IN_ATTESA_EVENTI")
                & F.col("statuscode").isNotNull()
            )
            .limit(1)
            .count()
        ):
            raise RuntimeError("Stato IN_ATTESA_EVENTI con statuscode valorizzato")
        if (
            df.filter(
                F.col("statuscode").isNotNull()
                & (F.col("statuscode") != "P000")
                & (F.trim(F.coalesce(F.col("clientrequesttimestamp"), F.lit(""))) == "")
            )
            .limit(1)
            .count()
        ):
            raise RuntimeError("Evento postale senza data di rendicontazione")
        log.info(
            "Quadratura: totale=%s, rendicontati=%s, attesa=%s",
            summary["oggetti"],
            summary["rendicontati"],
            summary["in_attesa"],
        )

        detail_pd = (
            df.select(*DETAIL_COLS)
            .orderBy("requestid")
            .toPandas()
            .fillna("")
            .astype(str)
        )
        pivot_pd = pd.DataFrame(pivot_data, columns=PIVOT_COLS)
        pivot_pd.loc[len(pivot_pd)] = ["TOTALE", "", summary["oggetti"]]
        # Mantiene numero_oggetti numerico per la pivot del Google Sheet.
        # Tutti i tab rappresentano l'ultima esecuzione.
        check_spreadsheet_capacity(spreadsheet, len(detail_pd), len(pivot_pd))
        overwrite_tab(creds, tabs[TAB_DETTAGLIO], TAB_DETTAGLIO, detail_pd)
        overwrite_tab(creds, tabs[TAB_RIEPILOGO], TAB_RIEPILOGO, pivot_pd)

        ended = utc_now()
        record.update(summary)
        record.update(
            {
                "fine_utc": utc_fmt(ended),
                "durata_min": f"{(ended - started).total_seconds()/60:.2f}",
                "esito": "OK",
            }
        )
        update_log(log_ws, record)
        log.info("Scrittura dei tre tab completata e verificata")

        # Comunicazione del monitoraggio, distinta dalla notifica tecnica dell'esecuzione.
        user_msg = (
            "*Monitoraggio ComBo Analogiche*\n"
            f"Il monitoraggio giornaliero delle Comunicazioni Bonarie Analogiche "
            f"è consultabile nel <{SLACK_LINK}|Google Sheet Dedicato>.\n"
            f"*Totale Comunicazioni Bonarie Analogiche:* {summary['oggetti']:,}".replace(
                ",", "."
            )
        )
        info_sent = safe_notify(channel_hook, user_msg)
        success_msg = (
            f"*CDE Job Alert* ({APP_NAME})\n"
            f"✅ *SUCCESS* | RunId: {run_id}\n"
            f"Righe: {summary['oggetti']} | Pivot: {summary['stati_distinti']} stati | "
            f"Link: {SLACK_LINK}\n"
            f"Notifica informativa: {'OK' if info_sent else 'NON INVIATA'}"
        )
        ops_sent = safe_notify(ops_hook, success_msg)
        if not info_sent or not ops_sent:
            log.warning(
                "Dataset esportato correttamente, una o piu' notifiche Slack non recapitate"
            )
    except Exception as exc:
        log.exception("SSDA-1771 FAILURE")
        ended = utc_now()
        record.update(summary)
        record.update(
            {
                "fine_utc": utc_fmt(ended),
                "durata_min": f"{(ended - started).total_seconds()/60:.2f}",
                "esito": "KO",
                "dettaglio_errore": str(exc)[:500],
            }
        )
        if log_ws is not None:
            try:
                update_log(log_ws, record)
            except Exception:
                log.exception("Impossibile registrare esito KO nel log GSheet")
        safe_notify(
            ops_hook,
            f"*CDE Job Alert* ({APP_NAME})\n❌ *FAILURE* | RunId: {run_id}\n"
            f"Errore: {str(exc)[:350]}\nLink: {SLACK_LINK}",
        )
        raise
    finally:
        if df is not None:
            try:
                df.unpersist()
            except Exception:
                log.warning("Unpersist fallito", exc_info=True)
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
