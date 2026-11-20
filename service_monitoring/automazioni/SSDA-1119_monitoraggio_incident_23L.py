#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SSDA-1119 - Monitoraggio incident 23L, CDE / PySpark.
La cella 'Log di controllo'!A2 seleziona il CSV nella cartella Drive.
Ogni run: legge input, esegue query SEND, archivia un NUOVO CSV su Drive,
aggiorna soltanto il tab 'Log di controllo' e notifica Slack.
"""

import io
import json
import logging
import math
import os
import re
import time
import traceback
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

APP_NAME = "SSDA-1119_monitoraggio_incident_23L"
GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"
DRIVE_FOLDER_ID = "1eyCtZATX-iGhrtbBsgF4WOS_ZrOANQk2"
SHEET_ID_OUTPUT = "1r0ccGPb55pCQv4UYCjSKZFcYYg6AD-EMAq-qIJ-n6mA"
TAB_META = "Log di controllo"
TAB_OUTPUT = "Estrazione"
DEFAULT_INPUT_FILENAME = "SNDA-2684_Analisi esiti.csv"
SLACK_LABEL = "SSDA-1119"
SLACK_WEBHOOK_NAME = "webhook_1"
SLACK_CONFIG_CANDIDATE_PATHS = [
    "/Slack/notifications_webhook.txt",
    "/app/mount/Slack/notifications_webhook.txt",
    "/app/mount/notifications_webhook.txt",
]
CSV_SEPARATOR = ";"
DRIVE_UPLOAD_CHUNK_BYTES = 5 * 1024 * 1024  # Caricamento resumable a blocchi da 5 MiB
DRIVE_UPLOAD_RETRIES = 4
ROME = ZoneInfo("Europe/Rome")
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
LOG = logging.getLogger(APP_NAME)

QUERY_SQL = r"""
WITH input_requestid AS (
    SELECT DISTINCT requestid
    FROM input_requestids
),
g_filtrata AS (
    SELECT
        g.*,
        CASE
            WHEN g.codice_oggetto LIKE 'R14%'
                THEN 'Fulmine'
            WHEN g.codice_oggetto LIKE '777%'
              OR g.codice_oggetto LIKE 'PSTAQ777%'
                THEN 'POST & SERVICE'
            WHEN g.codice_oggetto LIKE '211%'
                THEN 'RTI Sailpost-Snem'
            WHEN g.codice_oggetto LIKE '69%'
              OR g.codice_oggetto LIKE '381%'
              OR g.codice_oggetto LIKE 'RB1%'
                THEN 'Poste'
            ELSE g.recapitista_unificato
        END AS recapitista_unif
    FROM send.gold_postalizzazione_analytics g
    INNER JOIN input_requestid r
        ON g.requestid = r.requestid
),
iun_filtrati AS (
    SELECT DISTINCT iun
    FROM g_filtrata
),
n_filtrata AS (
    SELECT
        n.iun,
        n.senderpaid,
        n.type_notif,
        n.sentat,
        n.actual_status,
        n.tms_viewed,
        n.tms_effective_date,
        n.tms_date_payment AS tms_payment_gold,
        n.tms_recipient_deceased
    FROM send.gold_notification_analytics n
    INNER JOIN iun_filtrati i
        ON n.iun = i.iun
),
s_filtrata AS (
    SELECT
        s.iun,
        MAX(s.senderdenomination) AS senderdenomination
    FROM send.silver_notification s
    INNER JOIN iun_filtrati i
        ON s.iun = i.iun
    GROUP BY s.iun
),
tl_sched AS (
    SELECT DISTINCT
        t.iun
    FROM send.silver_timeline t
    INNER JOIN iun_filtrati i
        ON t.iun = i.iun
    WHERE t.category = 'SCHEDULE_REFINEMENT'
),
payment_timeline AS (
    SELECT
        t.iun,
        MAX(
            CAST(
                get_json_object(
                    t.details,
                    '$.eventTimestamp'
                ) AS TIMESTAMP
            )
        ) AS tms_payment_silver
    FROM send.silver_timeline t
    INNER JOIN iun_filtrati i
        ON t.iun = i.iun
    WHERE t.category = 'PAYMENT'
    GROUP BY t.iun
),
dup AS (
    SELECT DISTINCT
        w.requestid
    FROM send_dev.wi7_poste_da_escludere w
    INNER JOIN input_requestid r
        ON w.requestid = r.requestid
    WHERE w.tavolo_di_gestione = 'duplicati_23l_23i'
),
nrp AS (
    SELECT DISTINCT
        w.requestid
    FROM send_dev.wi7_poste_da_escludere w
    INNER JOIN input_requestid r
        ON w.requestid = r.requestid
    WHERE w.tavolo_di_gestione = 'non_rendicontabili_puri'
),
be AS (
    SELECT DISTINCT
        b.requestid
    FROM send_dev.report_bonifica_esiti b
    INNER JOIN input_requestid r
        ON b.requestid = r.requestid
),
base AS (
    SELECT
        COALESCE(g.senderpaid, n.senderpaid) AS senderpaid,
        s.senderdenomination,
        g.iun,
        g.requestid,
        g.recindex_number,
        g.pcretry_number,
        g.attempt_number,
        g.pcretry_rank,
        g.attempt_rank,
        n.sentat,
        g.data_deposito,
        n.type_notif,
        n.actual_status,
        n.tms_viewed,
        n.tms_effective_date,
        n.tms_recipient_deceased,
        n.tms_payment_gold,
        pt.tms_payment_silver,
        CASE
            WHEN n.tms_payment_gold IS NOT NULL
             AND pt.tms_payment_silver IS NOT NULL
                THEN 'Pagamento presente su doppia fonte'
            WHEN n.tms_payment_gold IS NOT NULL
                THEN 'Pagamento presente su gold_notification'
            WHEN pt.tms_payment_silver IS NOT NULL
                THEN 'Pagamento presente su silver_timeline'
            ELSE 'Nessuna evidenza pagamento'
        END AS stato_pagamento,
        g.requesttimestamp,
        g.prodotto,
        CONCAT("'", g.geokey) AS cap,
        g.recapitista_unif AS recapitista,
        g.lotto,
        g.region_name AS regione,
        g.province_name AS provincia,
        g.codice_oggetto,
        CONCAT("'", g.codice_oggetto) AS codiceOggetto,
        g.prezzo_ente,
        g.grammatura,
        g.numero_pagine,
        g.costo_consolidatore,
        g.costo_recapitista,
        g.affido_consolidatore_data,
        g.stampa_imbustamento_con080_data,
        g.materialita_pronta_con09a_data,
        g.scarto_consolidatore_stato,
        g.scarto_consolidatore_data,
        g.affido_recapitista_con016_data,
        g.accettazione_recapitista_con018_data,
        SUBSTRING(
            CAST(g.accettazione_recapitista_con018_data AS STRING),
            1,
            7
        ) AS mese_anno_accettazione,
        g.tentativo_recapito_stato,
        g.tentativo_recapito_data,
        g.tentativo_recapito_data_rendicontazione,
        g.messaingiacenza_recapito_stato,
        g.messaingiacenza_recapito_data,
        g.messaingiacenza_recapito_data_rendicontazione,
        g.certificazione_recapito_stato,
        g.certificazione_recapito_dettagli,
        g.certificazione_recapito_data,
        g.certificazione_recapito_data_rendicontazione,
        g.demat_23l_ar_stato,
        g.demat_23l_ar_path,
        g.demat_23l_ar_data_rendicontazione,
        g.demat_plico_stato,
        g.demat_plico_path,
        g.demat_plico_data_rendicontazione,
        g.demat_arcad_stato,
        g.demat_arcad_path,
        g.demat_arcad_data_rendicontazione,
        g.fine_recapito_stato,
        g.fine_recapito_data,
        g.fine_recapito_data_rendicontazione,
        g.accettazione_23l_recag012_data,
        g.accettazione_23l_recag012_data_rendicontazione,
        g.causa_forza_maggiore_dettagli,
        g.causa_forza_maggiore_data,
        g.causa_forza_maggiore_data_rendicontazione,
        g.ultimo_evento_stato,
        g.ultimo_evento_data,
        g.ultimo_evento_data_rendicontazione,
        g.flag_wi7_consolidatore,
        g.flag_wi7_report_postalizzazioni_incomplete,
        g.wi7_cluster,
        CASE
            WHEN tl.iun IS NOT NULL THEN 1
            ELSE 0
        END AS flag_schedule_refinement,
        g.perfezionamento_data,
        g.perfezionamento_tipo,
        g.perfezionamento_notificationdate,
        g.perfezionamento_stato,
        g.perfezionamento_stato_dettagli,
        LEAST(
            COALESCE(
                n.tms_viewed,
                n.tms_effective_date
            ),
            COALESCE(
                n.tms_effective_date,
                n.tms_viewed
            )
        ) AS tms_perfezionamento_notification,
        g.tms_cancelled,
        g.statusrequest,
        g.stato_richiesta,
        g.flag_prodotto_estero,
        CASE
            WHEN dup.requestid IS NOT NULL THEN 1
            ELSE 0
        END AS flag_duplicati_poste,
        CASE
            WHEN nrp.requestid IS NOT NULL THEN 1
            ELSE 0
        END AS flag_non_rendicontabili_puri,
        CASE
            WHEN g.flag_wi7_report_postalizzazioni_incomplete = 0
             AND g.fine_recapito_stato IS NULL
                THEN 1
            ELSE 0
        END AS flag_in_corso,
        CASE
            WHEN be.requestid IS NOT NULL THEN 1
            ELSE 0
        END AS flag_esiti_da_bonificare
    FROM g_filtrata g
    LEFT JOIN n_filtrata n
        ON g.iun = n.iun
    LEFT JOIN s_filtrata s
        ON g.iun = s.iun
    LEFT JOIN tl_sched tl
        ON g.iun = tl.iun
    LEFT JOIN payment_timeline pt
        ON g.iun = pt.iun
    LEFT JOIN dup
        ON g.requestid = dup.requestid
    LEFT JOIN nrp
        ON g.requestid = nrp.requestid
    LEFT JOIN be
        ON g.requestid = be.requestid
),
base_use_case AS (
    SELECT
        b.*,
        CASE
            WHEN b.prodotto IN ('RIR', 'RIS')
             AND b.stato_richiesta = '04_Giacenza in Corso'
                THEN 'Avviato paese estero'
            WHEN b.stato_richiesta = '07_Non Inviati'
                THEN 'Accettazione recapitista'
            WHEN b.stato_richiesta = '09_Altro'
             AND b.fine_recapito_stato IN (
                    'RECRS006',
                    'RECRS013',
                    'RECRN006',
                    'RECRN013',
                    'RECAG004',
                    'RECAG013',
                    'RECRI005',
                    'RECRSI005'
                )
                THEN 'Furto o non rendicontabile'
            WHEN b.stato_richiesta = '09_Altro'
             AND b.fine_recapito_stato IS NULL
                THEN 'In affido recapitista'
            ELSE SUBSTR(
                b.stato_richiesta,
                4
            )
        END AS use_case_recapito
    FROM base b
)
SELECT
    b.*,
    CASE
        WHEN b.tms_perfezionamento_notification IS NOT NULL
            THEN '08_Oggetto perfezionato'
        WHEN b.tms_cancelled IS NOT NULL
            THEN '00_Oggetto cancellato'
        WHEN b.flag_schedule_refinement = 1
            THEN '07_Oggetto in previsione di perfezionamento'
        WHEN b.statusrequest IN ('PN999', 'PN998')
          OR b.fine_recapito_stato IN ('PN999', 'PN998')
          OR b.tentativo_recapito_stato IN ('PN999', 'PN998')
          OR b.certificazione_recapito_stato IN ('PN999', 'PN998')
            THEN '00_Oggetto bloccato con PN998 / PN999'
        WHEN b.statusrequest IN (
                'error',
                'internalError',
                'syntaxError',
                'transformationError',
                'semanticError',
                'authenticationError',
                'duplicatedRequest'
            )
            THEN '00_Oggetto scartato con eventi di errore'
        WHEN b.scarto_consolidatore_stato IS NOT NULL
            THEN '00_Oggetto scartato dal Consolidatore'
        WHEN b.fine_recapito_stato IS NOT NULL
            THEN '06_Chiusura fascicolo'
        WHEN b.demat_23l_ar_stato IS NOT NULL
          OR b.demat_plico_stato IS NOT NULL
            THEN '05_Dematerializzazione'
        WHEN b.certificazione_recapito_stato IS NOT NULL
            THEN '04_Tentativo di recapito - Preesito'
        WHEN b.tentativo_recapito_stato IS NOT NULL
         AND b.flag_prodotto_estero = 0
            THEN '03_Tentativo di recapito - Inesito'
        WHEN b.tentativo_recapito_stato IS NOT NULL
         AND b.flag_prodotto_estero = 1
            THEN '03_Avviato all estero'
        WHEN b.affido_recapitista_con016_data IS NOT NULL
          OR b.accettazione_recapitista_con018_data IS NOT NULL
            THEN '02_Accettazione Recapitista'
        ELSE '01_In affido Recapitista'
    END AS stato_avanzamento_postalizzazione
FROM base_use_case b
"""

INPUT_COLUMNS = [
    "id_richiesta",
    "data_inserimento",
    "codice_documento",
    "nome_stampatore",
    "codice_lotto_cliente",
    "receiver_cap",
    "destinazione",
    "prenotazione_id",
]
META_COLUMNS = [
    "Nome file input",
    "Data esecuzione (Europe/Rome)",
    "Stato run",
    "Durata run (min)",
    "Requestid distinti input",
    "Righe estrazione",
]
EXTRA_COLUMNS = [
    "data_inserimento",
    "codice_documento",
    "nome_stampatore",
    "codice_lotto_cliente",
    "receiver_cap",
    "destinazione",
    "prenotazione_id",
]


def load_google_credentials():
    data = {}
    for name in os.listdir(GOOGLE_SECRET_PATH):
        path = os.path.join(GOOGLE_SECRET_PATH, name)
        if os.path.isfile(path):
            with open(path, encoding="utf-8") as f:
                data[name] = f.read().strip()
    # Non loggare mai chiavi private.
    return service_account.Credentials.from_service_account_info(data, scopes=SCOPES)


def google_clients(credentials):
    sheets = build("sheets", "v4", credentials=credentials, cache_discovery=False)
    drive = build("drive", "v3", credentials=credentials, cache_discovery=False)
    return sheets, drive


def run_api(call, retries=4):
    from googleapiclient.errors import HttpError

    for attempt in range(retries):
        try:
            return call.execute()
        except HttpError as exc:
            if (
                exc.resp.status not in (429, 500, 502, 503, 504)
                or attempt == retries - 1
            ):
                raise
            pause = 2**attempt
            LOG.warning("Google API HTTP %s; ritento fra %s s", exc.resp.status, pause)
            time.sleep(pause)


def tab_metadata(sheets):
    result = run_api(
        sheets.spreadsheets().get(
            spreadsheetId=SHEET_ID_OUTPUT,
            fields="sheets(properties(sheetId,title,gridProperties))",
        )
    )
    return {s["properties"]["title"]: s["properties"] for s in result["sheets"]}


def add_missing_tabs(sheets):
    existing = tab_metadata(sheets)
    requests = []
    for title in (TAB_META,):
        if title not in existing:
            requests.append(
                {
                    "addSheet": {
                        "properties": {
                            "title": title,
                            "gridProperties": {
                                "rowCount": 2,
                                "columnCount": len(META_COLUMNS),
                                "frozenRowCount": 0,
                            },
                        }
                    }
                }
            )
    if requests:
        run_api(
            sheets.spreadsheets().batchUpdate(
                spreadsheetId=SHEET_ID_OUTPUT, body={"requests": requests}
            )
        )
    return tab_metadata(sheets), {
        r["addSheet"]["properties"]["title"] for r in requests
    }


def ensure_grid(sheets, props, min_rows, min_cols):
    g = props["gridProperties"]
    rows = max(g.get("rowCount", 0), min_rows)
    cols = max(g.get("columnCount", 0), min_cols)
    if rows == g.get("rowCount") and cols == g.get("columnCount"):
        return
    run_api(
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID_OUTPUT,
            body={
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": props["sheetId"],
                                "gridProperties": {
                                    "rowCount": rows,
                                    "columnCount": cols,
                                },
                            },
                            "fields": "gridProperties.rowCount,gridProperties.columnCount",
                        }
                    }
                ]
            },
        )
    )


def write_values(sheets, tab, start_cell, values):
    if not values:
        return
    # La Sheets API rifiuta NaN/Infinity: convertiamo ogni cella in JSON valido.
    clean_values = []
    for row in values:
        clean_row = []
        for value in row:
            if (
                value is None
                or value is pd.NaT
                or value is pd.NA
                or (
                    isinstance(value, (float, np.floating)) and not math.isfinite(value)
                )
            ):
                clean_row.append("")
            elif isinstance(value, (np.integer,)):
                clean_row.append(int(value))
            elif isinstance(value, np.floating):
                clean_row.append(float(value))
            elif isinstance(value, (datetime, pd.Timestamp)):
                clean_row.append(str(value))
            elif not isinstance(value, (str, int, float, bool)) and pd.isna(value):
                clean_row.append("")
            else:
                clean_row.append(value)
        clean_values.append(clean_row)
    body = {"values": clean_values}
    json.dumps(body, allow_nan=False)  # Controllo locale prima della chiamata Google.
    run_api(
        sheets.spreadsheets()
        .values()
        .update(
            spreadsheetId=SHEET_ID_OUTPUT,
            range=f"'{tab}'!{start_cell}",
            valueInputOption="RAW",
            body=body,
        )
    )


def header_format(sheet_id, start_row, end_row, start_col, end_col, rgb):
    return {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row,
                "endRowIndex": end_row,
                "startColumnIndex": start_col,
                "endColumnIndex": end_col,
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": rgb,
                    "textFormat": {"bold": True, "italic": True},
                    "verticalAlignment": "MIDDLE",
                }
            },
            "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.verticalAlignment",
        }
    }


def format_headers(sheets, meta_props, output_props=None):
    violet = {"red": 0.85, "green": 0.78, "blue": 0.96}
    yellow = {"red": 1.0, "green": 0.93, "blue": 0.55}
    req = [
        header_format(meta_props["sheetId"], 0, 1, 0, len(META_COLUMNS), violet),
        header_format(meta_props["sheetId"], 0, 1, 0, 1, yellow),
        {
            "repeatCell": {
                "range": {
                    "sheetId": meta_props["sheetId"],
                    "startRowIndex": 1,
                    "endRowIndex": 2,
                    "startColumnIndex": 0,
                    "endColumnIndex": 1,
                },
                "cell": {"userEnteredFormat": {"backgroundColor": yellow}},
                "fields": "userEnteredFormat.backgroundColor",
            }
        },
    ]
    if output_props is not None:
        req.append(header_format(output_props["sheetId"], 0, 1, 0, 1, violet))
    run_api(
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID_OUTPUT, body={"requests": req}
        )
    )


def resize_control_grid(sheets, meta):
    # Il controllo occupa esattamente due righe e sei colonne, senza storico.
    g = meta["gridProperties"]
    if g.get("rowCount") == 2 and g.get("columnCount") == len(META_COLUMNS):
        return
    run_api(
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID_OUTPUT,
            body={
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": meta["sheetId"],
                                "gridProperties": {
                                    "rowCount": 2,
                                    "columnCount": len(META_COLUMNS),
                                },
                            },
                            "fields": "gridProperties.rowCount,gridProperties.columnCount",
                        }
                    }
                ]
            },
        )
    )


def initialize_control(sheets, props, created):
    meta = props[TAB_META]
    result = run_api(
        sheets.spreadsheets()
        .values()
        .get(spreadsheetId=SHEET_ID_OUTPUT, range=f"'{TAB_META}'!A1:F5")
    )
    cells = result.get("values", [])
    first = cells[0] if cells else []
    new_layout = first == META_COLUMNS
    if new_layout:
        # Non modificare il parametro A2 impostato manualmente.
        filename = cells[1][0].strip() if len(cells) > 1 and cells[1] else ""
    else:
        # Migrazione automatica dal vecchio schema A1=etichetta, B1=nome CSV.
        filename = (
            str(first[1]).strip()
            if len(first) > 1 and first[0] == "Nome file input"
            else DEFAULT_INPUT_FILENAME
        )
        # Scrittura del nuovo schema prima della riduzione della griglia.
        run_api(
            sheets.spreadsheets()
            .values()
            .clear(spreadsheetId=SHEET_ID_OUTPUT, range=f"'{TAB_META}'!A1:F5", body={})
        )
        write_values(sheets, TAB_META, "A1", [META_COLUMNS, [filename]])
        format_headers(sheets, meta)
    resize_control_grid(sheets, meta)
    if (
        not filename
        or not filename.lower().endswith(".csv")
        or "/" in filename
        or "\\" in filename
    ):
        raise ValueError("Nome CSV non valido in 'Log di controllo'!A2")
    return filename


def write_status(sheets, start_dt, state, input_count, output_count):
    end = datetime.now(ROME)
    duration = round((end - start_dt).total_seconds() / 60, 2)
    # Aggiorna solo B2:F2: il nome input A2 rimane sempre modificabile.
    write_values(
        sheets,
        TAB_META,
        "B2",
        [
            [
                start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                state,
                duration,
                input_count,
                output_count,
            ]
        ],
    )
    return duration


def get_input_csv(drive, filename):
    safe = filename.replace("\\", "\\\\").replace("'", "\\'")
    query = f"'{DRIVE_FOLDER_ID}' in parents and name = '{safe}' and trashed = false"
    found = run_api(
        drive.files().list(
            q=query,
            fields="nextPageToken,files(id,name,mimeType,size)",
            pageSize=100,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
    )
    files = found.get("files", [])
    if found.get("nextPageToken") or len(files) != 1:
        raise ValueError(
            f"CSV '{filename}': atteso esattamente un file nella cartella, trovati {len(files)} (o ulteriori pagine)"
        )
    if files[0].get("mimeType") == "application/vnd.google-apps.spreadsheet":
        raise ValueError(
            "Il file di input deve essere un vero CSV e non un Google Sheet"
        )
    buf = io.BytesIO()
    download = MediaIoBaseDownload(
        buf, drive.files().get_media(fileId=files[0]["id"], supportsAllDrives=True)
    )
    done = False
    while not done:
        _, done = download.next_chunk()
    buf.seek(0)
    df = pd.read_csv(
        buf, sep=CSV_SEPARATOR, dtype=str, keep_default_na=False, encoding="utf-8-sig"
    )
    df.columns = df.columns.str.strip()
    missing = [c for c in INPUT_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"CSV senza colonne obbligatorie: {missing}. Separator atteso: ';'"
        )
    return df


def make_input_views(spark, df):
    selected = df[INPUT_COLUMNS].copy().rename(columns={"id_richiesta": "requestid"})
    for col in selected.columns:
        selected[col] = selected[col].astype(str).str.strip()
    selected = selected[selected["requestid"] != ""].copy()
    if selected.empty:
        raise ValueError("Nessun requestid valido nel file CSV")
    count_distinct = int(selected["requestid"].nunique())
    dup_count = len(selected) - count_distinct
    if dup_count:
        LOG.warning(
            "Trovate %s righe duplicate per requestid nel CSV: si conserva la prima occorrenza",
            dup_count,
        )
    selected = selected.drop_duplicates(subset=["requestid"], keep="first")
    df_spark = spark.createDataFrame(selected)
    df_spark.createOrReplaceTempView("input_analisi_esiti")
    df_spark.select("requestid").distinct().createOrReplaceTempView("input_requestids")
    return count_distinct


def check_unique_requestids(df):
    if "requestid" not in df.columns:
        raise ValueError("La query non restituisce requestid")
    duplicates = df.groupBy("requestid").count().filter("count > 1").limit(5).collect()
    if duplicates:
        raise ValueError(
            "Query SEND non univoca per requestid, esempi: "
            + ", ".join(str(x["requestid"]) for x in duplicates)
        )


def output_to_pandas(spark, expected_distinct):
    sql_df = spark.sql(QUERY_SQL).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        check_unique_requestids(sql_df)
        in_df = spark.table("input_analisi_esiti")
        enriched = in_df.join(sql_df, on="requestid", how="left")
        original_fields = ["requestid"] + EXTRA_COLUMNS
        send_fields = [c for c in sql_df.columns if c != "requestid"]
        enriched = enriched.select(*(original_fields + send_fields))
        actual_count = enriched.count()
        if actual_count != expected_distinct:
            raise ValueError(
                f"Cardinalità errata: {actual_count} righe vs {expected_distinct} requestid input"
            )
        # Stessa sanitizzazione di SSDA-510: elimina NaN, NaT e +/-Infinity
        # PRIMA della conversione a stringa, evitando payload JSON non validi.
        result = enriched.toPandas()
        result = result.replace([np.inf, -np.inf], np.nan).fillna("")
        result = result.astype(str)
        return result
    finally:
        sql_df.unpersist()


def archive_csv(drive, output, filename, start_dt):
    """Crea un nuovo CSV su Drive, con upload resumable a blocchi e retry di rete.

    A differenza di ``request.execute()`` su un upload grande, ``next_chunk``
    permette di riprovare l'invio del blocco in caso di connessione interrotta.
    Non modifica o elimina gli archivi preesistenti.
    """
    archived_name = f"{start_dt.strftime('%Y%m%d')}_monitoraggio_{filename}"
    data = output.to_csv(index=False, sep=CSV_SEPARATOR).encode("utf-8-sig")
    media = MediaIoBaseUpload(
        io.BytesIO(data),
        mimetype="text/csv",
        chunksize=DRIVE_UPLOAD_CHUNK_BYTES,
        resumable=True,
    )
    metadata = {
        "name": archived_name,
        "parents": [DRIVE_FOLDER_ID],
        "mimeType": "text/csv",
    }
    request = drive.files().create(
        body=metadata,
        media_body=media,
        fields="id,name,webViewLink",
        supportsAllDrives=True,
    )
    LOG.info(
        "Upload CSV verso Drive: %s (%.2f MiB)", archived_name, len(data) / 1024**2
    )
    uploaded = None
    consecutive_errors = 0
    while uploaded is None:
        try:
            progress, uploaded = request.next_chunk(num_retries=3)
            consecutive_errors = 0
            if progress is not None:
                LOG.info("Upload CSV Drive: %.1f%%", progress.progress() * 100)
        except (OSError, ConnectionError) as exc:
            consecutive_errors += 1
            if consecutive_errors >= DRIVE_UPLOAD_RETRIES:
                raise
            pause = 2 ** (consecutive_errors - 1)
            LOG.warning(
                "Connessione Drive interrotta (%s): retry blocco %s/%s tra %s s",
                type(exc).__name__,
                consecutive_errors,
                DRIVE_UPLOAD_RETRIES,
                pause,
            )
            time.sleep(pause)
    LOG.info("Archivio CSV creato: %s (fileId=%s)", archived_name, uploaded["id"])
    return uploaded


def get_slack_webhook():
    for path in SLACK_CONFIG_CANDIDATE_PATHS:
        if not os.path.isfile(path):
            continue
        with open(path, encoding="utf-8") as f:
            for raw in f:
                raw = raw.strip()
                if not raw or raw.startswith("#") or "=" not in raw:
                    continue
                key, value = raw.split("=", 1)
                if key.strip() == SLACK_WEBHOOK_NAME:
                    return value.strip()
    LOG.warning("Webhook Slack %s non trovato", SLACK_WEBHOOK_NAME)
    return None


def send_slack(webhook, message):
    if not webhook:
        return
    request = urllib.request.Request(
        webhook,
        data=json.dumps({"text": message}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        response.read()


def notify_safe(webhook, message):
    try:
        send_slack(webhook, message)
    except Exception:
        LOG.warning("Notifica Slack non riuscita", exc_info=True)


def main():
    start = datetime.now(ROME)
    creds = None
    sheets = None
    filename = "N/D"
    count_input = 0
    count_output = 0
    webhook = get_slack_webhook()
    spark = None
    try:
        creds = load_google_credentials()
        sheets, drive = google_clients(creds)
        props, created = add_missing_tabs(sheets)
        filename = initialize_control(sheets, props, created)
        write_status(sheets, start, "IN CORSO", "-", "-")
        LOG.info("File input selezionato dal GSheet: %s", filename)

        file_pd = get_input_csv(drive, filename)
        spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
        count_input = make_input_views(spark, file_pd)
        LOG.info("Requestid distinti input: %s", count_input)
        output_pd = output_to_pandas(spark, count_input)
        count_output = len(output_pd)

        # La query può richiedere minuti: ricreiamo i client Google per evitare
        # il riuso di connessioni HTTP rimaste inattive durante il lavoro Spark.
        sheets, drive = google_clients(creds)
        # Solo CSV su Drive: NESSUNA scrittura del dettaglio nel tab Estrazione.
        archived = archive_csv(drive, output_pd, filename, start)
        duration = write_status(sheets, start, "OK", count_input, count_output)
        notify_safe(
            webhook,
            "\n".join(
                [
                    f"*CDE Job Alert* ({SLACK_LABEL})",
                    "✅ *SUCCESS*",
                    f"*Job:* {APP_NAME}",
                    f"*CSV input:* {filename}",
                    f"*Requestid distinti input:* {count_input}",
                    f"*Righe estrazione:* {count_output}",
                    f"*Durata:* {duration:.2f} min",
                    f"*CSV archivio:* {archived.get('webViewLink') or 'ID ' + archived['id']}",
                ]
            ),
        )
        LOG.info("Job completato correttamente")
    except Exception as exc:
        LOG.exception("Job fallito")
        if sheets is not None:
            try:
                write_status(
                    sheets, start, "KO", count_input or "-", count_output or "-"
                )
            except Exception:
                LOG.warning("Impossibile scrivere KO nel log GSheet", exc_info=True)
        notify_safe(
            webhook,
            "\n".join(
                [
                    f"*CDE Job Alert* ({SLACK_LABEL})",
                    "❌ *FAILURE*",
                    f"*Job:* {APP_NAME}",
                    f"*CSV input:* {filename}",
                    f"*Errore:* {type(exc).__name__}: {str(exc)[:500]}",
                ]
            ),
        )
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
