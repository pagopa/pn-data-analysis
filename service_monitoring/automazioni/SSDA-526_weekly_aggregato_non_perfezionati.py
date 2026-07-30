from __future__ import annotations

import logging
import math
import os
import re
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple
from zoneinfo import ZoneInfo

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from matplotlib import font_manager
from pdnd_google_utils import Sheet
from PIL import Image
from pyspark.sql import SparkSession

# =============================================================================
# CONFIGURAZIONE
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"
SHEET_ID = "1qfcFKvg2tSHXkyHdoV9UDufio4Z5BumZC9ZlCvFlScs"
DRIVE_FOLDER_ID = "1jbcnv2G99avZvn_gPueaigYKXnEcYN2Z"

CURRENT_SHEET_NAME = "Estrazione weekly SSDA-526"
HISTORY_SHEET_NAME = "Storico weekly SSDA-526"
UPDATE_SHEET_NAME = "Log di controllo"

OUTPUT_DIR = Path("/tmp/ssda526_outputs")
OUTPUT_FILES = {
    "table": "SSDA-526_tabella_latest.png",
    "legend": "SSDA-526_colonna_colori_latest.png",
    "chart": "SSDA-526_grafico_latest.png",
    "combined": "SSDA-526_completo_latest.png",
}

ROME_TZ = ZoneInfo("Europe/Rome")
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]

DELTA_GREEN = "#17823B"
DELTA_RED = "#C62828"
DELTA_NEUTRAL = "#4A4A4A"
HEADER_BG = "#008593"
TOTAL_BG = "#00b3c3"
HEADER_TEXT_COLOR = "#FFFFFF"
TOTAL_TEXT_COLOR = "#FFFFFF"
GRID_COLOR = "#B8C2CC"
TEXT_COLOR = "#263238"
OTHER_COLOR = "#D8C6E8"

PERIOD_COLUMN = "Data max deposito"
LEGACY_PERIOD_COLUMN = "periodo_riferimento"
FONT_FAMILY = "Montserrat"
BASE_FONT_SIZE = 10.5
NUMERIC_FONT_SIZE = 11.0
TABLE_FIGSIZE = (10.8, 7.2)
LEGEND_FIGSIZE = (0.8, 7.2)
IMAGE_DPI = 180
MONTSERRAT_FONT_PATH_ENV = "MONTSERRAT_FONT_PATH"

# Label, nota e colore sono modificabili senza intervenire sulla logica SQL o sullo storico.

CLUSTER_META: Dict[str, Dict[str, str]] = {
    "Errore rendicontazione/assenza eventi intermedi": {
        "label": "Oggetti con errori rendicontazione/assenza eventi intermedi",
        "footnote": "*",
        "color": "#c47182",
    },
    "Oggetto bloccato al primo attempt": {
        "label": "Oggetti bloccati al primo attempt",
        "footnote": "",
        "color": "#F6D58A",
    },
    "Oggetto con rendicontazione corretta da perfezionare": {
        "label": "Oggetti con rendicontazione corretta da perfezionare",
        "footnote": "",
        "color": "#8FAADC",
    },
    "Oggetto fuori sla": {
        "label": "Oggetti fuori SLA",
        "footnote": "*",
        "color": "#ffab40",
    },
    "Oggetto in corso di postalizzazione": {
        "label": "Oggetti in corso di postalizzazione",
        "footnote": "**",
        "color": "#92d050",
    },
    "Oggetto non perfezionato con evidenza di pagamento": {
        "label": "Oggetti non perfezionati con evidenza di pagamento",
        "footnote": "",
        "color": "#D8A7B1",
    },
    "Oggetto rientrante in tavoli Duplicati / Non Rendicontabili": {
        "label": "Oggetti rientranti in tavoli Duplicati / Non Rendicontabili",
        "footnote": "",
        "color": "#42c3eb",
    },
}

# Cluster storico della precedente classificazione a cinque righe. È mantenuto
# esclusivamente per leggere gli snapshot preesistenti; non viene più prodotto.
LEGACY_CLUSTER_LABELS = {
    "Oggetto con rendicontazione corretta in analisi Team Prodotto": (
        "Oggetti con rendicontazione corretta in analisi Team Prodotto"
    )
}

HISTORY_COLUMNS = [
    "settimana_riferimento",
    "data_inizio_settimana",
    "data_esecuzione_utc",
    PERIOD_COLUMN,
    "cluster_mancato_perfezionamento",
    "numero_oggetti",
    # Metrica temporaneamente disabilitata, mantenuta come riferimento per un
    # eventuale ripristino futuro:
    # "oggetti_pagati",
]


# =============================================================================
# CREDENZIALI E SERVIZI GOOGLE
# =============================================================================


def load_google_credentials(secret_path: str) -> dict:
    """Legge i file del workload secret e restituisce il dizionario credenziali."""
    creds: dict = {}
    for name in os.listdir(secret_path):
        file_path = os.path.join(secret_path, name)
        if os.path.isfile(file_path):
            with open(file_path, "r", encoding="utf-8") as file_obj:
                creds[name] = file_obj.read().strip()

    if not creds:
        raise RuntimeError(f"Nessuna credenziale trovata in {secret_path}")

    logging.info("Credenziali Google caricate: %s", sorted(creds.keys()))
    return creds


def build_drive_service(creds: dict):
    """Crea il client Google Drive usando lo stesso service account del GSheet."""
    service_info = dict(creds)
    private_key = service_info.get("private_key")
    if private_key and "\\n" in private_key and "\n" not in private_key:
        service_info["private_key"] = private_key.replace("\\n", "\n")

    credentials = service_account.Credentials.from_service_account_info(
        service_info,
        scopes=DRIVE_SCOPES,
    )
    return build("drive", "v3", credentials=credentials, cache_discovery=False)


def upload_or_update_drive_file(
    drive_service,
    local_path: Path,
    folder_id: str,
) -> dict:
    """Aggiorna il PNG omonimo nella cartella Drive, oppure lo crea se assente."""
    safe_name = local_path.name.replace("'", "\\'")
    query = f"name = '{safe_name}' and '{folder_id}' in parents " "and trashed = false"

    response = (
        drive_service.files()
        .list(
            q=query,
            spaces="drive",
            fields="files(id,name,webViewLink)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    matches = response.get("files", [])
    media = MediaFileUpload(str(local_path), mimetype="image/png", resumable=False)

    if matches:
        file_id = matches[0]["id"]
        result = (
            drive_service.files()
            .update(
                fileId=file_id,
                media_body=media,
                fields="id,name,webViewLink",
                supportsAllDrives=True,
            )
            .execute()
        )
        logging.info("File Drive aggiornato: %s", local_path.name)
        return result

    metadata = {"name": local_path.name, "parents": [folder_id]}
    result = (
        drive_service.files()
        .create(
            body=metadata,
            media_body=media,
            fields="id,name,webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )
    logging.info("File Drive creato: %s", local_path.name)
    return result


def upload_all_images_to_drive(
    paths: Dict[str, Path],
    creds: dict,
    folder_id: str,
) -> Dict[str, dict]:
    drive_service = build_drive_service(creds)
    uploaded: Dict[str, dict] = {}
    for key, path in paths.items():
        uploaded[key] = upload_or_update_drive_file(drive_service, path, folder_id)
    return uploaded


# =============================================================================
# LETTURA INPUT E QUERY SPARK
# =============================================================================


def validate_period_reference(raw_value: object) -> str:
    """Valida il valore letto dal GSheet prima di inserirlo nella query SQL."""
    value = str(raw_value).strip()
    if not value:
        raise ValueError("La cella A2 (data_max_deposito) è vuota.")

    pattern = re.compile(
        r"^\d{4}-\d{2}-\d{2}"
        r"(?:[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?"
        r"(?:Z|[+-]\d{2}:\d{2})?)?$"
    )
    if not pattern.fullmatch(value):
        raise ValueError(
            "data_max_deposito non valida. Formati ammessi: YYYY-MM-DD oppure "
            "timestamp ISO compatibile."
        )
    return value


def read_period_reference(sheet: Sheet) -> Tuple[str, pd.DataFrame]:
    logging.info("Lettura del periodo dal tab '%s'...", UPDATE_SHEET_NAME)
    df_update = sheet.download(UPDATE_SHEET_NAME)
    if df_update.shape[0] < 1 or df_update.shape[1] < 1:
        raise ValueError(
            f"Il tab '{UPDATE_SHEET_NAME}' non contiene la riga dati attesa."
        )

    period_reference = validate_period_reference(df_update.iloc[0, 0])
    logging.info("Periodo di riferimento letto: %s", period_reference)
    return period_reference, df_update


def run_query(spark: SparkSession, data_max_deposito: str) -> pd.DataFrame:
    """
    Restituisce un aggregato con granularità di una riga per cluster.

    La metrica è COUNT(DISTINCT requestid). Le logiche di classificazione finali
    restano quelle del job originario; l'ottimizzazione consiste nel delimitare
    prima il perimetro GOLD, filtrare le SILVER prima dell'EXPLODE e usare insiemi
    distinti/LEFT ANTI JOIN per evitare moltiplicazioni non necessarie.
    """
    logging.info("Esecuzione query Spark ottimizzata...")

    query_sql = f"""
        WITH gold_perimetro AS (
            SELECT
                s.iun,
                s.requestid,
                s.prodotto,
                s.data_deposito,
                s.tentativo_recapito_stato,
                s.tentativo_recapito_data,
                s.messaingiacenza_recapito_stato,
                s.certificazione_recapito_stato,
                s.certificazione_recapito_dettagli,
                s.certificazione_recapito_data,
                s.demat_23l_ar_stato,
                s.demat_plico_stato,
                s.demat_arcad_stato,
                s.demat_arcad_data_rendicontazione,
                s.fine_recapito_stato,
                s.accettazione_23l_recag012_data,
                s.tms_cancelled,
                s.flag_wi7_consolidatore,
                s.flag_wi7_report_postalizzazioni_incomplete,
                s.attempt_number,
                s.attempt_rank,
                s.pcretry_rank
            FROM send.gold_postalizzazione_analytics s
            WHERE s.scarto_consolidatore_stato IS NULL
              AND s.ultimo_evento_stato NOT IN ('P008', 'P010', 'P011')
              AND s.flag_prodotto_estero = 0
              AND s.statusrequest NOT IN ('PN999', 'PN998')
              AND s.attempt_rank = 1
              AND s.pcretry_rank = 1
              AND s.tms_cancelled IS NULL
              AND (
                    s.certificazione_recapito_stato NOT IN (
                        'RECRS006','RECRS013','RECRN006','RECRN013','RECAG004','RECAG013'
                    )
                    OR s.certificazione_recapito_stato IS NULL
              )
              AND COALESCE(s.tentativo_recapito_stato, '') NOT IN ('PN998','PN999')
              AND COALESCE(s.certificazione_recapito_stato, '') NOT IN ('PN998','PN999')
              AND COALESCE(s.fine_recapito_stato, '') NOT IN ('PN998','PN999')
              AND s.data_deposito < '{data_max_deposito}'
              AND COALESCE(s.certificazione_recapito_dettagli, '') <> 'M02'
        ),

        perimetro_requestid AS (
            SELECT DISTINCT requestid
            FROM gold_perimetro
        ),

        perimetro_iun AS (
            SELECT DISTINCT iun
            FROM gold_perimetro
        ),

        schedule_refinement_iun AS (
            SELECT DISTINCT tl.iun
            FROM send.silver_timeline tl
            INNER JOIN perimetro_iun p
                ON tl.iun = p.iun
            WHERE tl.category = 'SCHEDULE_REFINEMENT'
        ),

        gold_senza_schedule_refinement AS (
            SELECT g.*
            FROM gold_perimetro g
            LEFT ANTI JOIN schedule_refinement_iun sr
                ON g.iun = sr.iun
        ),

        perimetro_requestid_finale AS (
            SELECT DISTINCT requestid
            FROM gold_senza_schedule_refinement
        ),

        perimetro_iun_finale AS (
            SELECT DISTINCT iun
            FROM gold_senza_schedule_refinement
        ),

        temp_analog_attempt AS (
            SELECT DISTINCT
                tl.iun,
                CAST(
                    REGEXP_EXTRACT(tl.timelineelementid, 'ATTEMPT_([0-9]+)', 1)
                    AS INT
                ) AS attempt_number_timeline
            FROM send.silver_timeline tl
            INNER JOIN perimetro_iun_finale p
                ON tl.iun = p.iun
            WHERE tl.category = 'SEND_ANALOG_FEEDBACK'
        ),

        wi7_poste AS (
            SELECT DISTINCT requestid
            FROM send_dev.wi7_poste_da_escludere
        ),

        notification_perimetro AS (
            SELECT
                n.iun,
                n.tms_viewed,
                n.tms_effective_date,
                n.tms_date_payment
            FROM send.gold_notification_analytics n
            INNER JOIN perimetro_iun_finale p
                ON n.iun = p.iun
        ),

        silver_postalizzazione_perimetro AS (
            SELECT
                REGEXP_REPLACE(sp.requestid, '^pn-cons-000~', '') AS requestid,
                sp.eventslist
            FROM send.silver_postalizzazione sp
            INNER JOIN perimetro_requestid_finale p
                ON REGEXP_REPLACE(sp.requestid, '^pn-cons-000~', '') = p.requestid
        ),

        silver_eventi AS (
            SELECT
                sp.requestid,
                e.paperprogrstatus.statuscode AS statuscode,
                CASE
                    WHEN LENGTH(e.paperprogrstatus.statusdatetime) = 17 THEN CONCAT(
                        SUBSTR(e.paperprogrstatus.statusdatetime, 1, 16), ':00Z'
                    )
                    ELSE e.paperprogrstatus.statusdatetime
                END AS statusdatetime,
                CASE
                    WHEN LENGTH(e.paperprogrstatus.clientrequesttimestamp) = 17 THEN CONCAT(
                        SUBSTR(e.paperprogrstatus.clientrequesttimestamp, 1, 16), ':00.000Z'
                    )
                    ELSE e.paperprogrstatus.clientrequesttimestamp
                END AS clientrequesttimestamp,
                CASE
                    WHEN e.paperprogrstatus.statuscode IN (
                        'RECRN003C','RECRN004C','RECRN005C','RECRN001C','RECRN002C','RECRN002F',
                        'RECAG001C','RECAG002C','RECAG003C','RECAG003F','RECAG005C','RECAG006C',
                        'RECAG007C','RECAG008C'
                    ) THEN 'FINE_RECAPITO'
                    WHEN e.paperprogrstatus.statuscode IN (
                        'RECRN003A','RECRN004A','RECRN005A','RECRN001A','RECRN002A','RECRN002D',
                        'RECAG001A','RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A',
                        'RECAG007A','RECAG008A'
                    ) THEN 'CERTIFICAZIONE_RECAPITO'
                    ELSE NULL
                END AS tipo
            FROM silver_postalizzazione_perimetro sp
            LATERAL VIEW EXPLODE(sp.eventslist) ev AS e
            WHERE e.paperprogrstatus.statuscode IN (
                'RECRN001A','RECRN002A','RECRN002D','RECRN003A','RECRN004A','RECRN005A',
                'RECAG001A','RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A',
                'RECAG007A','RECAG008A','RECRN001C','RECRN002C','RECRN002F','RECRN003C',
                'RECRN004C','RECRN005C','RECAG001C','RECAG002C','RECAG003C','RECAG003F',
                'RECAG005C','RECAG006C','RECAG007C','RECAG008C'
            )
        ),

        silver_eventi_ranked AS (
            SELECT
                requestid,
                statuscode,
                statusdatetime,
                clientrequesttimestamp,
                tipo,
                ROW_NUMBER() OVER (
                    PARTITION BY requestid, tipo
                    ORDER BY clientrequesttimestamp DESC
                ) AS rn
            FROM silver_eventi
        ),

        ultimi_eventi_silver AS (
            SELECT
                requestid,
                MAX(CASE WHEN tipo = 'FINE_RECAPITO' THEN statuscode END) AS fine_recapito_stato_silver,
                MAX(CASE WHEN tipo = 'FINE_RECAPITO' THEN statusdatetime END) AS fine_recapito_data_silver,
                MAX(CASE WHEN tipo = 'CERTIFICAZIONE_RECAPITO' THEN statuscode END) AS certificazione_recapito_stato_silver,
                MAX(CASE WHEN tipo = 'CERTIFICAZIONE_RECAPITO' THEN statusdatetime END) AS certificazione_recapito_data_silver
            FROM silver_eventi_ranked
            WHERE rn = 1
            GROUP BY requestid
        ),

        controlli AS (
            SELECT
                g.iun,
                g.requestid,
                g.prodotto,
                g.tentativo_recapito_stato,
                g.tentativo_recapito_data,
                g.messaingiacenza_recapito_stato,
                g.certificazione_recapito_stato,
                g.certificazione_recapito_dettagli,
                g.certificazione_recapito_data,
                g.demat_23l_ar_stato,
                g.demat_plico_stato,
                g.demat_arcad_stato,
                g.demat_arcad_data_rendicontazione,
                g.fine_recapito_stato,
                g.accettazione_23l_recag012_data,
                g.flag_wi7_consolidatore,
                g.flag_wi7_report_postalizzazioni_incomplete,
                n.tms_date_payment,
                CASE WHEN w.requestid IS NOT NULL THEN 1 ELSE 0 END AS flag_wi7_poste,
                LEAST(
                    COALESCE(n.tms_viewed, n.tms_effective_date),
                    COALESCE(n.tms_effective_date, n.tms_viewed)
                ) AS tms_perfezionamento_notification,
                CASE
                    WHEN a.attempt_number_timeline = 0
                     AND CAST(g.attempt_number AS INT) = 0 THEN 1
                    ELSE 0
                END AS flag_feedback_attempt_0,

                CASE
                    WHEN g.certificazione_recapito_stato NOT IN (
                        'RECRS002A','RECRN002A','RECAG003A','RECRS002D','RECRN002D','RECAG003D'
                    )
                    AND g.certificazione_recapito_dettagli IN (
                        'M01','M03','M04','M02','M05','M06','M07','M08','M09'
                    ) THEN 1
                    WHEN g.certificazione_recapito_stato IN (
                        'RECRS002A','RECRN002A','RECAG003A'
                    )
                    AND (
                        g.certificazione_recapito_dettagli NOT IN ('M02','M05','M06','M07','M08','M09')
                        OR g.certificazione_recapito_dettagli IS NULL
                    ) THEN 1
                    WHEN g.certificazione_recapito_stato IN (
                        'RECRS002D','RECRN002D','RECAG003D'
                    )
                    AND (
                        g.certificazione_recapito_dettagli NOT IN ('M01','M03','M04')
                        OR g.certificazione_recapito_dettagli IS NULL
                    ) THEN 1
                    ELSE 0
                END AS controllo_causale,

                CASE
                    WHEN g.certificazione_recapito_stato IN ('RECRN003A','RECRN004A','RECRN005A')
                     AND (g.tentativo_recapito_stato = 'RECRN010' OR g.tentativo_recapito_stato IS NULL) THEN 0
                    WHEN g.certificazione_recapito_stato IN ('RECRS003A','RECRS004A','RECRS005A')
                     AND (g.tentativo_recapito_stato = 'RECRS010' OR g.tentativo_recapito_stato IS NULL) THEN 0
                    WHEN g.certificazione_recapito_stato IN ('RECAG005A','RECAG006A','RECAG007A','RECAG008A')
                     AND (g.tentativo_recapito_stato = 'RECAG010' OR g.tentativo_recapito_stato IS NULL) THEN 0
                    WHEN g.certificazione_recapito_stato NOT IN (
                        'RECRN003A','RECRN004A','RECRN005A','RECAG005A','RECRS003A',
                        'RECRS004A','RECRS005A','RECAG006A','RECAG007A','RECAG008A'
                    )
                     AND g.tentativo_recapito_stato = g.certificazione_recapito_stato THEN 0
                    ELSE 1
                END AS controllo_inesito_casi_giacenza,

                CASE
                    WHEN g.certificazione_recapito_stato = 'RECRN001A'
                     AND (g.demat_23l_ar_stato = 'RECRN001B' OR g.demat_plico_stato = 'RECRN001B' OR g.demat_arcad_stato = 'RECRN001B')
                     AND s.fine_recapito_stato_silver = 'RECRN001C' THEN 0
                    WHEN g.certificazione_recapito_stato = 'RECRN002A'
                     AND (g.demat_23l_ar_stato = 'RECRN002B' OR g.demat_plico_stato = 'RECRN002B' OR g.demat_arcad_stato = 'RECRN002B')
                     AND s.fine_recapito_stato_silver = 'RECRN002C' THEN 0
                    WHEN g.certificazione_recapito_stato = 'RECRN002D'
                     AND (g.demat_23l_ar_stato = 'RECRN002E' OR g.demat_plico_stato = 'RECRN002E' OR g.demat_arcad_stato = 'RECRN002E')
                     AND s.fine_recapito_stato_silver = 'RECRN002F' THEN 0
                    WHEN g.certificazione_recapito_stato = 'RECRN003A'
                     AND (g.demat_23l_ar_stato = 'RECRN003B' OR g.demat_plico_stato = 'RECRN003B' OR g.demat_arcad_stato = 'RECRN003B')
                     AND s.fine_recapito_stato_silver = 'RECRN003C' THEN 0
                    WHEN g.certificazione_recapito_stato = 'RECRN004A'
                     AND (g.demat_23l_ar_stato = 'RECRN004B' OR g.demat_plico_stato = 'RECRN004B' OR g.demat_arcad_stato = 'RECRN004B')
                     AND s.fine_recapito_stato_silver = 'RECRN004C' THEN 0
                    WHEN g.certificazione_recapito_stato = 'RECRN005A'
                     AND (g.demat_23l_ar_stato = 'RECRN005B' OR g.demat_plico_stato = 'RECRN005B' OR g.demat_arcad_stato = 'RECRN005B')
                     AND s.fine_recapito_stato_silver = 'RECRN005C' THEN 0
                    WHEN g.certificazione_recapito_stato = 'RECAG001A'
                     AND (g.demat_23l_ar_stato = 'RECAG001B' OR g.demat_plico_stato = 'RECAG001B' OR g.demat_arcad_stato = 'RECAG001B')
                     AND s.fine_recapito_stato_silver = 'RECAG001C' THEN 0
                    WHEN g.certificazione_recapito_stato = 'RECAG002A'
                     AND (g.demat_23l_ar_stato = 'RECAG002B' OR g.demat_plico_stato = 'RECAG002B' OR g.demat_arcad_stato = 'RECAG002B')
                     AND s.fine_recapito_stato_silver = 'RECAG002C' THEN 0
                    WHEN g.certificazione_recapito_stato = 'RECAG003A'
                     AND (g.demat_23l_ar_stato = 'RECAG003B' OR g.demat_plico_stato = 'RECAG003B' OR g.demat_arcad_stato = 'RECAG003B')
                     AND s.fine_recapito_stato_silver = 'RECAG003C' THEN 0
                    WHEN g.certificazione_recapito_stato = 'RECAG003D'
                     AND (g.demat_23l_ar_stato = 'RECAG003E' OR g.demat_plico_stato = 'RECAG003E' OR g.demat_arcad_stato = 'RECAG003E')
                     AND s.fine_recapito_stato_silver = 'RECAG003F' THEN 0
                    WHEN g.certificazione_recapito_stato = 'RECAG005A'
                     AND (
                        g.demat_23l_ar_stato IN ('RECAG011B','RECAG005B')
                        OR g.demat_plico_stato IN ('RECAG011B','RECAG005B')
                        OR g.demat_arcad_stato IN ('RECAG011B','RECAG005B')
                     )
                     AND s.fine_recapito_stato_silver = 'RECAG005C' THEN 0
                    WHEN g.certificazione_recapito_stato = 'RECAG006A'
                     AND (
                        g.demat_23l_ar_stato IN ('RECAG011B','RECAG006B')
                        OR g.demat_plico_stato IN ('RECAG011B','RECAG006B')
                        OR g.demat_arcad_stato IN ('RECAG011B','RECAG006B')
                     )
                     AND s.fine_recapito_stato_silver = 'RECAG006C' THEN 0
                    WHEN g.certificazione_recapito_stato = 'RECAG007A'
                     AND (
                        g.demat_23l_ar_stato IN ('RECAG011B','RECAG007B')
                        OR g.demat_plico_stato IN ('RECAG011B','RECAG007B')
                        OR g.demat_arcad_stato IN ('RECAG011B','RECAG007B')
                     )
                     AND s.fine_recapito_stato_silver = 'RECAG007C' THEN 0
                    WHEN g.certificazione_recapito_stato = 'RECAG008A'
                     AND (
                        g.demat_23l_ar_stato IN ('RECAG011B','RECAG008B')
                        OR g.demat_plico_stato IN ('RECAG011B','RECAG008B')
                        OR g.demat_arcad_stato IN ('RECAG011B','RECAG008B')
                     )
                     AND s.fine_recapito_stato_silver = 'RECAG008C' THEN 0
                    ELSE 1
                END AS controllo_tripletta,

                CASE
                    WHEN CAST(s.certificazione_recapito_data_silver AS TIMESTAMP)
                       = CAST(s.fine_recapito_data_silver AS TIMESTAMP) THEN 0
                    ELSE 1
                END AS controllo_date_business,

                CASE
                    WHEN g.certificazione_recapito_stato = 'RECRN005A'
                     AND DATEDIFF(
                        CAST(g.certificazione_recapito_data AS DATE),
                        CAST(g.tentativo_recapito_data AS DATE)
                     ) < 30 THEN 1
                    ELSE 0
                END AS controllo_tempistiche_compiuta_giacenza,

                IF(
                    g.fine_recapito_stato IN (
                        'RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C',
                        'RECAG005C','RECAG006C','RECAG007C','RECAG008C'
                    )
                    AND g.tentativo_recapito_stato IS NULL,
                    1, 0
                ) AS assenza_inesito,

                IF(
                    g.fine_recapito_stato IN (
                        'RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C',
                        'RECAG005C','RECAG006C','RECAG007C','RECAG008C'
                    )
                    AND g.messaingiacenza_recapito_stato IS NULL,
                    1, 0
                ) AS assenza_messa_in_giacenza,

                IF(g.certificazione_recapito_stato IS NULL, 1, 0) AS assenza_pre_esito,

                IF(
                    (
                        g.fine_recapito_stato IN ('RECAG008C')
                        AND (g.demat_23l_ar_stato IS NULL OR g.demat_plico_stato IS NULL)
                    )
                    OR (
                        g.demat_23l_ar_stato IS NULL
                        AND g.demat_plico_stato IS NULL
                    ),
                    1, 0
                ) AS assenza_dematerializzazione_23l_ar_plico,

                IF(
                    g.prodotto = '890'
                    AND g.fine_recapito_stato IN ('RECAG005C','RECAG006C','RECAG007C','RECAG008C')
                    AND g.demat_arcad_data_rendicontazione IS NULL,
                    1, 0
                ) AS assenza_demat_arcad,

                IF(
                    g.prodotto = '890'
                    AND g.fine_recapito_stato IN ('RECAG005C','RECAG006C','RECAG007C','RECAG008C')
                    AND g.accettazione_23l_recag012_data IS NULL,
                    1, 0
                ) AS assenza_recag012

            FROM gold_senza_schedule_refinement g
            LEFT JOIN notification_perimetro n
                ON g.iun = n.iun
            LEFT JOIN temp_analog_attempt a
                ON g.iun = a.iun
               AND CAST(g.attempt_number AS INT) = a.attempt_number_timeline
            LEFT JOIN ultimi_eventi_silver s
                ON g.requestid = s.requestid
            LEFT JOIN wi7_poste w
                ON g.requestid = w.requestid
        ),

        dettaglio AS (
            SELECT
                c.*,
                CASE
                    WHEN controllo_causale = 0
                     AND controllo_date_business = 0
                     AND controllo_tripletta = 0
                     AND controllo_tempistiche_compiuta_giacenza = 0
                     AND controllo_inesito_casi_giacenza = 0
                     AND assenza_inesito = 0
                     AND assenza_recag012 = 0
                     AND assenza_pre_esito = 0
                     AND assenza_dematerializzazione_23l_ar_plico = 0
                     AND assenza_messa_in_giacenza = 0
                     AND assenza_demat_arcad = 0
                     AND flag_feedback_attempt_0 = 0
                        THEN 'da perfezionare'
                    WHEN controllo_causale = 0
                     AND controllo_date_business = 0
                     AND controllo_tripletta = 0
                     AND controllo_tempistiche_compiuta_giacenza = 0
                     AND controllo_inesito_casi_giacenza = 0
                     AND assenza_inesito = 0
                     AND assenza_recag012 = 0
                     AND assenza_pre_esito = 0
                     AND assenza_dematerializzazione_23l_ar_plico = 0
                     AND assenza_messa_in_giacenza = 0
                     AND assenza_demat_arcad = 0
                     AND flag_feedback_attempt_0 = 1
                        THEN 'bloccato al primo attempt'
                    ELSE NULL
                END AS dettaglio_rendicontazione
            FROM controlli c
            WHERE tms_perfezionamento_notification IS NULL
        ),

        classificazione AS (
            SELECT
                requestid,
                CASE
                    WHEN flag_wi7_poste = 1
                        THEN 'Oggetto rientrante in tavoli Duplicati / Non Rendicontabili'
                    WHEN (
                        flag_wi7_report_postalizzazioni_incomplete = 1
                        OR flag_wi7_consolidatore = 1
                    ) AND flag_wi7_poste = 0
                        THEN 'Oggetto fuori sla'
                    WHEN (
                        controllo_causale = 1
                        OR controllo_date_business = 1
                        OR controllo_tripletta = 1
                        OR controllo_tempistiche_compiuta_giacenza = 1
                        OR controllo_inesito_casi_giacenza = 1
                        OR assenza_inesito = 1
                        OR assenza_messa_in_giacenza = 1
                        OR assenza_pre_esito = 1
                        OR assenza_dematerializzazione_23l_ar_plico = 1
                        OR assenza_demat_arcad = 1
                        OR assenza_recag012 = 1
                    )
                    AND fine_recapito_stato IS NOT NULL
                        THEN 'Errore rendicontazione/assenza eventi intermedi'
                    WHEN dettaglio_rendicontazione = 'bloccato al primo attempt'
                        THEN 'Oggetto bloccato al primo attempt'
                    WHEN dettaglio_rendicontazione = 'da perfezionare'
                     AND tms_date_payment IS NULL
                        THEN 'Oggetto con rendicontazione corretta da perfezionare'
                    WHEN dettaglio_rendicontazione = 'da perfezionare'
                     AND tms_date_payment IS NOT NULL
                        THEN 'Oggetto non perfezionato con evidenza di pagamento'
                    ELSE 'Oggetto in corso di postalizzazione'
                END AS cluster_mancato_perfezionamento
            FROM dettaglio
        )

        SELECT
            cluster_mancato_perfezionamento,
            COUNT(DISTINCT requestid) AS numero_oggetti
            -- Metrica temporaneamente disabilitata. Per ripristinarla, riportare
            -- tms_date_payment nella CTE classificazione e riattivare:
            -- , COUNT(DISTINCT CASE
            --     WHEN tms_date_payment IS NOT NULL THEN requestid
            --   END) AS oggetti_pagati
        FROM classificazione
        GROUP BY cluster_mancato_perfezionamento

    """

    df_spark = spark.sql(query_sql)
    logging.info("Conversione dell'aggregato in pandas...")
    return df_spark.toPandas()


def get_last_update_date(spark: SparkSession) -> str:
    logging.info("Estrazione MAX(requesttimestamp)...")
    max_date_df = spark.sql(
        "SELECT MAX(requesttimestamp) AS max_ts FROM send.gold_postalizzazione_analytics"
    )
    max_date = max_date_df.collect()[0]["max_ts"]
    if isinstance(max_date, datetime):
        return max_date.strftime("%Y-%m-%d %H:%M:%S")
    return str(max_date)


# =============================================================================
# NORMALIZZAZIONE, STORICO E DELTA
# =============================================================================


def cluster_order() -> list[str]:
    """Ordine alfabetico basato sulla label pulita e non sugli asterischi."""
    return sorted(
        CLUSTER_META.keys(),
        key=lambda cluster: CLUSTER_META[cluster]["label"].casefold(),
    )


def normalize_current_data(df: pd.DataFrame) -> pd.DataFrame:
    expected_columns = {
        "cluster_mancato_perfezionamento",
        "numero_oggetti",
    }
    missing_columns = expected_columns.difference(df.columns)
    if missing_columns:
        raise ValueError(f"Colonne mancanti nell'aggregato: {sorted(missing_columns)}")

    result = df.copy()
    result["cluster_mancato_perfezionamento"] = (
        result["cluster_mancato_perfezionamento"].astype(str).str.strip()
    )

    unknown = set(result["cluster_mancato_perfezionamento"]) - set(CLUSTER_META)
    if unknown:
        raise ValueError(f"Cluster non configurati: {sorted(unknown)}")

    if result["cluster_mancato_perfezionamento"].duplicated().any():
        duplicates = result.loc[
            result["cluster_mancato_perfezionamento"].duplicated(keep=False),
            "cluster_mancato_perfezionamento",
        ].tolist()
        raise ValueError(f"Cluster duplicati nell'aggregato: {duplicates}")

    result["numero_oggetti"] = (
        pd.to_numeric(result["numero_oggetti"], errors="raise")
        .fillna(0)
        .astype("int64")
    )

    result = (
        result.set_index("cluster_mancato_perfezionamento")
        .reindex(cluster_order(), fill_value=0)
        .reset_index()
    )

    result["cluster_label_clean"] = result["cluster_mancato_perfezionamento"].map(
        lambda cluster: CLUSTER_META[cluster]["label"]
    )
    result["cluster_label_table"] = result["cluster_mancato_perfezionamento"].map(
        lambda cluster: (
            CLUSTER_META[cluster]["label"] + CLUSTER_META[cluster]["footnote"]
        )
    )
    result["cluster_color"] = result["cluster_mancato_perfezionamento"].map(
        lambda cluster: CLUSTER_META[cluster]["color"]
    )

    # Metrica temporaneamente disabilitata, conservata come traccia per un
    # eventuale ripristino:
    # result["oggetti_pagati"] = pd.to_numeric(...)
    return result


def normalize_historical_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizza uno snapshot già pubblicato senza aggiungere i cluster mancanti.

    Questa distinzione è essenziale nella migrazione dalla struttura a cinque
    cluster: i tre nuovi cluster devono risultare non confrontabili, non uguali
    a zero nella settimana precedente.
    """
    required = {"cluster_mancato_perfezionamento", "numero_oggetti"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Colonne mancanti nello snapshot storico: {sorted(missing)}")

    result = df[list(required)].copy()
    result["cluster_mancato_perfezionamento"] = (
        result["cluster_mancato_perfezionamento"].astype(str).str.strip()
    )
    result["numero_oggetti"] = (
        pd.to_numeric(result["numero_oggetti"], errors="coerce")
        .fillna(0)
        .astype("int64")
    )
    result = result.drop_duplicates(
        subset=["cluster_mancato_perfezionamento"], keep="last"
    )
    return result.reset_index(drop=True)


def empty_history() -> pd.DataFrame:
    return pd.DataFrame(columns=HISTORY_COLUMNS)


def read_current_sheet_snapshot(
    sheet: Sheet,
    fallback_period_reference: str,
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Legge il tab corrente prima della sovrascrittura per inizializzare lo storico.

    Sono accettate sia la nuova intestazione ``Data max deposito`` sia la vecchia
    ``periodo_riferimento``. Se entrambe mancano, viene usato il valore presente
    nel tab di controllo.
    """
    try:
        snapshot = sheet.download(CURRENT_SHEET_NAME)
    except Exception as exc:
        logging.warning("Tab corrente non disponibile come fallback del delta: %s", exc)
        return None, None

    if snapshot is None or snapshot.empty:
        return None, None

    required_columns = {
        "cluster_mancato_perfezionamento",
        "numero_oggetti",
    }
    missing_columns = required_columns.difference(snapshot.columns)
    if missing_columns:
        logging.warning(
            "Il tab '%s' non può essere usato come fallback: colonne mancanti %s.",
            CURRENT_SHEET_NAME,
            sorted(missing_columns),
        )
        return None, None

    snapshot = snapshot.copy()
    if PERIOD_COLUMN in snapshot.columns:
        period_values = snapshot[PERIOD_COLUMN].astype("string").str.strip()
    elif LEGACY_PERIOD_COLUMN in snapshot.columns:
        logging.info(
            "Nel tab '%s' viene migrata la colonna '%s' in '%s'.",
            CURRENT_SHEET_NAME,
            LEGACY_PERIOD_COLUMN,
            PERIOD_COLUMN,
        )
        period_values = snapshot[LEGACY_PERIOD_COLUMN].astype("string").str.strip()
    else:
        logging.info(
            "Nel tab '%s' manca il campo '%s': viene assunto il valore del tab "
            "di controllo (%s).",
            CURRENT_SHEET_NAME,
            PERIOD_COLUMN,
            fallback_period_reference,
        )
        period_values = pd.Series(
            [fallback_period_reference] * len(snapshot),
            index=snapshot.index,
            dtype="string",
        )

    missing_period = period_values.isna() | period_values.isin(
        ["", "nan", "None", "<NA>"]
    )
    period_values = period_values.mask(missing_period, fallback_period_reference)
    snapshot[PERIOD_COLUMN] = period_values.astype(str)

    unique_periods = sorted(set(snapshot[PERIOD_COLUMN].astype(str).str.strip()))
    if len(unique_periods) != 1:
        raise ValueError(
            f"Il tab '{CURRENT_SHEET_NAME}' contiene più valori di '{PERIOD_COLUMN}': "
            f"{unique_periods}. Impossibile usarlo per un delta affidabile."
        )

    normalized = normalize_historical_snapshot(
        snapshot[["cluster_mancato_perfezionamento", "numero_oggetti"]]
    )
    return normalized, unique_periods[0]


def read_history(sheet: Sheet) -> pd.DataFrame:
    try:
        history = sheet.download(HISTORY_SHEET_NAME)
    except Exception as exc:
        logging.warning(
            "Storico non disponibile; verrà inizializzato da questa esecuzione: %s",
            exc,
        )
        return empty_history()

    if history is None or history.empty:
        return empty_history()

    history = history.copy()
    if PERIOD_COLUMN not in history.columns:
        if LEGACY_PERIOD_COLUMN in history.columns:
            history[PERIOD_COLUMN] = history[LEGACY_PERIOD_COLUMN]
            logging.info(
                "Storico migrato dalla colonna '%s' alla colonna '%s'.",
                LEGACY_PERIOD_COLUMN,
                PERIOD_COLUMN,
            )
        else:
            history[PERIOD_COLUMN] = pd.NA

    for column in HISTORY_COLUMNS:
        if column not in history.columns:
            history[column] = pd.NA

    history = history[HISTORY_COLUMNS]
    history["numero_oggetti"] = (
        pd.to_numeric(history["numero_oggetti"], errors="coerce")
        .fillna(0)
        .astype("int64")
    )
    return history


def week_metadata(reference_date) -> Tuple[str, str]:
    """Restituisce settimana ISO e lunedì della settimana per una data."""
    iso = reference_date.isocalendar()
    week_reference = f"{iso.year}-W{iso.week:02d}"
    week_start = reference_date.fromisocalendar(iso.year, iso.week, 1).isoformat()
    return week_reference, week_start


def current_week_metadata(now_rome: datetime) -> Tuple[str, str]:
    return week_metadata(now_rome.date())


def previous_week_metadata(now_rome: datetime) -> Tuple[str, str]:
    return week_metadata(now_rome.date() - timedelta(days=7))


def get_previous_snapshot(
    history: pd.DataFrame,
    current_week: str,
) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
    """Restituisce l'ultimo snapshot precedente, la data max deposito e la settimana."""
    if history.empty:
        return None, None, None

    previous_rows = history.loc[
        history["settimana_riferimento"].astype(str) != current_week
    ].copy()
    if previous_rows.empty:
        return None, None, None

    previous_rows["data_inizio_settimana_parsed"] = pd.to_datetime(
        previous_rows["data_inizio_settimana"], errors="coerce"
    )
    previous_rows = previous_rows.dropna(subset=["data_inizio_settimana_parsed"])
    if previous_rows.empty:
        return None, None, None

    max_start = previous_rows["data_inizio_settimana_parsed"].max()
    snapshot = previous_rows.loc[
        previous_rows["data_inizio_settimana_parsed"] == max_start
    ].copy()

    previous_week = str(snapshot["settimana_riferimento"].iloc[0])
    previous_period = str(snapshot[PERIOD_COLUMN].iloc[0])
    return snapshot, previous_period, previous_week


def calculate_delta(
    current_df: pd.DataFrame,
    history: pd.DataFrame,
    current_week: str,
    current_period: str,
) -> Tuple[pd.DataFrame, bool, Optional[int], Optional[str]]:
    """
    Calcola il delta per ciascun cluster solo quando esiste una riga storica
    confrontabile e la ``Data max deposito`` coincide.

    Nel passaggio da cinque a sette cluster, i quattro cluster invariati mantengono
    quindi un delta numerico, mentre i tre nuovi cluster ricevono ``n.d.``. Il
    totale rimane confrontabile a livello complessivo quando la data coincide.
    """
    result = current_df.copy()
    result["numero_oggetti_precedente"] = pd.NA
    result["delta_settimanale"] = pd.NA
    result["delta_disponibile"] = False

    previous, previous_period, previous_week = get_previous_snapshot(
        history, current_week
    )
    if previous is None:
        return result, False, None, None

    if previous_period != current_period:
        logging.info(
            "Delta non calcolato: '%s' corrente (%s) diversa dalla precedente "
            "(%s, settimana %s).",
            PERIOD_COLUMN,
            current_period,
            previous_period,
            previous_week,
        )
        return result, False, None, previous_week

    previous_norm = normalize_historical_snapshot(
        previous[["cluster_mancato_perfezionamento", "numero_oggetti"]]
    ).rename(columns={"numero_oggetti": "numero_oggetti_precedente"})

    result = result.drop(
        columns=["numero_oggetti_precedente", "delta_settimanale", "delta_disponibile"]
    ).merge(
        previous_norm,
        on="cluster_mancato_perfezionamento",
        how="left",
        validate="one_to_one",
    )
    result["delta_disponibile"] = result["numero_oggetti_precedente"].notna()
    result["delta_settimanale"] = (
        result["numero_oggetti"]
        - pd.to_numeric(result["numero_oggetti_precedente"], errors="coerce")
    ).where(result["delta_disponibile"])
    result["delta_settimanale"] = result["delta_settimanale"].astype("Int64")

    previous_total = int(previous_norm["numero_oggetti_precedente"].sum())
    current_total = int(result["numero_oggetti"].sum())
    total_delta = current_total - previous_total
    return result, True, total_delta, previous_week


def build_current_history_snapshot(
    current_df: pd.DataFrame,
    week_reference: str,
    week_start: str,
    execution_utc: str,
    period_reference: str,
) -> pd.DataFrame:
    snapshot = current_df[["cluster_mancato_perfezionamento", "numero_oggetti"]].copy()
    snapshot.insert(0, PERIOD_COLUMN, period_reference)
    snapshot.insert(0, "data_esecuzione_utc", execution_utc)
    snapshot.insert(0, "data_inizio_settimana", week_start)
    snapshot.insert(0, "settimana_riferimento", week_reference)

    # Metrica temporaneamente disabilitata:
    # snapshot["oggetti_pagati"] = current_df["oggetti_pagati"]
    return snapshot[HISTORY_COLUMNS]


def upsert_history(
    history: pd.DataFrame,
    current_snapshot: pd.DataFrame,
    current_week: str,
) -> pd.DataFrame:
    """Sovrascrive le righe della settimana corrente e mantiene le altre in append."""
    if history.empty:
        result = current_snapshot.copy()
    else:
        old = history.loc[
            history["settimana_riferimento"].astype(str) != current_week
        ].copy()
        result = pd.concat([old, current_snapshot], ignore_index=True)

    result["data_inizio_settimana_parsed"] = pd.to_datetime(
        result["data_inizio_settimana"], errors="coerce"
    )
    result["cluster_sort"] = result["cluster_mancato_perfezionamento"].map(
        lambda cluster: CLUSTER_META.get(cluster, {})
        .get("label", LEGACY_CLUSTER_LABELS.get(cluster, str(cluster)))
        .casefold()
    )
    result = result.sort_values(
        ["data_inizio_settimana_parsed", "cluster_sort"], kind="stable"
    ).drop(columns=["data_inizio_settimana_parsed", "cluster_sort"])
    return result[HISTORY_COLUMNS].reset_index(drop=True)


def validate_report(df: pd.DataFrame) -> None:
    if len(df) != len(CLUSTER_META):
        raise ValueError(
            f"Numero cluster inatteso: {len(df)}; attesi {len(CLUSTER_META)}."
        )
    if (df["numero_oggetti"] < 0).any():
        raise ValueError("Sono presenti conteggi negativi.")


# =============================================================================
# FORMATTAZIONE E GENERAZIONE IMMAGINI
# =============================================================================


def configure_montserrat_font() -> str:
    """Registra Montserrat da un path montato oppure usa l'installazione di sistema."""
    candidates = []
    configured_path = os.environ.get(MONTSERRAT_FONT_PATH_ENV)
    if configured_path:
        candidates.append(Path(configured_path))

    candidates.extend(
        [
            Path("/app/mount/Montserrat-Regular.ttf"),
            Path("/app/mount/fonts/Montserrat-Regular.ttf"),
            Path("/app/mount/Montserrat/Montserrat-Regular.ttf"),
            Path("/usr/share/fonts/truetype/montserrat/Montserrat-Regular.ttf"),
        ]
    )

    selected_family = FONT_FAMILY
    for candidate in candidates:
        if candidate.is_file():
            font_manager.fontManager.addfont(str(candidate))
            selected_family = font_manager.FontProperties(
                fname=str(candidate)
            ).get_name()
            logging.info("Font Montserrat registrato da: %s", candidate)
            break
    else:
        try:
            font_manager.findfont(FONT_FAMILY, fallback_to_default=False)
            logging.info("Font Montserrat disponibile nel sistema.")
        except ValueError:
            selected_family = "DejaVu Sans"
            logging.warning(
                "Montserrat non disponibile. Montare Montserrat-Regular.ttf e impostare "
                "la variabile %s; per questa esecuzione viene usato %s.",
                MONTSERRAT_FONT_PATH_ENV,
                selected_family,
            )

    matplotlib.rcParams.update(
        {
            "font.family": selected_family,
            "font.size": BASE_FONT_SIZE,
        }
    )
    return selected_family


def format_integer_it(value: int) -> str:
    return f"{int(value):,}".replace(",", ".")


def format_percentage_it(value: float) -> str:
    return f"{value:.2f}%".replace(".", ",")


def format_delta(value: object, available: bool) -> Tuple[str, str]:
    if not available or pd.isna(value):
        return "n.d.", DELTA_NEUTRAL

    numeric = int(value)
    if numeric < 0:
        return f"{format_integer_it(numeric)} ↓", DELTA_GREEN
    if numeric > 0:
        return f"+{format_integer_it(numeric)} ↑", DELTA_RED
    return "0 ↔", DELTA_NEUTRAL


def prepare_output_dir() -> None:
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def render_table(
    report_df: pd.DataFrame,
    total_delta_available: bool,
    total_delta: Optional[int],
    output_path: Path,
) -> None:
    """
    Genera la tabella mantenendo le etichette dei cluster su una sola riga.

    Le descrizioni e le intestazioni usano la dimensione base di 10,5 pt; i
    valori delle due colonne numeriche, compreso il totale, usano 11 pt e sono
    in corsivo. La figura viene salvata senza ritaglio automatico, così la sua
    altezza resta identica a quella della colonna cromatica.
    """
    rows = []
    delta_colors = []
    for row in report_df.itertuples(index=False):
        row_available = bool(row.delta_disponibile)
        delta_text, delta_color = format_delta(row.delta_settimanale, row_available)
        rows.append(
            [
                str(row.cluster_label_table),
                format_integer_it(row.numero_oggetti),
                delta_text,
            ]
        )
        delta_colors.append(delta_color)

    total_objects = int(report_df["numero_oggetti"].sum())
    total_delta_text, _ = format_delta(total_delta, total_delta_available)
    rows.append(["Totale", format_integer_it(total_objects), total_delta_text])

    columns = [
        "Cluster di mancato perfezionamento",
        "Numero oggetti",
        "Delta settimanale",
    ]

    fig = plt.figure(figsize=TABLE_FIGSIZE, dpi=IMAGE_DPI, facecolor="white")
    ax = fig.add_axes([0.0, 0.0, 1.0, 1.0])
    ax.axis("off")
    table = ax.table(
        cellText=rows,
        colLabels=columns,
        cellLoc="center",
        colLoc="center",
        loc="center",
        colWidths=[0.69, 0.14, 0.17],
        bbox=[0.0, 0.0, 1.0, 1.0],
    )
    table.auto_set_font_size(False)
    table.set_fontsize(BASE_FONT_SIZE)

    total_row_index = len(rows)
    table_row_count = len(rows) + 1  # intestazione + righe dati + totale
    row_height = 1.0 / table_row_count

    for (row_idx, col_idx), cell in table.get_celld().items():
        cell.set_height(row_height)
        cell.set_edgecolor(GRID_COLOR)
        cell.set_linewidth(1.0)
        cell.get_text().set_color(TEXT_COLOR)
        cell.get_text().set_fontsize(BASE_FONT_SIZE)

        if row_idx == 0:
            cell.set_facecolor(HEADER_BG)
            cell.get_text().set_color(HEADER_TEXT_COLOR)
            cell.get_text().set_weight("bold")
        elif row_idx == total_row_index:
            cell.set_facecolor(TOTAL_BG)
            cell.get_text().set_color(TOTAL_TEXT_COLOR)
            cell.get_text().set_weight("bold")
        else:
            cell.set_facecolor("#FFFFFF")

        # Etichette dei cluster: una sola riga, allineate a sinistra e in corsivo.
        if 0 < row_idx < total_row_index and col_idx == 0:
            cell.get_text().set_ha("left")
            cell.get_text().set_style("italic")
            cell.PAD = 0.025

        # Colonne numeriche: 11 pt e corsivo, inclusa la riga Totale.
        if row_idx > 0 and col_idx in (1, 2):
            cell.get_text().set_fontsize(NUMERIC_FONT_SIZE)
            cell.get_text().set_style("italic")

        if 0 < row_idx < total_row_index and col_idx == 2:
            cell.get_text().set_color(delta_colors[row_idx - 1])
            cell.get_text().set_weight("bold")

    fig.savefig(output_path, dpi=IMAGE_DPI, pad_inches=0, facecolor="white")
    plt.close(fig)


def render_color_column(report_df: pd.DataFrame, output_path: Path) -> None:
    """
    Disegna una colonna cromatica perfettamente allineata alla tabella.

    Ogni cluster occupa l'intera altezza della rispettiva cella; le fasce
    corrispondenti all'intestazione e al totale restano vuote. La figura ha la
    stessa altezza in pixel della tabella e viene salvata senza ritagli, così nel
    pannello completo la colonna può essere accostata senza spazi al bordo
    sinistro della tabella.
    """
    table_row_count = len(report_df) + 2  # intestazione + cluster + totale
    row_height = 1.0 / table_row_count

    fig = plt.figure(figsize=LEGEND_FIGSIZE, dpi=IMAGE_DPI, facecolor="white")
    ax = fig.add_axes([0.0, 0.0, 1.0, 1.0])
    ax.set_xlim(0.0, 1.0)
    ax.set_ylim(0.0, 1.0)
    ax.axis("off")

    for idx, color in enumerate(report_df["cluster_color"].tolist()):
        # idx=0 corrisponde alla prima riga dati, subito sotto l'intestazione.
        y_bottom = 1.0 - ((idx + 2) * row_height)
        rectangle = plt.Rectangle(
            (0.0, y_bottom),
            1.0,
            row_height,
            facecolor=color,
            edgecolor=GRID_COLOR,
            linewidth=1.0,
        )
        ax.add_patch(rectangle)

    fig.savefig(output_path, dpi=IMAGE_DPI, pad_inches=0, facecolor="white")
    plt.close(fig)


def _adjust_small_label_positions(points: list[dict], min_gap: float = 0.16) -> None:
    for side in (-1, 1):
        side_points = sorted(
            [point for point in points if point["side"] == side],
            key=lambda point: point["y"],
        )
        for idx in range(1, len(side_points)):
            if side_points[idx]["y"] - side_points[idx - 1]["y"] < min_gap:
                side_points[idx]["y"] = side_points[idx - 1]["y"] + min_gap


def prepare_donut_data(report_df: pd.DataFrame) -> pd.DataFrame:
    """Aggrega in 'Altri' almeno due cluster positivi con quota inferiore all'1%."""
    donut = report_df[["cluster_label_clean", "numero_oggetti", "cluster_color"]].copy()
    donut = donut.loc[donut["numero_oggetti"] > 0].reset_index(drop=True)
    total = float(donut["numero_oggetti"].sum())
    if total <= 0:
        return donut

    donut["quota"] = donut["numero_oggetti"] / total
    small_mask = donut["quota"] < 0.01
    if int(small_mask.sum()) >= 2:
        others_value = int(donut.loc[small_mask, "numero_oggetti"].sum())
        donut = donut.loc[~small_mask].copy()
        others = pd.DataFrame(
            [
                {
                    "cluster_label_clean": "Altri",
                    "numero_oggetti": others_value,
                    "cluster_color": OTHER_COLOR,
                    "quota": others_value / total,
                }
            ]
        )
        donut = pd.concat([donut, others], ignore_index=True)

    return donut.reset_index(drop=True)


def render_donut(report_df: pd.DataFrame, output_path: Path) -> None:
    donut_df = prepare_donut_data(report_df)
    values = donut_df["numero_oggetti"].astype(float).to_numpy()
    colors = donut_df["cluster_color"].tolist()
    labels = donut_df["cluster_label_clean"].tolist()
    total = int(report_df["numero_oggetti"].sum())

    fig, ax = plt.subplots(figsize=(7.5, 7.2), dpi=180)
    ax.set_aspect("equal")
    ax.axis("off")

    if total <= 0:
        circle = plt.Circle((0, 0), 0.8, fill=False, linewidth=24, edgecolor="#D9E1E8")
        ax.add_artist(circle)
        ax.text(0, 0.08, "0", ha="center", va="center", fontsize=24, weight="bold")
        ax.text(0, -0.18, "oggetti", ha="center", va="center", fontsize=BASE_FONT_SIZE)
        ax.text(
            0,
            -1.18,
            "Nessun oggetto nel perimetro",
            ha="center",
            fontsize=BASE_FONT_SIZE,
        )
    else:
        wedges, _ = ax.pie(
            values,
            colors=colors,
            startangle=90,
            counterclock=False,
            wedgeprops={"width": 0.40, "edgecolor": "white", "linewidth": 2.0},
        )

        small_labels: list[dict] = []
        for wedge, value, cluster_label in zip(wedges, values, labels):
            if value <= 0:
                continue
            percentage = float(value / total * 100.0)
            angle = math.radians((wedge.theta1 + wedge.theta2) / 2.0)
            percentage_label = format_percentage_it(percentage)
            display_label = (
                f"Altri\n{percentage_label}"
                if cluster_label == "Altri"
                else percentage_label
            )

            if percentage >= 5.0 and cluster_label != "Altri":
                radius = 0.79
                ax.text(
                    radius * math.cos(angle),
                    radius * math.sin(angle),
                    display_label,
                    ha="center",
                    va="center",
                    fontsize=BASE_FONT_SIZE,
                    weight="bold",
                    color=TEXT_COLOR,
                )
            else:
                x = 1.10 * math.cos(angle)
                y = 1.10 * math.sin(angle)
                small_labels.append(
                    {
                        "angle": angle,
                        "y": y,
                        "side": 1 if x >= 0 else -1,
                        "label": display_label,
                    }
                )

        _adjust_small_label_positions(small_labels)
        for item in small_labels:
            angle = item["angle"]
            cos_angle = math.cos(angle)
            x_anchor = 0.98 * cos_angle
            y_anchor = 0.98 * math.sin(angle)
            x_text = 1.14 * cos_angle
            if abs(cos_angle) < 0.15:
                horizontal_alignment = "center"
            else:
                horizontal_alignment = "left" if cos_angle > 0 else "right"
            ax.annotate(
                item["label"],
                xy=(x_anchor, y_anchor),
                xytext=(x_text, item["y"]),
                ha=horizontal_alignment,
                va="center",
                fontsize=BASE_FONT_SIZE,
                weight="bold",
                color=TEXT_COLOR,
                arrowprops={
                    "arrowstyle": "-",
                    "connectionstyle": "arc3,rad=0.04",
                    "color": "#74828F",
                    "linewidth": 1.0,
                    "shrinkA": 0,
                    "shrinkB": 0,
                },
            )

        ax.text(
            0,
            0.09,
            format_integer_it(total),
            ha="center",
            va="center",
            fontsize=25,
            weight="bold",
            color=TEXT_COLOR,
        )
        ax.text(
            0,
            -0.19,
            "oggetti",
            ha="center",
            va="center",
            fontsize=BASE_FONT_SIZE,
            color=TEXT_COLOR,
        )

    fig.savefig(output_path, bbox_inches="tight", pad_inches=0.05, facecolor="white")
    plt.close(fig)


def render_combined(
    table_path: Path,
    legend_path: Path,
    chart_path: Path,
    output_path: Path,
) -> None:
    table_img = Image.open(table_path).convert("RGB")
    legend_img = Image.open(legend_path).convert("RGB")
    chart_img = Image.open(chart_path).convert("RGB")

    target_height = table_img.height

    # Tabella e colonna cromatica sono generate con la stessa altezza. Il resize
    # resta soltanto come protezione nel caso di differenze introdotte dal runtime.
    if legend_img.height != target_height:
        legend_width = max(
            1, int(round(legend_img.width * target_height / legend_img.height))
        )
        legend_resized = legend_img.resize(
            (legend_width, target_height), Image.Resampling.LANCZOS
        )
    else:
        legend_resized = legend_img
        legend_width = legend_img.width

    chart_width = int(round(chart_img.width * target_height / chart_img.height))
    chart_resized = chart_img.resize(
        (chart_width, target_height), Image.Resampling.LANCZOS
    )

    chart_gap = 28
    canvas_width = legend_width + table_img.width + chart_gap + chart_width
    canvas = Image.new("RGB", (canvas_width, target_height), "white")

    # Nessun margine tra colonna cromatica e tabella.
    canvas.paste(legend_resized, (0, 0))
    canvas.paste(table_img, (legend_width, 0))
    canvas.paste(chart_resized, (legend_width + table_img.width + chart_gap, 0))
    canvas.save(output_path, format="PNG", optimize=True)


def generate_all_images(
    report_df: pd.DataFrame,
    total_delta_available: bool,
    total_delta: Optional[int],
) -> Dict[str, Path]:
    configure_montserrat_font()
    prepare_output_dir()
    paths = {key: OUTPUT_DIR / name for key, name in OUTPUT_FILES.items()}

    render_table(
        report_df,
        total_delta_available,
        total_delta,
        paths["table"],
    )
    render_color_column(report_df, paths["legend"])
    render_donut(report_df, paths["chart"])
    render_combined(
        paths["table"],
        paths["legend"],
        paths["chart"],
        paths["combined"],
    )

    for path in paths.values():
        if not path.exists() or path.stat().st_size == 0:
            raise RuntimeError(f"Immagine non generata correttamente: {path}")
        logging.info("Immagine generata: %s (%s byte)", path, path.stat().st_size)

    return paths


# =============================================================================
# SCRITTURA GSHEET
# =============================================================================


def export_to_sheets(
    df: pd.DataFrame, creds: dict, sheet_id: str, sheet_name: str
) -> None:
    logging.info("Scrittura su Google Sheet: %s", sheet_name)
    export_df = df.copy().astype(str)
    sheet = Sheet(sheet_id=sheet_id, service_credentials=creds, id_mode="key")
    sheet.upload(sheet_name, export_df)
    logging.info("Scrittura completata: %s", sheet_name)


def update_control_sheet(
    sheet: Sheet,
    df_update: pd.DataFrame,
    period_reference: str,
    max_date_str: str,
    execution_utc: str,
) -> None:
    result = df_update.copy()

    # Mantiene la struttura preesistente e aggiorna la riga usata dal job originario.
    result.loc[1, "Data max deposito"] = period_reference
    result.loc[1, "Ultimo aggiornamento dati (MAX requesttimestamp)"] = max_date_str
    result.loc[1, "Data esecuzione script (UTC)"] = execution_utc

    sheet.upload(UPDATE_SHEET_NAME, result.astype(str))


# =============================================================================
# MAIN
# =============================================================================


def main() -> None:
    spark: Optional[SparkSession] = None

    try:
        logging.info("Inizializzazione SparkSession...")
        spark = SparkSession.builder.appName(
            "SSDA-526 Aggregato non perfezionati con immagini"
        ).getOrCreate()

        creds = load_google_credentials(GOOGLE_SECRET_PATH)
        sheet = Sheet(sheet_id=SHEET_ID, service_credentials=creds, id_mode="key")

        period_reference, df_update = read_period_reference(sheet)
        current_sheet_fallback, current_sheet_fallback_period = (
            read_current_sheet_snapshot(sheet, period_reference)
        )
        history = read_history(sheet)

        now_utc = datetime.now(timezone.utc)
        now_rome = now_utc.astimezone(ROME_TZ)
        execution_utc = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
        current_week, week_start = current_week_metadata(now_rome)

        # Migrazione iniziale: se lo storico non esiste, il tab corrente viene
        # materializzato come settimana precedente. La struttura originaria a
        # cinque cluster viene preservata senza aggiungere zeri artificiali ai
        # tre nuovi cluster, che resteranno quindi non confrontabili.
        history_for_run = history.copy()
        if history_for_run.empty and current_sheet_fallback is not None:
            legacy_week, legacy_week_start = previous_week_metadata(now_rome)
            legacy_snapshot = build_current_history_snapshot(
                current_sheet_fallback,
                week_reference=legacy_week,
                week_start=legacy_week_start,
                execution_utc="",
                period_reference=str(current_sheet_fallback_period),
            )
            history_for_run = legacy_snapshot
            logging.info(
                "Storico inizializzato dal tab '%s' come settimana %s (%s=%s).",
                CURRENT_SHEET_NAME,
                legacy_week,
                PERIOD_COLUMN,
                current_sheet_fallback_period,
            )

        current_raw = run_query(spark, period_reference)
        current = normalize_current_data(current_raw)
        report, total_delta_available, total_delta, previous_week = calculate_delta(
            current,
            history_for_run,
            current_week,
            period_reference,
        )
        validate_report(report)

        comparable_clusters = int(report["delta_disponibile"].sum())
        logging.info(
            "Confronto delta: settimana_precedente=%s, cluster_confrontabili=%s/%s, "
            "totale_confrontabile=%s.",
            previous_week,
            comparable_clusters,
            len(report),
            total_delta_available,
        )

        image_paths = generate_all_images(
            report,
            total_delta_available,
            total_delta,
        )
        drive_results = upload_all_images_to_drive(
            image_paths,
            creds,
            DRIVE_FOLDER_ID,
        )
        for key, metadata in drive_results.items():
            logging.info(
                "Output Drive [%s]: %s - %s",
                key,
                metadata.get("name"),
                metadata.get("webViewLink", metadata.get("id")),
            )

        current_snapshot = build_current_history_snapshot(
            current,
            week_reference=current_week,
            week_start=week_start,
            execution_utc=execution_utc,
            period_reference=period_reference,
        )
        updated_history = upsert_history(
            history_for_run,
            current_snapshot,
            current_week,
        )

        export_to_sheets(
            updated_history,
            creds,
            SHEET_ID,
            HISTORY_SHEET_NAME,
        )

        current_sheet_df = report[
            [
                "cluster_mancato_perfezionamento",
                "numero_oggetti",
                "delta_settimanale",
                "delta_disponibile",
            ]
        ].copy()
        current_sheet_df.insert(0, PERIOD_COLUMN, period_reference)
        current_sheet_df["delta_settimanale"] = current_sheet_df.apply(
            lambda row: (
                int(row["delta_settimanale"])
                if bool(row["delta_disponibile"])
                and not pd.isna(row["delta_settimanale"])
                else "n.d."
            ),
            axis=1,
        )
        current_sheet_df = current_sheet_df.drop(columns=["delta_disponibile"])

        # Metrica temporaneamente disabilitata dall'output GSheet:
        # current_sheet_df["oggetti_pagati"] = report["oggetti_pagati"]
        export_to_sheets(
            current_sheet_df,
            creds,
            SHEET_ID,
            CURRENT_SHEET_NAME,
        )

        max_date_str = get_last_update_date(spark)
        update_control_sheet(
            sheet,
            df_update,
            period_reference,
            max_date_str,
            execution_utc,
        )

        logging.info(
            "Processo completato: settimana=%s, %s=%s, cluster=%s.",
            current_week,
            PERIOD_COLUMN,
            period_reference,
            len(current),
        )

    except Exception:
        logging.exception("Errore durante l'esecuzione del job SSDA-526.")
        raise
    finally:
        if spark is not None:
            spark.stop()
            logging.info("SparkSession arrestata.")


if __name__ == "__main__":
    main()
