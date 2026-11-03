from __future__ import annotations

import logging
import math
import os
import re
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from typing import Dict, Optional, Tuple
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
from PIL import Image, ImageDraw, ImageFont
from pyspark import StorageLevel
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
    "viewed_chart": "SSDA-526_grafico_visualizzazioni_latest.png",
    "combined": "SSDA-526_completo_latest.png",
    "combined_viewed": "SSDA-526_completo_visualizzazioni_latest.png",
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
VIEWED_COLOR = "#087F8C"
NOT_VIEWED_COLOR = "#D5DFE6"
VIEWED_COLUMN = "numero_oggetti_con_notifica_visualizzata"

PERIOD_COLUMN = "Data max deposito"
LEGACY_PERIOD_COLUMN = "periodo_riferimento"
FONT_FAMILY = "Montserrat"
BASE_FONT_SIZE = 12.0
NUMERIC_FONT_SIZE = 13.0
TABLE_FIGSIZE = (13.2, 7.2)
LEGEND_FIGSIZE = (0.8, 7.2)
IMAGE_DPI = 180
MONTSERRAT_FONT_PATH_ENV = "MONTSERRAT_FONT_PATH"

# Label, nota e colore sono modificabili senza intervenire sulla logica SQL o sullo storico.

CLUSTER_ORDER = [
    "Errore rendicontazione/assenza eventi intermedi",
    "Oggetto con rendicontazione corretta - Bloccato al primo attempt",
    "Oggetto con rendicontazione corretta - Bloccato al secondo attempt",
    "Oggetto rientrante in tavoli Duplicati / Non Rendicontabili",
    "Oggetto fuori sla",
    "Oggetto in corso di postalizzazione",
]

# I colori dei cluster invariati restano identici alla versione precedente.
# Per i due nuovi cluster vengono riutilizzati due colori già presenti nella palette storica.
CLUSTER_META: Dict[str, Dict[str, str]] = {
    "Errore rendicontazione/assenza eventi intermedi": {
        "label": "Oggetti con errori rendicontazione/assenza eventi intermedi",
        "footnote": "*",
        "color": "#c47182",
    },
    "Oggetto con rendicontazione corretta - Bloccato al primo attempt": {
        "label": "Rendicontazione corretta - Bloccati al primo attempt",
        "footnote": "",
        "color": "#F6D58A",
    },
    "Oggetto con rendicontazione corretta - Bloccato al secondo attempt": {
        "label": "Rendicontazione corretta - Bloccati al secondo attempt",
        "footnote": "",
        "color": "#8FAADC",
    },
    "Oggetto rientrante in tavoli Duplicati / Non Rendicontabili": {
        "label": "Oggetti rientranti in tavoli Duplicati / Non Rendicontabili",
        "footnote": "",
        "color": "#42c3eb",
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
}

# Etichette legacy mantenute esclusivamente per leggere e ordinare gli snapshot
# preesistenti; non vengono più prodotte dalla nuova baseline.
LEGACY_CLUSTER_LABELS = {
    "Oggetto con rendicontazione corretta in analisi Team Prodotto": "Oggetti con rendicontazione corretta in analisi Team Prodotto",
    "Oggetto bloccato al primo attempt": "Oggetti bloccati al primo attempt",
    "Oggetto con rendicontazione corretta da perfezionare": "Oggetti con rendicontazione corretta da perfezionare",
    "Oggetto non perfezionato con evidenza di pagamento": "Oggetti non perfezionati con evidenza di pagamento",
}

HISTORY_COLUMNS = [
    "settimana_riferimento",
    "data_inizio_settimana",
    "data_esecuzione_utc",
    PERIOD_COLUMN,
    "cluster_mancato_perfezionamento",
    "numero_oggetti",
    VIEWED_COLUMN,
    # Indicatore introdotto con la nuova baseline: gli snapshot legacy restano vuoti.
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
    Esegue SSDA-526 a fasi materializzate per evitare la ricostruzione ripetuta
    dello stesso DAG Spark.

    L'ottimizzazione riguarda esclusivamente il piano di esecuzione:
      1. GOLD filtrato materializzato una volta;
      2. feedback analogico calcolato sul solo perimetro IUN;
      3. perimetro senza feedback materializzato una volta;
      4. SILVER postalizzazione letto una sola volta, con explode immediato di
         eventslist e attachments e successivo riuso del dataset piatto;
      5. aggregazione finale eseguita sui dataset intermedi già materializzati.

    I log riportano inizio/fine, durata e cardinalità dei passaggi principali.
    """

    persisted = []
    run_start = perf_counter()

    def log_step_start(step: str) -> float:
        logging.info("[SPARK] >>> %s", step)
        return perf_counter()

    def log_step_end(step: str, started_at: float, rows: Optional[int] = None) -> None:
        elapsed = perf_counter() - started_at
        if rows is None:
            logging.info("[SPARK] <<< %s completato in %.1f s", step, elapsed)
        else:
            logging.info(
                "[SPARK] <<< %s completato in %.1f s - righe=%s",
                step,
                elapsed,
                f"{rows:,}".replace(",", "."),
            )

    def persist_and_materialize(
        df, view_name: str, step: str, storage=StorageLevel.MEMORY_AND_DISK
    ):
        started = log_step_start(step)
        cached = df.persist(storage)
        persisted.append(cached)
        cached.createOrReplaceTempView(view_name)
        rows = cached.count()
        log_step_end(step, started, rows)
        return cached, rows

    try:
        # ------------------------------------------------------------------
        # 1) GOLD: una sola scansione della tabella di partenza.
        # ------------------------------------------------------------------
        gold_sql = f"""
            SELECT
                s.iun,
                s.requestid,
                s.prodotto,
                s.tentativo_recapito_stato,
                s.tentativo_recapito_data,
                s.messaingiacenza_recapito_stato,
                s.certificazione_recapito_stato,
                s.certificazione_recapito_dettagli,
                s.certificazione_recapito_data,
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
                s.accettazione_23l_recag012_data,
                s.recindex_number,
                s.flag_wi7_consolidatore,
                s.flag_wi7_report_postalizzazioni_incomplete,
                s.attempt_number
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
              -- RS e RIS temporaneamente esclusi; restano pronti nei controlli.
              AND s.prodotto IN ('AR', 'RIR', '890')
              AND s.data_deposito < '{data_max_deposito}'
        """
        gold_base, gold_rows = persist_and_materialize(
            spark.sql(gold_sql),
            "ssda526_gold_base",
            "1/7 GOLD filtrato e materializzato",
        )

        # ------------------------------------------------------------------
        # 2) Perimetro IUN + SEND_ANALOG_FEEDBACK.
        # ------------------------------------------------------------------
        perim_iun, iun_rows = persist_and_materialize(
            spark.sql(
                "SELECT DISTINCT iun FROM ssda526_gold_base WHERE iun IS NOT NULL"
            ),
            "ssda526_perimetro_iun",
            "2/7 Perimetro IUN materializzato",
            StorageLevel.MEMORY_ONLY,
        )

        feedback_sql = """
            SELECT DISTINCT
                tl.iun,
                CAST(REGEXP_EXTRACT(tl.timelineelementid, 'RECINDEX_([0-9]+)', 1) AS INT) AS recindex_number,
                CAST(REGEXP_EXTRACT(tl.timelineelementid, 'ATTEMPT_([0-9]+)', 1) AS INT) AS attempt_number
            FROM send.silver_timeline tl
            INNER JOIN ssda526_perimetro_iun p
                ON tl.iun = p.iun
            WHERE tl.category = 'SEND_ANALOG_FEEDBACK'
        """
        feedback, feedback_rows = persist_and_materialize(
            spark.sql(feedback_sql),
            "ssda526_feedback",
            "3/7 Feedback analogici sul perimetro",
        )

        # ------------------------------------------------------------------
        # 3) Perimetro definitivo: anti-join puntuale IUN+RECINDEX+ATTEMPT.
        # ------------------------------------------------------------------
        no_feedback_sql = """
            SELECT gb.*
            FROM ssda526_gold_base gb
            LEFT ANTI JOIN ssda526_feedback saf
              ON gb.iun = saf.iun
             AND CAST(gb.recindex_number AS INT) = saf.recindex_number
             AND CAST(gb.attempt_number AS INT) = saf.attempt_number
        """
        gold_no_feedback, no_feedback_rows = persist_and_materialize(
            spark.sql(no_feedback_sql),
            "gold_base_senza_feedback",
            "4/7 GOLD senza feedback materializzato",
        )

        logging.info(
            "[SPARK] Perimetro feedback: GOLD=%s, feedback=%s, senza_feedback=%s, esclusi=%s",
            f"{gold_rows:,}".replace(",", "."),
            f"{feedback_rows:,}".replace(",", "."),
            f"{no_feedback_rows:,}".replace(",", "."),
            f"{max(gold_rows - no_feedback_rows, 0):,}".replace(",", "."),
        )

        # Non servono più: liberiamo memoria prima della parte SILVER più pesante.
        gold_base.unpersist(blocking=False)
        feedback.unpersist(blocking=False)
        perim_iun.unpersist(blocking=False)

        perim_requestid, requestid_rows = persist_and_materialize(
            spark.sql(
                "SELECT DISTINCT requestid FROM gold_base_senza_feedback "
                "WHERE requestid IS NOT NULL"
            ),
            "ssda526_perimetro_requestid",
            "5/7 Perimetro requestId materializzato",
            StorageLevel.MEMORY_ONLY,
        )

        # ------------------------------------------------------------------
        # 4) SILVER: UNA SOLA LETTURA. Il pruning del perimetro requestId
        #    e' GIA' espresso nella INNER JOIN immediatamente prima dei
        #    LATERAL VIEW: manteniamo questa posizione e l'explode immediato.
        #    NON trasciniamo eventslist/attachments in CTE intermedie.
        #    Un'ulteriore riscrittura non garantirebbe un piano fisico migliore;
        #    il guadagno reale va verificato con EXPLAIN e metriche Spark.
        #    Il persist riguarda soltanto il risultato piatto, riutilizzato.
        # ------------------------------------------------------------------
        silver_flat_sql = """
            SELECT
                REGEXP_REPLACE(p.requestid, '^pn-cons-000~', '') AS requestid,
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
                a.documenttype AS documenttype
            FROM send.silver_postalizzazione p
            INNER JOIN ssda526_perimetro_requestid pr
                ON REGEXP_REPLACE(p.requestid, '^pn-cons-000~', '') = pr.requestid
            LATERAL VIEW explode(p.eventslist) ev AS e
            LATERAL VIEW OUTER explode(e.paperprogrstatus.attachments) av AS a
            WHERE e.paperprogrstatus.statuscode IN (
                'RECRN001A','RECRN002A','RECRN002D','RECRN003A','RECRN004A','RECRN005A',
                'RECAG001A','RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A','RECAG007A','RECAG008A',
                'RECRI003A','RECRI004A',
                'RECRN001C','RECRN002C','RECRN002F','RECRN003C','RECRN004C','RECRN005C',
                'RECAG001C','RECAG002C','RECAG003C','RECAG003F','RECAG005C','RECAG006C','RECAG007C','RECAG008C',
                'RECRI003C','RECRI004C',
                'RECRN001B','RECRN002B','RECRN002E','RECRN003B','RECRN004B','RECRN005B',
                'RECAG001B','RECAG002B','RECAG003B','RECAG003E','RECAG005B','RECAG006B','RECAG007B','RECAG008B',
                'RECRI003B','RECRI004B'
                -- RS/RIS restano pronti nei CASE business e vanno riattivati insieme al perimetro prodotto.
            )
            OR a.documenttype IN ('23L', 'AR')
        """
        silver_flat, silver_rows = persist_and_materialize(
            spark.sql(silver_flat_sql),
            "ssda526_silver_flat",
            "6/7 SILVER esploso e materializzato (unica scansione)",
        )
        logging.info(
            "[SPARK] SILVER piatto: requestId_perimetro=%s, righe_evento/attachment=%s",
            f"{requestid_rows:,}".replace(",", "."),
            f"{silver_rows:,}".replace(",", "."),
        )
        # Il perimetro requestId ha ormai esaurito il suo scopo.
        perim_requestid.unpersist(blocking=False)

        # Gli ultimi eventi A/B/C derivano dal dataset piatto già materializzato.
        latest_events_sql = """
            WITH classificati AS (
                SELECT
                    requestid,
                    statuscode,
                    statusdatetime,
                    clientrequesttimestamp,
                    CASE
                        WHEN statuscode IN (
                            'RECRN001C','RECRN002C','RECRN002F','RECRN003C','RECRN004C','RECRN005C',
                            'RECAG001C','RECAG002C','RECAG003C','RECAG003F','RECAG005C','RECAG006C','RECAG007C','RECAG008C',
                            'RECRI003C','RECRI004C'
                        ) THEN 'FINE_RECAPITO'
                        WHEN statuscode IN (
                            'RECRN001A','RECRN002A','RECRN002D','RECRN003A','RECRN004A','RECRN005A',
                            'RECAG001A','RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A','RECAG007A','RECAG008A',
                            'RECRI003A','RECRI004A'
                        ) THEN 'CERTIFICAZIONE_RECAPITO'
                        WHEN statuscode IN (
                            'RECRN001B','RECRN002B','RECRN002E','RECRN003B','RECRN004B','RECRN005B',
                            'RECAG001B','RECAG002B','RECAG003B','RECAG003E','RECAG005B','RECAG006B','RECAG007B','RECAG008B',
                            'RECRI003B','RECRI004B'
                        ) THEN 'DEMAT_RECAPITO'
                        ELSE NULL
                    END AS tipo
                FROM ssda526_silver_flat
                WHERE statuscode IS NOT NULL
            ),
            ranked AS (
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
                FROM classificati
                WHERE tipo IS NOT NULL
            )
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
            FROM ranked
            WHERE rn = 1
            GROUP BY requestid
        """
        latest_events, latest_rows = persist_and_materialize(
            spark.sql(latest_events_sql),
            "temp_ultimi_eventi_silver_postalizzazione",
            "6b/7 Ultimi eventi SILVER per requestId",
        )

        # DocumentType: stesso criterio della versione di input, ma senza una nuova
        # lettura di silver_postalizzazione.
        temp_demat_sql = """
            SELECT requestid, documenttype, rn_demat
            FROM (
                SELECT
                    requestid,
                    documenttype,
                    ROW_NUMBER() OVER (
                        PARTITION BY requestid
                        ORDER BY clientrequesttimestamp DESC
                    ) AS rn_demat
                FROM ssda526_silver_flat
                WHERE documenttype IN ('23L', 'AR')
            ) x
        """
        temp_demat, demat_rows = persist_and_materialize(
            spark.sql(temp_demat_sql),
            "temp_demat",
            "6c/7 DocumentType DEMAT dal SILVER già esploso",
            StorageLevel.MEMORY_ONLY,
        )
        logging.info(
            "[SPARK] Derivazioni SILVER: ultimi_eventi=%s, righe_demat=%s",
            f"{latest_rows:,}".replace(",", "."),
            f"{demat_rows:,}".replace(",", "."),
        )
        # Ora entrambe le derivazioni necessarie sono materializzate: il dataset
        # SILVER piatto può essere liberato prima dei controlli finali.
        silver_flat.unpersist(blocking=False)

        wi7 = spark.sql(
            "SELECT DISTINCT requestid FROM send_dev.wi7_poste_da_escludere"
        )
        wi7.createOrReplaceTempView("wi7_poste")

        # ------------------------------------------------------------------
        # 5) Controlli e classificazione: logica business invariata.
        # ------------------------------------------------------------------
        final_started = log_step_start(
            "7/7 Controlli business, classificazione e aggregazione"
        )
        final_sql = """
            WITH
        -- Il timestamp e' della notifica (IUN), mentre il conteggio finale
        -- rimane per postalizzazione (requestid). Raggruppiamo preventivamente
        -- per IUN per impedire moltiplicazioni nella LEFT JOIN.
        perimetro_notifiche AS (
            SELECT DISTINCT iun
            FROM gold_base_senza_feedback
            WHERE iun IS NOT NULL
        ),
        notifiche_visualizzate AS (
            SELECT n.iun,
                   MAX(CASE WHEN n.tms_viewed IS NOT NULL THEN 1 ELSE 0 END)
                       AS flag_notifica_visualizzata
            FROM send.gold_notification_analytics n
            INNER JOIN perimetro_notifiche p ON n.iun = p.iun
            GROUP BY n.iun
        ),
        temp_controlli AS (
            SELECT
                -- Manteniamo solo i campi necessari ai controlli e alla classificazione.
                s.requestid,
                -- Il flag nasce dalla GOLD notifiche, aggregata per IUN nella CTE
                -- notifiche_visualizzate. Qui l'alias n e' effettivamente disponibile.
                COALESCE(n.flag_notifica_visualizzata, 0) AS flag_notifica_visualizzata,
                s.prodotto,
                s.tentativo_recapito_stato,
                s.tentativo_recapito_data,
                s.messaingiacenza_recapito_stato,
                s.certificazione_recapito_stato,
                s.certificazione_recapito_dettagli,
                s.certificazione_recapito_data,
                s.demat_23l_ar_stato,
                s.demat_plico_stato,
                s.demat_documenttype_da_controllare,
                s.demat_arcad_stato,
                s.demat_arcad_data_rendicontazione,
                s.fine_recapito_stato,
                s.accettazione_23l_recag012_data,
                s.flag_wi7_consolidatore,
                s.flag_wi7_report_postalizzazioni_incomplete,
                CASE WHEN w.requestid IS NOT NULL THEN 1 ELSE 0 END AS flag_wi7_poste,
                s.attempt_number,
                CASE
                    WHEN s.certificazione_recapito_dettagli IN ('M02') THEN 1
                    ELSE 0
                END AS flag_destinatario_deceduto,
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
            LEFT JOIN notifiche_visualizzate n
                ON s.iun = n.iun
            LEFT JOIN temp_demat d
                ON d.requestid = s.requestid
            AND d.rn_demat = 1
            LEFT JOIN temp_ultimi_eventi_silver_postalizzazione t
                ON s.requestid = t.requestid
            LEFT JOIN wi7_poste w
                ON s.requestid = w.requestid
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
            -- Per l'aggregato manteniamo solo la chiave, il flag di esclusione finale
            -- e il cluster. Tutti i campi diagnostici restano calcolati solo finché
            -- servono ai CASE di classificazione.
            SELECT
                t.requestid,
                t.flag_notifica_visualizzata,
                t.flag_destinatario_deceduto,
                CASE
                    -- I tavoli Poste mantengono la precedenza già prevista dal report aggregato.
                    WHEN flag_wi7_poste = 1
                        THEN 'Oggetto rientrante in tavoli Duplicati / Non Rendicontabili'
                    WHEN (flag_wi7_report_postalizzazioni_incomplete = 1 OR flag_wi7_consolidatore = 1)
                         AND flag_wi7_poste = 0
                        THEN 'Oggetto fuori sla'
                    WHEN (
                        controllo_causale = 1
                        OR controllo_date_business = 1
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
                        AND CAST(t.attempt_number AS INT) = 0
                    )
                        THEN 'Oggetto con rendicontazione corretta - Bloccato al primo attempt'
                    WHEN (
                        controllo_causale = 0
                        AND controllo_date_business = 0
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
                        AND CAST(t.attempt_number AS INT) = 1
                    )
                        THEN 'Oggetto con rendicontazione corretta - Bloccato al secondo attempt'
                    ELSE 'Oggetto in corso di postalizzazione'
                END AS cluster_mancato_perfezionamento
            FROM temp_assenza_eventi t
        ),

        finale_filtrato AS (
            -- A questo punto tutti i filtri tecnici di perimetro sono già stati
            -- applicati a monte. Per l'aggregato servono solo chiave e cluster.
            SELECT
                requestid,
                flag_notifica_visualizzata,
                cluster_mancato_perfezionamento
            FROM finale
        )

        SELECT
            cluster_mancato_perfezionamento,
            COUNT(DISTINCT requestid) AS numero_oggetti,
            COUNT(DISTINCT CASE WHEN flag_notifica_visualizzata = 1
                                THEN requestid END)
                AS numero_oggetti_con_notifica_visualizzata
        FROM finale_filtrato
        GROUP BY cluster_mancato_perfezionamento
        ;

        """
        df_spark = spark.sql(final_sql)
        logging.info("[SPARK] Conversione del solo aggregato finale in pandas...")
        result = df_spark.toPandas()
        log_step_end(
            "7/7 Controlli business, classificazione e aggregazione",
            final_started,
            len(result),
        )
        logging.info(
            "[SPARK] Query SSDA-526 completata in %.1f s - cluster restituiti=%s",
            perf_counter() - run_start,
            len(result),
        )
        return result

    finally:
        logging.info("[SPARK] Rilascio dataset persistiti (%s)...", len(persisted))
        for df in reversed(persisted):
            try:
                df.unpersist(blocking=False)
            except Exception:
                logging.warning(
                    "[SPARK] Impossibile rilasciare un dataset persistito.",
                    exc_info=True,
                )


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
    """Ordine business esplicito usato da GSheet, tabella, colori e grafico."""
    return CLUSTER_ORDER.copy()


def normalize_current_data(df: pd.DataFrame) -> pd.DataFrame:
    expected_columns = {
        "cluster_mancato_perfezionamento",
        "numero_oggetti",
        VIEWED_COLUMN,
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

    result[VIEWED_COLUMN] = (
        pd.to_numeric(result[VIEWED_COLUMN], errors="raise").fillna(0).astype("int64")
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

    Lo storico legacy viene mantenuto senza aggiungere artificialmente i cluster
    della nuova baseline. La non confrontabilità viene gestita da calculate_delta.
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
    # Celle legacy lasciate vuote: non sostituire l'assenza con 0 o con 'N/A'.
    history[VIEWED_COLUMN] = pd.to_numeric(
        history[VIEWED_COLUMN].replace("", pd.NA), errors="coerce"
    ).astype("Int64")
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
    Calcola il delta solo quando lo snapshot precedente appartiene già alla nuova
    baseline a sei cluster e la Data max deposito coincide.

    La prima esecuzione con la nuova classificazione restituisce quindi ``n.d.``
    per tutti i cluster e per il totale. Dalla settimana successiva il confronto
    riparte automaticamente sulla nuova baseline, senza alterare lo storico legacy.
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

    previous_clusters = set(
        previous["cluster_mancato_perfezionamento"].astype(str).str.strip()
    )
    current_schema = set(CLUSTER_ORDER)
    if previous_clusters != current_schema:
        logging.info(
            "Nuova baseline SSDA-526: schema precedente non confrontabile (%s). "
            "Delta impostato a n.d. per tutti i cluster.",
            previous_week,
        )
        return result, False, None, previous_week

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
    return result, True, current_total - previous_total, previous_week


def build_current_history_snapshot(
    current_df: pd.DataFrame,
    week_reference: str,
    week_start: str,
    execution_utc: str,
    period_reference: str,
) -> pd.DataFrame:
    # Il fallback legacy può non possedere la metrica, introdotta con la nuova baseline.
    snapshot = current_df.reindex(
        columns=["cluster_mancato_perfezionamento", "numero_oggetti", VIEWED_COLUMN]
    ).copy()
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
    order_map = {cluster: idx for idx, cluster in enumerate(CLUSTER_ORDER)}
    result["cluster_sort"] = result["cluster_mancato_perfezionamento"].map(
        lambda cluster: order_map.get(cluster, len(CLUSTER_ORDER) + 100)
    )
    result["cluster_label_sort"] = result["cluster_mancato_perfezionamento"].map(
        lambda cluster: CLUSTER_META.get(cluster, {})
        .get("label", LEGACY_CLUSTER_LABELS.get(cluster, str(cluster)))
        .casefold()
    )
    result = result.sort_values(
        ["data_inizio_settimana_parsed", "cluster_sort", "cluster_label_sort"],
        kind="stable",
    ).drop(
        columns=["data_inizio_settimana_parsed", "cluster_sort", "cluster_label_sort"]
    )
    return result[HISTORY_COLUMNS].reset_index(drop=True)


def validate_report(df: pd.DataFrame) -> None:
    if len(df) != len(CLUSTER_META):
        raise ValueError(
            f"Numero cluster inatteso: {len(df)}; attesi {len(CLUSTER_META)}."
        )
    if (df["numero_oggetti"] < 0).any():
        raise ValueError("Sono presenti conteggi negativi.")
    if ((df[VIEWED_COLUMN] < 0) | (df[VIEWED_COLUMN] > df["numero_oggetti"])).any():
        raise ValueError(
            "Conteggio visualizzate non coerente con il totale per cluster."
        )


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
                format_integer_it(getattr(row, VIEWED_COLUMN)),
                delta_text,
            ]
        )
        delta_colors.append(delta_color)

    total_objects = int(report_df["numero_oggetti"].sum())
    total_viewed = int(report_df[VIEWED_COLUMN].sum())
    total_delta_text, _ = format_delta(total_delta, total_delta_available)
    rows.append(
        [
            "Totale",
            format_integer_it(total_objects),
            format_integer_it(total_viewed),
            total_delta_text,
        ]
    )

    columns = [
        "Cluster di mancato perfezionamento",
        "Numero oggetti",
        "Di cui con notifica\nvisualizzata",
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
        colWidths=[0.63, 0.10, 0.16, 0.11],
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
            if col_idx in (1, 2, 3):
                cell.get_text().set_fontsize(10.0)
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
        if row_idx > 0 and col_idx in (1, 2, 3):
            cell.get_text().set_fontsize(NUMERIC_FONT_SIZE)
            cell.get_text().set_style("italic")

        if 0 < row_idx < total_row_index and col_idx == 3:
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


def _adjust_small_label_positions(
    points: list[dict], min_gap: float = 0.22, y_limit: float = 1.10
) -> None:
    """Distribuisce le etichette esterne evitando sovrapposizioni sullo stesso lato."""
    for side in (-1, 1):
        side_points = sorted(
            [point for point in points if point["side"] == side],
            key=lambda point: point["y"],
        )
        if not side_points:
            continue

        # Passaggio dal basso verso l'alto.
        side_points[0]["y"] = max(side_points[0]["y"], -y_limit)
        for idx in range(1, len(side_points)):
            side_points[idx]["y"] = max(
                side_points[idx]["y"], side_points[idx - 1]["y"] + min_gap
            )

        # Se l'ultima etichetta supera il limite, ricompattiamo dall'alto verso il basso.
        if side_points[-1]["y"] > y_limit:
            side_points[-1]["y"] = y_limit
            for idx in range(len(side_points) - 2, -1, -1):
                side_points[idx]["y"] = min(
                    side_points[idx]["y"], side_points[idx + 1]["y"] - min_gap
                )


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
                        "is_other": cluster_label == "Altri",
                    }
                )

        _adjust_small_label_positions(small_labels)
        for item in small_labels:
            angle = item["angle"]
            cos_angle = math.cos(angle)
            x_anchor = 0.98 * cos_angle
            y_anchor = 0.98 * math.sin(angle)
            radial_text = 1.30 if item.get("is_other") else 1.15
            x_text = radial_text * cos_angle
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


def render_viewed_donut(
    report_df: pd.DataFrame, output_path: Path, show_title: bool = True
) -> None:
    """Donut per postalizzazioni, non per IUN distinti.

    La fetta teal e' centrata sul semipiano sinistro (ore 9), coerentemente
    con la prima voce della legenda. La rotazione non tocca alcun testo.
    """
    total = int(report_df["numero_oggetti"].sum())
    viewed = int(report_df[VIEWED_COLUMN].sum())
    # Il COUNT DISTINCT condizionato prende un sottoinsieme degli stessi
    # requestId della metrica principale: non aggiungiamo assert ridondanti.
    fig, ax = plt.subplots(figsize=(5.2, 5.0), dpi=IMAGE_DPI)
    ax.set_aspect("equal")
    ax.axis("off")
    if total:
        viewed_angle = 360.0 * viewed / total
        # In Matplotlib, con counterclock=False, il centro del primo settore
        # e' startangle - viewed_angle/2. Lo centriamo a 180 gradi.
        viewed_startangle = (180.0 + viewed_angle / 2.0) % 360.0
        ax.pie(
            [viewed, total - viewed],
            colors=[VIEWED_COLOR, NOT_VIEWED_COLOR],
            startangle=viewed_startangle,
            counterclock=False,
            wedgeprops={"width": 0.38, "edgecolor": "white", "linewidth": 2},
        )
        percent = 100.0 * viewed / total
    else:
        ax.add_patch(
            plt.Circle(
                (0, 0), 0.8, fill=False, linewidth=22, edgecolor=NOT_VIEWED_COLOR
            )
        )
        percent = 0.0
    ax.text(
        0,
        0.08,
        format_percentage_it(percent),
        ha="center",
        va="center",
        fontsize=21,
        weight="bold",
        color=TEXT_COLOR,
    )
    ax.text(
        0,
        -0.20,
        "visualizzate",
        ha="center",
        va="center",
        fontsize=11,
        color=TEXT_COLOR,
    )
    if show_title:
        ax.set_title(
            "Visualizzazione della notifica",
            fontsize=13,
            weight="bold",
            color=TEXT_COLOR,
            pad=8,
        )
    from matplotlib.patches import Patch

    ax.legend(
        handles=[
            Patch(
                facecolor=VIEWED_COLOR,
                label=f"Visualizzate: {format_integer_it(viewed)}",
            ),
            Patch(
                facecolor=NOT_VIEWED_COLOR,
                label=f"Non visualizzate: {format_integer_it(total - viewed)}",
            ),
        ],
        loc="lower center",
        bbox_to_anchor=(0.5, -0.15),
        ncol=2,
        frameon=False,
        fontsize=9,
    )
    # Il grafico e' volutamente quasi quadrato, per non risultare minuscolo
    # quando viene inserito nella composizione verticale a due donut.
    fig.savefig(output_path, bbox_inches="tight", pad_inches=0.10, facecolor="white")
    plt.close(fig)


def _title_font_for_composite(px: int):
    """Font PIL per titoli compatti; fallback se Montserrat non disponibile."""
    font_path = font_manager.findfont(
        font_manager.FontProperties(family=FONT_FAMILY, weight="bold"),
        fallback_to_default=True,
    )
    try:
        return ImageFont.truetype(font_path, size=px)
    except (OSError, ValueError):
        return ImageFont.load_default()


def render_combined_viewed(
    table_path: Path,
    legend_path: Path,
    chart_path: Path,
    viewed_chart_path: Path,
    output_path: Path,
) -> None:
    """Variante con due grafici: 64% principale, 33% visualizzazioni, 3% gap.

    Le percentuali si riferiscono all'altezza della tabella. All'interno
    di ciascuna fascia il titolo occupa pochissimo spazio; si usa il
    rapporto d'aspetto originale dei PNG senza deformarne testo e fette.
    """
    table_img = Image.open(table_path).convert("RGB")
    legend_img = Image.open(legend_path).convert("RGB")
    chart_img = Image.open(chart_path).convert("RGB")
    viewed_img = Image.open(viewed_chart_path).convert("RGB")
    height = table_img.height
    if legend_img.height != height:
        width = max(1, round(legend_img.width * height / legend_img.height))
        legend_img = legend_img.resize((width, height), Image.Resampling.LANCZOS)

    main_slot = round(height * 0.64)
    small_slot = round(height * 0.33)
    vertical_gap = height - main_slot - small_slot
    title_height = max(22, round(height * 0.052))
    main_available = main_slot - title_height
    small_available = small_slot - title_height

    # Dimensioni naturali: il secondo donut arriva a ~70-75% del diametro
    # del principale. Niente stiramenti anisotropi delle immagini.
    main_width = round(chart_img.width * main_available / chart_img.height)
    small_width = round(viewed_img.width * small_available / viewed_img.height)
    chart_width = max(main_width, small_width)
    horizontal_gap = 28
    canvas = Image.new(
        "RGB",
        (
            legend_img.width + table_img.width + horizontal_gap + chart_width,
            height,
        ),
        "white",
    )
    canvas.paste(legend_img, (0, 0))
    canvas.paste(table_img, (legend_img.width, 0))
    x0 = legend_img.width + table_img.width + horizontal_gap

    draw = ImageDraw.Draw(canvas)
    font = _title_font_for_composite(max(18, round(height * 0.025)))
    fill = TEXT_COLOR

    def paste_slot(img: Image.Image, y_top: int, img_h: int, title: str):
        # Ancoraggio orizzontale centralizzato per grafici e titoli.
        draw.text(
            (x0 + chart_width // 2, y_top + title_height // 2),
            title,
            font=font,
            fill=fill,
            anchor="mm",
        )
        width = max(1, round(img.width * img_h / img.height))
        resized = img.resize((width, img_h), Image.Resampling.LANCZOS)
        canvas.paste(resized, (x0 + (chart_width - width) // 2, y_top + title_height))

    paste_slot(chart_img, 0, main_available, "Postalizzazioni per cluster")
    paste_slot(
        viewed_img,
        main_slot + vertical_gap,
        small_available,
        "Visualizzazione della notifica",
    )
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
    render_viewed_donut(report_df, paths["viewed_chart"])
    # Variante senza titolo interno, perche' nel completo il titolo e'
    # disegnato direttamente nella fascia del grafico (no doppio titolo).
    viewed_chart_compact = OUTPUT_DIR / "_ssda526_viewed_for_combined.png"
    render_viewed_donut(report_df, viewed_chart_compact, show_title=False)
    render_combined(
        paths["table"],
        paths["legend"],
        paths["chart"],
        paths["combined"],
    )
    render_combined_viewed(
        paths["table"],
        paths["legend"],
        paths["chart"],
        viewed_chart_compact,
        paths["combined_viewed"],
    )
    # Artefatto temporaneo locale: non viene caricato su Drive.
    viewed_chart_compact.unlink(missing_ok=True)

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
    # I campi storici privi di rilevazione restano celle vuote sul GSheet.
    export_df = df.astype(object).where(pd.notna(df), "").astype(str)
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

        # Se lo storico non esiste, il tab corrente viene materializzato come
        # settimana precedente. La prima esecuzione della nuova baseline resta comunque
        # non confrontabile: calculate_delta imposta n.d. su tutti i sei cluster.
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
                VIEWED_COLUMN,
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
