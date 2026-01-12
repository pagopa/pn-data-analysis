import time
from pyspark.sql import SparkSession
from pdnd_google_utils import Sheet
import pandas as pd
import json
import os
import logging
from datetime import datetime

# ---------------- CONFIGURAZIONE BASE ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"
# Versione L3
SHEET_ID_L3 = "1s83mCJAC3Yxye2pBlNaPPEFUkHC8U4eCuhNdZxe1T0I"
# Versione General
SHEET_ID_L4 = "1B_8YkdtruSqCtkrukjHtAqPUnrelNrGySYmhv4t5Myc" 

#Versione Recapitisti
SHEET_ID_Fulmine = "1qeNYn3al9iFVlJ7rlR_zO9NXLld313rsx3aFlHL0ekA"
SHEET_ID_Poste = "1idDmew64tfxfI7-KdI4BrYwD-xWm5PxINTsY6rWQoQs"
SHEET_ID_Sailpost = "1JSIum1WhNXJ1CZ-TAuvs8GbeDqeITNqLduVzeLUUInI"
SHEET_ID_Po_Se = "1FDVU_LyK_Ch9DJBsOni0k-4mEKeH7pREc3UiUkSclo8"

# ---------------- FUNZIONI ----------------

def load_google_credentials(secret_path: str) -> dict:
    """Legge i file di credenziali e li restituisce come dizionario."""
    creds = {
        name: open(os.path.join(secret_path, name)).read().strip()
        for name in os.listdir(secret_path)
        if os.path.isfile(os.path.join(secret_path, name))
    }
    logging.info(f"Credenziali caricate: {list(creds.keys())}")
    return creds


def build_queries() -> dict:
    """Costruisce la query base e le versioni filtrate per ciascun recapitista."""

    # Query base -- presa la versione CDE con Lateral View
    query_sql_base = """
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
            ),
        ---- 1° CTE temp_silver_postalizzazione: prendo la silver_postalizzazione e applico il row number su tutti gli stati della certificazione e fine recapito
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
                            'RECRN003C','RECRN004C','RECRN005C','RECRN001C','RECRN002C','RECRN002F','RECAG001C',
                            'RECAG002C','RECAG003C','RECAG003F','RECAG005C','RECAG006C','RECAG007C','RECAG008C'
                        ) THEN 'FINE_RECAPITO'
                        WHEN e.paperprogrstatus.statuscode IN (
                            'RECRN003A','RECRN004A','RECRN005A','RECRN001A','RECRN002A','RECRN002D','RECAG001A',
                            'RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A','RECAG007A','RECAG008A'
                        ) THEN 'CERTIFICAZIONE_RECAPITO'
                        ELSE null
                    END AS tipo
                FROM send.silver_postalizzazione t
                LATERAL VIEW EXPLODE(t.eventslist) AS e 
                WHERE e.paperprogrstatus.statuscode IN (
                    'RECRN001A','RECRN002A','RECRN002D','RECRN003A','RECRN004A','RECRN005A','RECAG001A',
                    'RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A','RECAG007A','RECAG008A',
                    'RECRN001C','RECRN002C','RECRN002F','RECRN003C','RECRN004C','RECRN005C','RECAG001C',
                    'RECAG002C','RECAG003C','RECAG003F','RECAG005C','RECAG006C','RECAG007C','RECAG008C'
                )
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
                    MAX( CASE WHEN tipo = 'FINE_RECAPITO' THEN statuscode END ) AS fine_recapito_stato,
                    MAX( CASE WHEN tipo = 'FINE_RECAPITO' THEN statusdatetime END ) AS fine_recapito_data,
                    MAX( CASE WHEN tipo = 'FINE_RECAPITO' THEN clientrequesttimestamp END ) AS fine_recapito_rendicontazione,
                    MAX( CASE WHEN tipo = 'CERTIFICAZIONE_RECAPITO' THEN statuscode END ) AS certificazione_recapito_stato,
                    MAX( CASE WHEN tipo = 'CERTIFICAZIONE_RECAPITO' THEN statusdatetime END ) AS certificazione_recapito_data,
                    MAX( CASE WHEN tipo = 'CERTIFICAZIONE_RECAPITO' THEN clientrequesttimestamp END ) AS certificazione_recapito_rendicontazione  
                FROM
                    ---- FIX inserita vista sulla silver_postalizzazione
                    vista_silver_postalizzazione
                WHERE
                    rn = 1
                GROUP BY
                    requestid
            ),
            temp_postalizzazione_e_controlli AS (
                SELECT
                    s.senderpaid,
                    sn.senderdenomination, -- FIX: aggiunto il senderdenomination della silver_notification
                    s.iun,
                    s.requestid,
                    s.requesttimestamp,
                    s.prodotto,
                    s.geokey,
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
                    LEAST (
                        COALESCE(n.tms_viewed, n.tms_effective_date),
                        COALESCE(n.tms_effective_date, n.tms_viewed)
                    ) AS tms_perfezionamento_notification,
                    CASE
                        WHEN tl.category = 'SCHEDULE_REFINEMENT' THEN 1
                        ELSE 0
                    END AS flag_schedule_refinement,
                    s.tms_cancelled,
                    n.tms_date_payment,
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
                        WHEN s.certificazione_recapito_stato NOT IN ('RECRS002A','RECRN002A','RECAG003A','RECRS002D','RECRN002D','RECAG003D')
                            AND s.certificazione_recapito_dettagli IN ('M01','M03','M04','M02','M05','M06','M07','M08','M09') THEN 1
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
                        WHEN s.certificazione_recapito_stato IN ('RECRN003A', 'RECRN004A', 'RECRN005A')
                            AND (s.tentativo_recapito_stato = 'RECRN010' OR s.tentativo_recapito_stato IS NULL) THEN 0
                        WHEN s.certificazione_recapito_stato IN ('RECRS003A', 'RECRS004A', 'RECRS005A')
                            AND (s.tentativo_recapito_stato = 'RECRS010' OR s.tentativo_recapito_stato IS NULL) THEN 0
                        WHEN s.certificazione_recapito_stato IN ('RECAG005A','RECAG006A','RECAG007A','RECAG008A')
                            AND (s.tentativo_recapito_stato = 'RECAG010' OR s.tentativo_recapito_stato IS NULL) THEN 0
                        WHEN s.certificazione_recapito_stato NOT IN ('RECRN003A','RECRN004A','RECRN005A','RECAG005A','RECRS003A','RECRS004A','RECRS005A','RECAG006A','RECAG007A','RECAG008A')
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
                                    OR s.demat_arcad_stato IN ('RECAG011B', 'RECAG005B')
                                    )
                                AND t.fine_recapito_stato = 'RECAG005C' THEN 0
                        WHEN s.certificazione_recapito_stato = 'RECAG006A'
                                AND (s.demat_23l_ar_stato IN ('RECAG011B', 'RECAG006B')
                                    OR s.demat_plico_stato IN ('RECAG011B', 'RECAG006B')
                                    OR s.demat_arcad_stato IN ('RECAG011B', 'RECAG006B')
                                    )
                                AND t.fine_recapito_stato = 'RECAG006C' THEN 0
                        WHEN s.certificazione_recapito_stato = 'RECAG007A'
                                AND (s.demat_23l_ar_stato IN ('RECAG011B', 'RECAG007B')
                                    OR s.demat_plico_stato IN ('RECAG011B', 'RECAG007B')
                                    OR s.demat_arcad_stato IN ('RECAG011B', 'RECAG007B')
                                    )
                                AND t.fine_recapito_stato = 'RECAG007C' THEN 0
                        WHEN s.certificazione_recapito_stato = 'RECAG008A'
                                AND (s.demat_23l_ar_stato IN ('RECAG011B', 'RECAG008B')
                                    OR s.demat_plico_stato IN ('RECAG011B', 'RECAG008B')
                                    OR s.demat_arcad_stato IN ('RECAG011B', 'RECAG008B')
                                    )
                                AND t.fine_recapito_stato = 'RECAG008C' THEN 0
                        ELSE 1
                        END AS controllo_tripletta,
                    CASE
                        WHEN CAST(t.certificazione_recapito_data AS TIMESTAMP) = CAST(t.fine_recapito_data AS TIMESTAMP) THEN 0
                        ELSE 1
                    END AS controllo_date_business,
                    CASE
                        WHEN s.certificazione_recapito_stato = 'RECRN005A'
                        AND DATEDIFF (
                            CAST(s.certificazione_recapito_data AS DATE),
                            CAST(s.tentativo_recapito_data AS DATE)
                        ) < 30 THEN 1
                        ELSE 0
                    END AS controllo_tempistiche_compiuta_giacenza,
                    CASE
                        WHEN s.prodotto = '890' AND d.documenttype = '23L' THEN 0
                        WHEN s.prodotto = 'AR'  AND d.documenttype = 'AR'  THEN 0
                        WHEN d.documenttype IS NULL THEN 0
                        ELSE 1
                    END AS controllo_documentType
                FROM
                    send.gold_postalizzazione_analytics s
                    LEFT JOIN temp_demat d ON d.requestid = s.requestid AND d.rn_demat = 1
                    LEFT JOIN send.silver_notification sn ON (sn.iun = s.iun)
                    LEFT JOIN send.gold_notification_analytics n ON (s.iun = n.iun)
                    LEFT JOIN send.silver_timeline tl ON (s.iun = tl.iun AND tl.category = 'SCHEDULE_REFINEMENT')
                    ---- FIX inserita vista sui massimi eventi della silver_postalizzazione
                    LEFT JOIN max_events_silver_postalizzazione t ON (s.requestid = t.requestid)        
                WHERE
                    s.requestid NOT IN ( SELECT requestid FROM send_dev.wi7_poste_da_escludere )
                    AND s.scarto_consolidatore_stato IS NULL
                    AND s.fine_recapito_stato IS NOT NULL
                    AND s.flag_prodotto_estero = 0
                    AND s.statusrequest NOT IN ('PN999', 'PN998') --- FIX statusrequest ora è presente direttamente a livello della gold quindi posso prenderlo direttamente da li 
            ),
            temp_postalizzazione AS (
                SELECT
                    s.*,
                    IF (
                        fine_recapito_stato IN (
                            'RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C','RECAG005C','RECAG006C','RECAG007C','RECAG008C'
                        )
                        AND tentativo_recapito_stato IS NULL,
                        1,
                        0
                    ) AS assenza_inesito,
                    IF (
                        fine_recapito_stato IN (
                            'RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C','RECAG005C','RECAG006C','RECAG007C','RECAG008C'
                        )
                        AND messaingiacenza_recapito_stato IS NULL,
                        1,
                        0
                    ) AS assenza_messa_in_giacenza,
                    IF (
                        certificazione_recapito_stato IS NULL,
                        1,
                        0
                    ) AS assenza_pre_esito,
                    IF (
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
                        ),
                        1,
                        0
                    ) AS assenza_dematerializzazione_23l_ar_plico,
                    IF (
                        prodotto = '890'
                        AND fine_recapito_stato IN (
                            'RECAG005C','RECAG006C','RECAG007C','RECAG008C'
                        )
                        AND demat_arcad_data_rendicontazione IS NULL,
                        1,
                        0
                    ) AS assenza_demat_ARCAD,
                    IF (
                        prodotto = '890'
                        AND fine_recapito_stato IN (
                            'RECAG005C','RECAG006C','RECAG007C','RECAG008C'
                        )
                        AND accettazione_23l_recag012_data IS NULL,
                        1,
                        0
                    ) AS assenza_RECAG012
                ---- FIX riferimento alla cte temp_postalizzazione_e_controlli
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
                    OR assenza_demat_ARCAD = 1
                    OR assenza_RECAG012 = 1,
                    1,
                    0
                ) AS flag_esiti_mancanti
                FROM
                    temp_postalizzazione
            ),
            finale AS (
                SELECT
                    t.senderpaid,
                    t.senderdenomination, -- FIX inserito il senderdenomination
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
                    t.tms_perfezionamento_notification,
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
				        OR controllo_documentType = 1 THEN 1
                        ELSE 0
                    END AS flag_errore_rendicontazione,
                    CONCAT_WS (
                        ', ',
                        CASE WHEN controllo_documentType = 1 THEN 'errore documenttype' END,
                        CASE WHEN controllo_causale = 1 THEN 'errore rend. causale' END,
                        CASE WHEN controllo_date_business = 1 THEN 'errore rend. date business' END,
                        CASE WHEN controllo_tripletta = 1 THEN 'errore rend. tripletta' END,
                        CASE WHEN controllo_tempistiche_compiuta_giacenza = 1 THEN 'errore rend. tempistiche compiuta giacenza' END,
                        CASE WHEN controllo_inesito_casi_giacenza = 1 THEN 'errore rend. inesito casi giacenza' END,
                        CASE WHEN assenza_inesito =1  THEN 'assenza inesito' END,
                        CASE WHEN assenza_messa_in_giacenza =1 THEN 'assenza messa in giacenza' END,
                        CASE WHEN assenza_pre_esito = 1 THEN 'assenza pre-esito' END,
                        CASE WHEN assenza_dematerializzazione_23l_ar_plico = 1 THEN 'assenza demat 23l_ar / plico' END,
                        CASE WHEN assenza_demat_ARCAD = 1 THEN 'assenza demat ARCAD' END,
                        CASE WHEN assenza_RECAG012 = 1 THEN 'assenza RECAG012' END
                    ) AS Causa_mancato_perfezionamento,
                    t.controllo_date_business,
                    t.controllo_tripletta,
                    t.controllo_tempistiche_compiuta_giacenza,
                    t.controllo_inesito_casi_giacenza,
                    t.controllo_documentType,
                    t.assenza_inesito,
                    t.assenza_messa_in_giacenza,
                    t.assenza_pre_esito,
                    t.assenza_dematerializzazione_23l_ar_plico,
                    t.assenza_demat_ARCAD,
                    t.assenza_RECAG012
                FROM
                    temp_assenza_eventi t
            ),
            finale_filtrato AS (
                SELECT *
                FROM finale f
                WHERE
                    flag_ultima_postalizzazione = 1
                    AND tms_cancelled IS NULL
                    AND flag_schedule_refinement = 0
                    AND certificazione_recapito_stato NOT IN ('RECRS006','RECRS013','RECRN006','RECRN013','RECAG004','RECAG013')
                    AND (
                        tentativo_recapito_stato NOT IN ('PN998', 'PN999')
                        OR certificazione_recapito_stato NOT IN ('PN998', 'PN999')
                        OR fine_recapito_stato NOT IN ('PN998', 'PN999')
                    )
                    AND tms_perfezionamento_notification IS NULL
                    AND (
                        flag_errore_rendicontazione = 1
                        OR flag_esiti_mancanti = 1
                    )
            )
    """
    # Query VGeneral
    query_general = query_sql_base + """
    SELECT DISTINCT * 
    FROM finale_filtrato;
    """

    # Base per recapitisti
    query_base_recapitisti = query_sql_base + """
    SELECT DISTINCT
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
        Causa_mancato_perfezionamento
    FROM finale_filtrato
    """

    # Query derivate con filtro per recapitista
    query_fulmine = query_base_recapitisti + """
    WHERE recapitista_unif = 'Fulmine'
    """

    query_poste = query_base_recapitisti + """
    WHERE recapitista_unif = 'Poste'
    """

    query_post_service = query_base_recapitisti + """
    WHERE recapitista_unif = 'POST & SERVICE'
    """

    query_sailpost = query_base_recapitisti + """
    WHERE recapitista_unif = 'RTI Sailpost-Snem'
    """

    # Query per L3 - senza filtro dei recapitisti
    query_l3 = query_sql_base + """
    SELECT DISTINCT
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
        Causa_mancato_perfezionamento
    FROM finale_filtrato;
    """

    return {
        "general" : query_general,
        "Fulmine": query_fulmine,
        "Poste": query_poste,
        "POST & SERVICE": query_post_service,
        "RTI Sailpost-Snem": query_sailpost,
        "L3": query_l3
    }

# FUNZIONI AUSILIARIE

def run_query(spark: SparkSession, query_sql: str) -> pd.DataFrame:
    """Esegue la query su Spark e restituisce un DataFrame Pandas."""
    logging.info("Esecuzione query su Spark...")
    df_spark = spark.sql(query_sql)
    logging.info("Trasformazione in Pandas DataFrame...")
    return df_spark.toPandas()


def get_last_update_date(spark: SparkSession) -> str:
    """Estrae la data più recente (MAX(requesttimestamp)) dal dataset."""
    logging.info("Estrazione data ultimo aggiornamento...")

    max_date_df = spark.sql("SELECT MAX(requesttimestamp) AS max_ts FROM send.gold_postalizzazione_analytics")
    max_date = max_date_df.collect()[0]["max_ts"]

    if isinstance(max_date, datetime):
        return max_date.strftime("%Y-%m-%d %H:%M:%S")
    return str(max_date)

"""
def export_to_sheets(df: pd.DataFrame, creds: dict, sheet_id: str, sheet_name: str):
    
    logging.info(f"Scrittura su Google Sheet: {sheet_name}")
    df = df.astype(str)
    sheet = Sheet(sheet_id=sheet_id, service_credentials=creds, id_mode='key')
    sheet.upload(sheet_name, df)
    logging.info("Scrittura completata.")
"""

def export_to_sheets(df: pd.DataFrame, creds: dict, sheet_id: str, sheet_name: str):
    """
    Esporta i dati su Google Sheet a blocchi, utilizzando nativamente
    la funzione upload() della libreria pdnd_google_utils, senza sovrascrivere tutto.
    """
    logging.info(f"Scrittura su Google Sheet: {sheet_name}")
    df = df.fillna("").astype(str)

    sheet = Sheet(sheet_id=sheet_id, service_credentials=creds, id_mode='key')

    chunk_size = 10000
    total_rows = len(df)
    logging.info(f"Totale righe da caricare: {total_rows}")

    for i, start in enumerate(range(0, total_rows, chunk_size)):
        end = min(start + chunk_size, total_rows)
        chunk = df.iloc[start:end]

        # Calcola cella iniziale per il chunk
        ul_row = start + 1  # riga base
        ul_cell = f"A{ul_row}"

        # Se non è il primo chunk, disattiva header e pulizia automatica
        header = (i == 0)
        clear_on_open = (i == 0)

        try:
            logging.info(f"Caricamento righe {start}–{end} (ul_cell={ul_cell})...")
            sheet.upload(
                worksheet_name=sheet_name,
                panda_df=chunk,
                ul_cell=ul_cell,
                header=header,
                clear_on_open=clear_on_open,
            )
            logging.info(f" Chunk {i+1}: righe {start}–{end} caricate con successo.")
        except Exception as e:
            logging.error(f" Errore upload righe {start}–{end}: {e}")
            time.sleep(5)
            continue

    logging.info(" Tutti i chunk caricati correttamente su Google Sheet.")

# ---------------- MAIN SCRIPT ----------------

def main():
    logging.info("Inizializzazione SparkSession...")
    spark = SparkSession.builder.appName("Esiti da Bonificare").getOrCreate()

    spark_conf = spark.sparkContext.getConf().getAll()
    for k, v in spark_conf:
        logging.info(f"{k}: {v}")

    # Caricamento credenziali Google
    creds = load_google_credentials(GOOGLE_SECRET_PATH)
    #sheet = Sheet(sheet_id=SHEET_ID, service_credentials=creds, id_mode='key')

    # Costruisci tutte le query
    queries = build_queries()

    # Mappatura nome query --> sheet di destinazione
    SHEET_MAP = {
        "general": { "sheet_id": SHEET_ID_L4, "sheet_name": "Estrazione Esiti da Bonificare" },
        "L3": { "sheet_id": SHEET_ID_L3, "sheet_name": "Estrazione Esiti da Bonificare" },
        "Fulmine": { "sheet_id": SHEET_ID_Fulmine, "sheet_name": "Esiti da Bonificare - Fulmine" },
        "Poste": { "sheet_id": SHEET_ID_Poste, "sheet_name": "Esiti da Bonificare - Poste" },
        "POST & SERVICE": { "sheet_id": SHEET_ID_Po_Se, "sheet_name": "Esiti da Bonificare - Post & Service" },
        "RTI Sailpost-Snem": { "sheet_id": SHEET_ID_Sailpost, "sheet_name": "Esiti da Bonificare - Sailpost-Snem" }
    }

    # 1. Calcolo una volta le date del job
    job_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    max_date_str = get_last_update_date(spark)

    update_df = pd.DataFrame({
        "Ultimo aggiornamento dati (MAX requesttimestamp)": [max_date_str],
        "Data esecuzione job": [job_time]
    })

   
    # 2. Per ogni query → export + aggiornamento tab
    for nome_query, query_sql in queries.items():
        try:
            logging.info(f"Esecuzione query per: {nome_query}")
            df = run_query(spark, query_sql)

            if df.empty:
                logging.warning(f"Nessun dato trovato per {nome_query}. Salto.")
                continue

            if nome_query not in SHEET_MAP:
                logging.error(f"Nessun mapping sheet per '{nome_query}'. Skipping.")
                continue

            sheet_id_target = SHEET_MAP[nome_query]["sheet_id"]
            sheet_name = SHEET_MAP[nome_query]["sheet_name"]

            # Scrittura contenuto principale
            logging.info(f"Scrittura del dataset su sheet '{sheet_name}'...")
            export_to_sheets(df, creds, sheet_id_target, sheet_name)

            # Scrittura date di aggiornamento
            logging.info(f"Aggiornamento tab Data Aggiornamento per {nome_query}...")
            export_to_sheets(update_df, creds, sheet_id_target, "Aggiornamento Job")

        except Exception as e:
            logging.error(f"Errore durante elaborazione di {nome_query}: {e}", exc_info=True)
            continue

    
    spark.stop()
    logging.info("Job completato con successo.")


if __name__ == "__main__":
    main()
