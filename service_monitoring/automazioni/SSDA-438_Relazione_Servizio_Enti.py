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
SHEET_ID = "1yvTyUH7aEXqWpeoCtP5d5gqy5atvTfS60McZPRQThQQ"

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



def run_query(spark: SparkSession, senderpaid: str) -> dict:
    """Esegue la query su Spark e restituisce un DataFrame Pandas."""
    logging.info("Esecuzione query Relazione Servizio Enti...")

    query2_totale_notifiche = f"""
        SELECT 
            YEAR(sentat) AS anno,
            type_notif,
            COUNT(*) AS numero_notifiche
        FROM send.gold_notification_analytics
        WHERE senderpaid = '{senderpaid}'  
            --AND type_notif IS NOT NULL   -- da capire se vanno escluse quelle con type notif NULL oppure anche le MULTI
            --AND tms_cancelled IS NULL    -- da capire se vanno escluse quelle cancellate
            AND type_notif IS NOT NULL
            --AND YEAR(sentat) IN (2024, 2025)
        GROUP BY YEAR(sentat), 
                type_notif
        ORDER BY anno ASC, type_notif DESC"""
    
    query3_tempisticheperfezionamento = f"""
         WITH base AS (
            SELECT  
                iun,
                cast(sentat AS timestamp) AS sentat,
                type_notif,
                -- Calcolo tms_perfezionamento
                CASE 
                    WHEN tms_viewed IS NULL THEN cast(tms_effective_date AS timestamp)
                    WHEN tms_effective_date IS NULL THEN cast(tms_viewed AS timestamp)
                    WHEN tms_viewed < tms_effective_date THEN cast(tms_viewed AS timestamp)
                    ELSE cast(tms_effective_date AS timestamp)
                END AS tms_perfezionamento
            FROM send.gold_notification_analytics
            WHERE senderpaid =  '{senderpaid}'  
            AND (tms_viewed IS NOT NULL OR tms_effective_date IS NOT NULL)
        ),
        diff AS (
            SELECT
                iun,
                type_notif,
                sentat,
                tms_perfezionamento,
                -- Differenza in giorni (equivalente del unix_timestamp / 86400)
                ROUND(
                    (unix_timestamp(tms_perfezionamento) - unix_timestamp(sentat)) 
                    / (3600 * 24),
                2) AS diff_sentat_perfezionamento,
                YEAR(sentat) AS anno_deposito
            FROM base
        )
        SELECT
            anno_deposito,
            type_notif,
            ROUND(SUM(diff_sentat_perfezionamento), 2) AS somma_tempistiche,
            COUNT(tms_perfezionamento) AS notifiche_perfezionate,
            ROUND(
                SUM(diff_sentat_perfezionamento) / COUNT(tms_perfezionamento),
                2
            ) AS tempo_medio_perfezionamento
        FROM diff
        GROUP BY
            anno_deposito,
            type_notif
        ORDER BY
            anno_deposito,
            type_notif;
    """

    query4_perc_conversione_digitale = f"""
    WITH courtesy AS (
            SELECT DISTINCT
                iun,
                notificationsentat
            FROM
                send.silver_timeline
            WHERE
                category = 'SEND_COURTESY_MESSAGE'
                AND paid = '{senderpaid}'  
        ),
        perfezionate AS (
            SELECT DISTINCT
                iun,
                notificationsentat
            FROM
                send.silver_timeline
            WHERE
                category IN ('NOTIFICATION_VIEWED', 'REFINEMENT')
                AND paid = '{senderpaid}'  
        ),
        prepared AS (
            SELECT DISTINCT
                iun,
                notificationsentat
            FROM
                send.silver_timeline
            WHERE
                category = 'PREPARE_ANALOG_DOMICILE'
                AND paid = '{senderpaid}'  
        ),
        digital AS (
            SELECT DISTINCT
                iun,
                notificationsentat
            FROM
                send.silver_timeline
            WHERE
                category = 'SEND_DIGITAL_FEEDBACK'
                AND paid = '{senderpaid}'  
        ),
        joined AS (
            SELECT
                v.iun AS iun,
                MONTH (v.notificationsentat) AS mese_sentat,
                YEAR (v.notificationsentat) AS anno_sentat,
                CASE
                    WHEN d.iun IS NOT NULL THEN 1
                    ELSE 0
                END AS has_digital_feedback,
                CASE
                    WHEN c.iun IS NOT NULL THEN 1
                    ELSE 0
                END AS has_courtesy,
                CASE
                    WHEN v.iun IS NOT NULL THEN 1
                    ELSE 0
                END AS has_perfezionamento,
                CASE
                    WHEN p.iun IS NOT NULL THEN 1
                    ELSE 0
                END AS has_prepared
            FROM
                perfezionate v
                LEFT JOIN digital d ON d.iun = v.iun
                LEFT JOIN courtesy c ON v.iun = c.iun
                LEFT JOIN prepared p ON v.iun = p.iun
        ),
        clusterized AS (
            SELECT
                iun,
                mese_sentat,
                anno_sentat,
                CASE
                    WHEN has_courtesy = 1
                    AND has_digital_feedback = 0
                    AND has_perfezionamento = 1
                    AND has_prepared = 0 THEN 'Analogiche evitate grazie ai messaggi di cortesia'
                    ELSE 'Altro'
                END AS cluster
            FROM
                joined
        ),
        D_perfezionamento AS (
            SELECT
                MONTH (notificationsentat) AS mese_sentat,
                YEAR (notificationsentat) AS anno_sentat,
                COUNT(DISTINCT iun) AS totale_perfezionate
            FROM
                send.silver_timeline
            WHERE
                category IN (
                    'SEND_DIGITAL_FEEDBACK',
                    'SEND_ANALOG_FEEDBACK',
                    'NOTIFICATION_VIEWED',
                    'REFINEMENT'
                )
                AND paid = '{senderpaid}'  
            GROUP BY
                MONTH (notificationsentat),
                YEAR (notificationsentat)
        ),
        B_final AS (
            SELECT
                mese_sentat,
                anno_sentat,
                cluster,
                COUNT(DISTINCT iun) AS totale_cluster
            FROM
                clusterized
            WHERE
                cluster <> 'Altro'
            GROUP BY
                cluster,
                mese_sentat,
                anno_sentat
            ORDER BY
                mese_sentat,
                anno_sentat DESC
        ),
        C_digitali_perfezionate AS (
            SELECT
                MONTH (notificationsentat) AS mese_sentat,
                YEAR (notificationsentat) AS anno_sentat,
                COUNT(DISTINCT iun) AS digitali_perfezionate
            FROM
                send.silver_timeline
            WHERE
                category = 'SEND_DIGITAL_FEEDBACK'
                AND paid = '{senderpaid}'  
            GROUP BY
                MONTH (notificationsentat),
                YEAR (notificationsentat)
        ),
        F_notifiche_affidate AS (
            SELECT
                MONTH (sentat) AS mese_sentat,
                YEAR (sentat) AS anno_sentat,
                COUNT(DISTINCT iun) AS conteggio_affidate
            FROM
                send.gold_notification_analytics
            WHERE
                type_notif IS NOT NULL
                AND senderpaid = '{senderpaid}'  
            GROUP BY
                MONTH (sentat),
                YEAR (sentat)
        )
        SELECT
            CONCAT (
                CAST(f.mese_sentat AS STRING),
                '_',
                CAST(f.anno_sentat AS STRING)
            ) AS `anno_mese deposito`,
            COALESCE(
                (
                    CASE
                        WHEN cluster = 'Analogiche evitate grazie ai messaggi di cortesia' THEN totale_cluster
                    END
                ),
                0
            ) AS `Analogiche evitate grazie ai messaggi di cortesia`,
            COALESCE(digitali_perfezionate, 0) AS `Digitali perfezionate`,
            COALESCE(totale_perfezionate, 0) AS `Totale perfezionate (ricevuto feedback)`,
            COALESCE(conteggio_affidate, 0) - COALESCE(digitali_perfezionate, 0) AS `Notifiche analogiche`,
            COALESCE(conteggio_affidate, 0) AS `Notifiche affidate`,
            ROUND(
                (
                    COALESCE(
                        (
                            CASE
                                WHEN cluster = 'Analogiche evitate grazie ai messaggi di cortesia' THEN totale_cluster
                            END
                        ),
                        0
                    ) + COALESCE(digitali_perfezionate, 0)
                ) / COALESCE(totale_perfezionate, 0),
                4
            ) AS `% Conversioni (mancate analogiche+pec) sul perfezionato`,
            ROUND(
                COALESCE(
                    (
                        CASE
                            WHEN cluster = 'Analogiche evitate grazie ai messaggi di cortesia' THEN totale_cluster
                        END
                    ),
                    0
                ) / (
                    COALESCE(conteggio_affidate, 0) - COALESCE(digitali_perfezionate, 0)
                ),
                4
            ) AS `% Conversione analogico`,
            ROUND(
                (
                    COALESCE(
                        (
                            CASE
                                WHEN cluster = 'Analogiche evitate grazie ai messaggi di cortesia' THEN totale_cluster
                            END
                        ),
                        0
                    ) + COALESCE(digitali_perfezionate, 0)
                ) / COALESCE(conteggio_affidate, 0),
                4
            ) AS `% Conversioni (mancate analogiche+pec) su affidato`, -- chiedere conferma per il nome della colonna
            ROUND(
                COALESCE(
                    (
                        CASE
                            WHEN cluster = 'Analogiche evitate grazie ai messaggi di cortesia' THEN totale_cluster
                        END
                    ),
                    0
                ) / COALESCE(conteggio_affidate, 0),
                4
            ) AS `% Conversione (mancate analogiche) su affidato`
        FROM
            F_notifiche_affidate f
            LEFT JOIN D_perfezionamento d ON (
                f.mese_sentat = d.mese_sentat
                AND f.anno_sentat = d.anno_sentat
            )
            LEFT JOIN C_digitali_perfezionate c ON (
                f.mese_sentat = c.mese_sentat
                AND f.anno_sentat = c.anno_sentat
            )
            LEFT JOIN B_final b ON (
                b.mese_sentat = f.mese_sentat
                AND b.anno_sentat = f.anno_sentat
            )
            --WHERE f.anno_sentat <> 2023
        ORDER BY
            f.anno_sentat ASC,
            f.mese_sentat ASC"""

    query5_pagate_ai_cittadini = f""" 
        WITH postalizzazione_dedup AS (
            SELECT DISTINCT iun, prezzo_ente
            FROM send.gold_postalizzazione_analytics
            WHERE pcretry_rank = 1
            -- pcretry_rank = 1 garantisce 1 sola riga per iun  
        ),base AS (
            SELECT 
                    b.iun,
                    b.sentat,
                    b.type_notif,
                    b.tms_date_payment,
                    b.tms_viewed,
                    b.tms_effective_date,
                    CASE 
                        WHEN b.tms_viewed IS NULL THEN b.tms_effective_date
                        WHEN b.tms_effective_date IS NULL THEN b.tms_viewed
                        WHEN b.tms_viewed < b.tms_effective_date THEN b.tms_viewed
                        ELSE b.tms_effective_date
                    END AS tms_perfezionamento,
                    prezzo_ente
                FROM send.gold_notification_analytics b
                LEFT JOIN postalizzazione_dedup c
                ON b.iun = c.iun
                WHERE b.senderpaid = '{senderpaid}' 
                AND b.type_notif IS NOT NULL
                --AND c.pcretry_rank = 1
        )      
        SELECT 
                YEAR(sentat) AS anno,
                type_notif AS `tipo notifica`,
                --COUNT(*) AS numero_notifiche,
                COUNT(CASE WHEN tms_date_payment IS NOT NULL THEN 1 END) AS `Totale notifiche pagate`, -- COLONNA C
                COUNT(CASE WHEN tms_perfezionamento IS NOT NULL THEN 1 END) AS `Totale notifiche perfezionate`, -- COLONNA D 
                COUNT(CASE WHEN tms_perfezionamento IS NOT NULL AND tms_date_payment IS NOT NULL THEN 1 END) AS `Totale notifiche pagate e perfezionate`, -- COLONNA E
                COUNT(CASE WHEN tms_perfezionamento IS NULL AND tms_date_payment IS NOT NULL THEN 1 END) AS `Totale notifiche pagate no perfezionate`, -- COLONNA F
                -- percentuale su pagate e perfezionate COLONNA C/D
                ROUND(
                    COUNT(CASE WHEN tms_date_payment IS NOT NULL THEN 1 END)
                    / NULLIF(
                        COUNT(CASE WHEN tms_perfezionamento IS NOT NULL THEN 1 END),
                        0
                    )
                , 4) AS `% pagate su perfezionate`, -- COLONNA G
                --Corrispettivo digitale pagate e perfezionate
                CAST(
                    COUNT(CASE WHEN tms_perfezionamento IS NOT NULL AND tms_date_payment IS NOT NULL THEN 1 END)
                    AS DECIMAL(18,2)
                ) AS `Corrispettivo digitale pagate e perfezionate`,  -- COLONNA H - impostare nel google Sheet il formato euro nella colonna
                -- Corrispettivo pagate e perfezionate
                SUM(CASE WHEN tms_date_payment IS NOT NULL 
                        AND tms_perfezionamento IS NOT NULL 
                    THEN prezzo_ente END
                ) AS `Corrispettivo pagate e perfezionate eurocent (ANALOG)`, -- COLONNA I - impostare nel google Sheet il formato euro nella colonna
                -- Corrispettivo digitale pagate e non perfezionate
                CAST(
                    COUNT(CASE WHEN tms_perfezionamento IS NULL  AND tms_date_payment IS NOT NULL THEN 1 END)
                    AS DECIMAL(18,2)
                ) AS `Corrispettivo digitale pagate e non perfezionate`, --COLONNA J - impostare nel google Sheet il formato euro nella colonna 
                -- Corrispettivo pagate e non perfezionate
                SUM(CASE WHEN tms_date_payment IS NOT NULL 
                        AND tms_perfezionamento IS NULL 
                    THEN prezzo_ente END
                ) AS `Corrispettivo pagate non perfezionate eurocent (ANALOG)` -- COLONNA K - impostare nel google Sheet il formato euro nella colonna
            FROM base
            WHERE YEAR(sentat) IN (2024, 2025)
            GROUP BY YEAR(sentat), 
                    type_notif
            ORDER BY anno ASC, type_notif DESC"""

    query6_tempi_medi_pagamento = f""" 
        WITH base AS (
            SELECT  
                b.iun,
                b.sentat,
                b.type_notif,
                b.tms_date_payment,
                DATEDIFF(b.tms_date_payment, b.sentat) AS giorni_pagamento
            FROM send.gold_notification_analytics b
            --LEFT JOIN send.gold_postalizzazione_analytics c 
            --  ON b.iun = c.iun
            WHERE b.senderpaid = '{senderpaid}'
            AND b.type_notif IS NOT NULL
            --AND c.pcretry_rank = 1
            AND b.tms_date_payment IS NOT NULL
        )
        SELECT
            YEAR(sentat) AS anno,
            type_notif,
            ROUND(AVG(giorni_pagamento), 2) AS media_giorni_pagamento,
            COUNT(*) AS notifiche_pagate,
            SUM(giorni_pagamento) AS somma_giorni_pagamento,
            -- Conteggi per scaglione
            SUM(CASE WHEN giorni_pagamento BETWEEN 0 AND 10 THEN 1 ELSE 0 END) AS count_0_10gg,
            SUM(CASE WHEN giorni_pagamento BETWEEN 11 AND 20 THEN 1 ELSE 0 END) AS count_11_20gg,
            SUM(CASE WHEN giorni_pagamento BETWEEN 21 AND 30 THEN 1 ELSE 0 END) AS count_21_30gg,
            SUM(CASE WHEN giorni_pagamento BETWEEN 31 AND 40 THEN 1 ELSE 0 END) AS count_31_40gg,
            SUM(CASE WHEN giorni_pagamento BETWEEN 41 AND 50 THEN 1 ELSE 0 END) AS count_41_50gg,
            SUM(CASE WHEN giorni_pagamento BETWEEN 51 AND 60 THEN 1 ELSE 0 END) AS count_51_60gg,
            SUM(CASE WHEN giorni_pagamento BETWEEN 61 AND 70 THEN 1 ELSE 0 END) AS count_61_70gg,
            SUM(CASE WHEN giorni_pagamento BETWEEN 71 AND 80 THEN 1 ELSE 0 END) AS count_71_80gg,
            SUM(CASE WHEN giorni_pagamento > 80 THEN 1 ELSE 0 END) AS count_oltre_80gg,
            -- Percentuali per scaglione
            ROUND(SUM(CASE WHEN giorni_pagamento BETWEEN 0 AND 10 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS perc_0_10gg,
            ROUND(SUM(CASE WHEN giorni_pagamento BETWEEN 11 AND 20 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS perc_11_20gg,
            ROUND(SUM(CASE WHEN giorni_pagamento BETWEEN 21 AND 30 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS perc_21_30gg,
            ROUND(SUM(CASE WHEN giorni_pagamento BETWEEN 31 AND 40 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS perc_31_40gg,
            ROUND(SUM(CASE WHEN giorni_pagamento BETWEEN 41 AND 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS perc_41_50gg,
            ROUND(SUM(CASE WHEN giorni_pagamento BETWEEN 51 AND 60 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS perc_51_60gg,
            ROUND(SUM(CASE WHEN giorni_pagamento BETWEEN 61 AND 70 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS perc_61_70gg,
            ROUND(SUM(CASE WHEN giorni_pagamento BETWEEN 71 AND 80 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS perc_71_80gg,
            ROUND(SUM(CASE WHEN giorni_pagamento > 80 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS perc_oltre_80gg
        FROM base
        GROUP BY YEAR(sentat), type_notif 
        ORDER BY anno """

     # --- Esecuzione di tutte le query e conversione in Pandas
    results = {}
    results["2. Totale notifiche"] = spark.sql(query2_totale_notifiche).toPandas()
    results["3. Tempistiche di perfezionamento"] = spark.sql(query3_tempisticheperfezionamento).toPandas()
    results["4. % conversione digitale"] = spark.sql(query4_perc_conversione_digitale).toPandas()
    results["5. Pagate dai cittadini"] = spark.sql(query5_pagate_ai_cittadini).toPandas()
    results["6. Tempi medi pagamento"] = spark.sql(query6_tempi_medi_pagamento).toPandas()

    logging.info("Tutte le query sono state trasformate in Pandas DataFrame")

    return results


# FUNZIONI AUSILIARIE
def get_last_update_date(spark: SparkSession) -> str:
    """Estrae la data più recente (MAX(requesttimestamp)) dal dataset."""
    logging.info("Estrazione data ultimo aggiornamento...")
    
    max_date_df = spark.sql("SELECT MAX(requesttimestamp) AS max_ts FROM send.gold_postalizzazione_analytics") 
    max_date = max_date_df.collect()[0]["max_ts"] 

    if isinstance(max_date, datetime): 
        return max_date.strftime("%Y-%m-%d %H:%M:%S") 
    return str(max_date)


def get_sender_denomination(spark: SparkSession, senderpaid: str) -> str:
    """
    Estrae la senderDenomination (description) da selfcare.gold_contracts dato un senderpaid.
    Gestisce errori Spark/Iceberg senza far crashare lo script.
    """
    logging.info(f"[SenderDenomination] Ricerca per senderpaid={senderpaid}...")

    query1 = f"""
        SELECT description
        FROM selfcare.gold_contracts
        WHERE internalistitutionid = '{senderpaid}'
        LIMIT 1
    """
    query = f""" SELECT DISTINCT 
                    internalistitutionid,
                    CONCAT_WS(' - ', institution.rootParent.description, institution.description) AS description
                FROM selfcare.silver_contracts e
            WHERE internalistitutionid = '{senderpaid}'
            LIMIT 1"""

    try:
        df = spark.sql(query)
        rows = df.collect()

    except Exception as e:
        logging.error(
            f"[SenderDenomination] Errore durante la query su selfcare.gold_contracts "
            f"per senderpaid={senderpaid}: {e}"
        )
        # evitando crash del job
        return "N/D (errore lettura tabella)"

    # se la query è andata ma non ha trovato nulla
    if not rows:
        logging.warning(
            f"[SenderDenomination] Nessuna description trovata per senderpaid={senderpaid}."
        )
        return "N/D (non trovato)"

    #description = rows[0].get("description")
    description = rows[0]["description"]

    if description is None:
        logging.warning(
            f"[SenderDenomination] Description NULL per senderpaid={senderpaid}."
        )
        return "N/D (vuoto)"

    return str(description).strip()


def export_to_sheets(df: pd.DataFrame, creds: dict, sheet_id: str, sheet_name: str):
    """Esporta i dati su Google Sheet."""
    logging.info(f"Scrittura su Google Sheet: {sheet_name}")
    df = df.astype(str)
    sheet = Sheet(sheet_id=sheet_id, service_credentials=creds, id_mode='key')
    sheet.upload(sheet_name, df)
    logging.info("Scrittura completata.")


# ---------------- MAIN SCRIPT ----------------

def main():
    logging.info("Inizializzazione SparkSession...")
    spark = SparkSession.builder.appName("SSDA-438 Relazione Servizio Enti").getOrCreate()

    # Caricamento credenziali Google
    creds = load_google_credentials(GOOGLE_SECRET_PATH)
    sheet = Sheet(sheet_id=SHEET_ID, service_credentials=creds, id_mode='key')

    
    # Lettura senderpaid da sheet: info_senderpaid
    df_info = sheet.download("info_senderpaid")
    # Debug
    print("DataFrame info_senderpaid:")
    print(df_info)
    print("Shape:", df_info.shape)

    # Controllo che esista almeno una riga
    if df_info.shape[0] < 1:
        raise ValueError("Lo sheet 'info_senderpaid' non contiene un senderpaid valido.")

    # Lettura del valore in A2 → prima riga dati = iloc[0]
    senderpaid = str(df_info.iloc[0, 0]).strip()

    if not senderpaid:
        raise ValueError("La cella A2 (senderpaid) è vuota o non valida.")
    

    logging.info(f"Senderpaid letto: {senderpaid}")

    # Esecuzione query
    dfs = run_query(spark, senderpaid=senderpaid)

    # Scrittura ogni query su sheet dedicato
    for query_name, df in dfs.items():
        logging.info(f"Scrittura risultati della query '{query_name}'...")
        export_to_sheets(df, creds, SHEET_ID, query_name)

    # Calcolo date di aggiornamento
    max_date_str = get_last_update_date(spark)
    job_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logging.info(f"Ultimo aggiornamento dati gold_postalizzazione: {max_date_str}")
    logging.info(f"Data esecuzione job: {job_time}")

    # Aggiornamento date e senderdenomintion in info_senderpaid
    logging.info("Aggiornamento campi in info_senderpaid...")

    senderdenomination = get_sender_denomination(spark, senderpaid)

    print("SenderDenomination:", senderdenomination)

    # colonne attese: A senderpaid | B senderdenomination | C ultimo update | D job time
    # aggiorniamo solo C e D sulla riga del senderpaid (riga 1 indice base zero)

    df_info.loc[1, "senderdenomination"] = senderdenomination
    
    df_info.loc[1, "Ultimo aggiornamento dati (MAX requesttimestamp)"] = max_date_str
    df_info.loc[1, "Data esecuzione script (UTC)"] = job_time

    # Scrittura sul foglio intero senza sovrascrivere il senderpaid
    sheet.upload("info_senderpaid", df_info)


    # Chiusura sessione
    spark.stop()
    logging.info("Processo completato con successo.")


if __name__ == "__main__":
    main()
