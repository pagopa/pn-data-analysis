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

SHEET_ID_DETTAGLIO = "1k6MLUpu1ljFupXf7rOEUrforWgabQAnZvpOBhGhzo4c" 
SHEET_ID_AGGREGATO = "1OivdWPlLr-RYqhU5axCZUayzBCtjNRGPRLRs5qUw6wA"


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


def staging_basequery(spark: SparkSession): 

    # Query base -- versione CDE con Lateral View
    query_sql_base = """
        WITH 
            cte_postalizzazione_certificazione_recapito AS (
                SELECT DISTINCT
                    t.requestid,
                    e.paperprogrstatus.statuscode AS certificazione_recapito_stato,
                    CASE
                        WHEN LENGTH(e.paperprogrstatus.statusdatetime) = 17 
                        THEN CONCAT(SUBSTRING(e.paperprogrstatus.statusdatetime, 1, 16), ':00Z')
                        ELSE e.paperprogrstatus.statusdatetime
                    END AS statusdatetime,
                    e.paperprogrstatus.deliveryfailurecause AS deliveryfailurecause
                FROM send.silver_postalizzazione t
                LATERAL VIEW EXPLODE(t.eventslist) e AS e
                WHERE e.paperprogrstatus.statuscode IN (
                    'RECRN001A','RECRN002A','RECRN002D','RECRN003A','RECRN004A','RECRN005A',
                    'RECRS001C','RECRS002A','RECRS002D','RECRS003C','RECRS004A','RECRS005A',
                    'RECAG001A','RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A',
                    'RECAG007A','RECAG008A','RECRI003A','RECRI004A','RECRSI003C','RECRSI004A'
                )
            ),
            vista_conteggio_duplicati AS (
                SELECT
                    requestid,
                    'certificazione_recapito_stato' AS campo_switch
                FROM cte_postalizzazione_certificazione_recapito
                GROUP BY requestid
                HAVING COUNT(*) > 1
            ),
            cte_postalizzazione_inesito AS (
                SELECT DISTINCT
                    t.requestid,
                    e.paperprogrstatus.statuscode AS certificazione_recapito_stato,
                    CASE
                        WHEN LENGTH(e.paperprogrstatus.statusdatetime) = 17 
                        THEN CONCAT(SUBSTRING(e.paperprogrstatus.statusdatetime, 1, 16), ':00Z')
                        ELSE e.paperprogrstatus.statusdatetime
                    END AS statusdatetime
                FROM send.silver_postalizzazione t
                LATERAL VIEW EXPLODE(t.eventslist) e AS e
                WHERE e.paperprogrstatus.statuscode IN ('RECRS010', 'RECAG010', 'RECRN010')
            ),
            cte_postalizzazione_altri_stati AS (
                SELECT DISTINCT
                    t.requestid,
                    e.paperprogrstatus.statuscode AS certificazione_recapito_stato,
                    CASE
                        WHEN LENGTH(e.paperprogrstatus.statusdatetime) = 17 
                        THEN CONCAT(SUBSTRING(e.paperprogrstatus.statusdatetime, 1, 16), ':00Z')
                        ELSE e.paperprogrstatus.statusdatetime
                    END AS statusdatetime,
                    e.paperprogrstatus.deliveryfailurecause AS deliveryfailurecause 
                FROM send.silver_postalizzazione t
                LATERAL VIEW EXPLODE(t.eventslist) e AS e
                WHERE e.paperprogrstatus.statuscode IN (
                    'RECRN001A','RECRN002A','RECRN002D',
                    'RECRS001C','RECRS002A','RECRS002D',
                    'RECAG001A','RECAG002A','RECAG003A','RECAG003D'
                )
            ),
            conteggio_duplicati_inesito AS (
                SELECT
                    i.requestid,
                    'tentativo_recapito_stato' AS campo_switch
                FROM cte_postalizzazione_inesito i
                LEFT JOIN cte_postalizzazione_altri_stati a
                    ON i.requestid = a.requestid
                WHERE i.requestid IS NOT NULL
                AND a.requestid IS NOT NULL
                GROUP BY i.requestid
                HAVING COUNT(*) > 1
            ),
            unione_requestid AS (
                SELECT requestid, campo_switch 
                FROM vista_conteggio_duplicati
                UNION
                SELECT requestid, campo_switch 
                FROM conteggio_duplicati_inesito
                WHERE requestid NOT IN (
                    SELECT requestid FROM vista_conteggio_duplicati
                )
            ),
            dettaglio AS (
                SELECT 
                    SUBSTRING(u.requestid, 13) AS requestid,
                    u.campo_switch,
                    g.recapitista,
                    g.lotto,
                    MONTH(COALESCE(g.accettazione_recapitista_con018_data, g.affido_recapitista_con016_data)) AS mese_accettazione,
                    YEAR(COALESCE(g.accettazione_recapitista_con018_data, g.affido_recapitista_con016_data)) AS anno_accettazione
                FROM unione_requestid u
                LEFT JOIN send.gold_postalizzazione_analytics g
                    ON SUBSTRING(u.requestid, 13) = g.requestid
            ),
            cte_base_rank_certificazione AS (
                SELECT DISTINCT
                    SUBSTRING(t.requestid, 13) AS requestid,
                    e.paperprogrstatus.statuscode AS statuscode,
                    CASE
                        WHEN LENGTH(e.paperprogrstatus.statusdatetime) = 17 
                        THEN CONCAT(SUBSTRING(e.paperprogrstatus.statusdatetime, 1, 16), ':00Z')
                        ELSE e.paperprogrstatus.statusdatetime
                    END AS statusdatetime,
                    e.paperprogrstatus.deliveryfailurecause AS deliveryfailurecause,
                    CASE
                        WHEN LENGTH(e.paperprogrstatus.clientrequesttimestamp) = 17 
                        THEN CONCAT(SUBSTRING(e.paperprogrstatus.clientrequesttimestamp, 1, 16), ':00Z')
                        ELSE e.paperprogrstatus.clientrequesttimestamp
                    END AS clientrequesttimestamp
                FROM send.silver_postalizzazione t
                LATERAL VIEW EXPLODE(t.eventslist) e AS e
                WHERE e.paperprogrstatus.statuscode IN (
                    'RECRN001A','RECRN002A','RECRN002D','RECRN003A','RECRN004A','RECRN005A',
                    'RECRS001C','RECRS002A','RECRS002D','RECRS003C','RECRS004A','RECRS005A',
                    'RECAG001A','RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A',
                    'RECAG007A','RECAG008A','RECRI003A','RECRI004A','RECRSI003C','RECRSI003C'
                )
                AND SUBSTRING(t.requestid, 13) IN (
                    SELECT requestid
                    FROM dettaglio
                    WHERE campo_switch = 'certificazione_recapito_stato'
                )
            ),
            ranked_min_certificazione AS (
                SELECT
                    requestid,
                    statusdatetime AS data_business_minimo_evento,
                    clientrequesttimestamp AS data_rendicontazione_minimo_evento,
                    ROW_NUMBER() OVER (PARTITION BY requestid ORDER BY clientrequesttimestamp ASC) AS rn_min
                FROM cte_base_rank_certificazione
            ),
            ranked_max_certificazione AS (
                SELECT
                    requestid,
                    statusdatetime AS data_business_massimo_evento,
                    clientrequesttimestamp AS data_rendicontazione_massimo_evento,
                    ROW_NUMBER() OVER (PARTITION BY requestid ORDER BY clientrequesttimestamp DESC) AS rn_max
                FROM cte_base_rank_certificazione
            ),
            cte_base_rank_tentativo AS (
                SELECT DISTINCT
                    SUBSTRING(t.requestid, 13) AS requestid,
                    e.paperprogrstatus.statuscode AS statuscode,
                    CASE
                        WHEN LENGTH(e.paperprogrstatus.statusdatetime) = 17 
                        THEN CONCAT(SUBSTRING(e.paperprogrstatus.statusdatetime, 1, 16), ':00Z')
                        ELSE e.paperprogrstatus.statusdatetime
                    END AS statusdatetime,
                    e.paperprogrstatus.deliveryfailurecause AS deliveryfailurecause,
                    CASE
                        WHEN LENGTH(e.paperprogrstatus.clientrequesttimestamp) = 17 
                        THEN CONCAT(SUBSTRING(e.paperprogrstatus.clientrequesttimestamp, 1, 16), ':00Z')
                        ELSE e.paperprogrstatus.clientrequesttimestamp
                    END AS clientrequesttimestamp
                FROM send.silver_postalizzazione t
                LATERAL VIEW EXPLODE(t.eventslist) e AS e
                WHERE e.paperprogrstatus.statuscode IN (
                    'RECRN001A','RECRN002A','RECRN002D',
                    'RECRS001C','RECRS002A','RECRS002D',
                    'RECAG001A','RECAG002A','RECAG003A','RECAG003D',
                    'RECRS010','RECAG010','RECRN010'
                )
                AND SUBSTRING(t.requestid, 13) IN (
                    SELECT requestid
                    FROM dettaglio
                    WHERE campo_switch = 'tentativo_recapito_stato'
                )
            ),
            ranked_min_tentativo AS (
                SELECT
                    requestid,
                    statusdatetime AS data_business_minimo_evento,
                    clientrequesttimestamp AS data_rendicontazione_minimo_evento,
                    ROW_NUMBER() OVER (PARTITION BY requestid ORDER BY clientrequesttimestamp ASC) AS rn_min
                FROM cte_base_rank_tentativo
            ),
            ranked_max_tentativo AS (
                SELECT
                    requestid,
                    statusdatetime AS data_business_massimo_evento,
                    clientrequesttimestamp AS data_rendicontazione_massimo_evento,
                    ROW_NUMBER() OVER (PARTITION BY requestid ORDER BY clientrequesttimestamp DESC) AS rn_max
                FROM cte_base_rank_tentativo
            )
            SELECT
                d.*,
                r.data_business_minimo_evento,
                r.data_rendicontazione_minimo_evento,
                rm.data_rendicontazione_massimo_evento
            FROM dettaglio d
            LEFT JOIN ranked_min_certificazione r
            ON r.requestid = d.requestid
            AND r.rn_min = 1
            LEFT JOIN ranked_max_certificazione rm
            ON rm.requestid = d.requestid
            AND rm.rn_max = 1
            WHERE d.campo_switch = 'certificazione_recapito_stato'
            UNION ALL
            SELECT
                d.*,
                r.data_business_minimo_evento,
                r.data_rendicontazione_minimo_evento,
                rm.data_rendicontazione_massimo_evento
            FROM dettaglio d
            LEFT JOIN ranked_min_tentativo r
            ON r.requestid = d.requestid
            AND r.rn_min = 1
            LEFT JOIN ranked_max_tentativo rm
            ON rm.requestid = d.requestid
            AND rm.rn_max = 1
            WHERE d.campo_switch = 'tentativo_recapito_stato'
    """

    # Esecuzione della query spark e salvataggio in un dataframe
    df = spark.sql(query_sql_base)

    df.createOrReplaceTempView("DF_BASE")
                            
    spark.sql("""SELECT * FROM DF_BASE""").writeTo("send_dev.temp_correzione_preesito")\
                .using("iceberg")\
                .tableProperty("format-version","2")\
                .tableProperty("engine.hive.enabled","true")\
                .createOrReplace()		     


def build_queries() -> dict:

    # Query per lo sheet di dettaglio
    query_dettaglio = """
        SELECT *,
            DATEDIFF(data_rendicontazione_minimo_evento, data_business_minimo_evento) AS `diff_rend_data_primo_pre-esito`,
            DATEDIFF(data_rendicontazione_massimo_evento, data_rendicontazione_minimo_evento) AS `diff_rend_rend_primo_ultimo_pre-esito` -- trovarci un nome più parlante
        FROM send_dev.temp_correzione_preesito
    """

    # Base per recapitisti
    query_aggregato = """WITH base AS (
        SELECT *,
            DATEDIFF(data_rendicontazione_minimo_evento, data_business_minimo_evento) AS `diff_rend_data_primo_pre-esito`,
            DATEDIFF(data_rendicontazione_massimo_evento, data_rendicontazione_minimo_evento) AS `diff_rend_rend_primo_ultimo_pre-esito` -- trovarci un nome più parlante
        FROM send_dev.temp_correzione_preesito
        ), totale_affidi AS (
            SELECT  
                g.recapitista,
                g.lotto,
                MONTH(COALESCE(g.accettazione_recapitista_con018_data, g.affido_recapitista_con016_data)) AS mese_accettazione,
                YEAR(COALESCE(g.accettazione_recapitista_con018_data, g.affido_recapitista_con016_data)) AS anno_accettazione,
                COUNT(*) AS totale_accettate
            FROM send.gold_postalizzazione_analytics g
            GROUP BY 
                g.recapitista,
                g.lotto,
                MONTH(COALESCE(g.accettazione_recapitista_con018_data, g.affido_recapitista_con016_data)),
                YEAR(COALESCE(g.accettazione_recapitista_con018_data, g.affido_recapitista_con016_data))
        )SELECT 
            t.recapitista, 
            t.lotto, 
            t.mese_accettazione, 
            t.anno_accettazione,
            COALESCE(COUNT(v.requestid), 0) AS conteggio_requestid_con_esito_ritrasmesso,
            t.totale_accettate,
            ROUND(COALESCE(COUNT(v.requestid), 0) / totale_accettate , 2) AS `% esiti ritrasmessi su totale accettate`,
            --- Conteggi per scaglione - tempistica 1
            SUM(CASE WHEN `diff_rend_data_primo_pre-esito` = 0 THEN 1 ELSE 0 END) AS `[diff_rend_data_primo_pre-esito] stesso_giorno`,
            SUM(CASE WHEN `diff_rend_data_primo_pre-esito` BETWEEN 1 AND 10 THEN 1 ELSE 0 END) AS `[diff_rend_data_primo_pre-esito] count_1_10gg`,
            SUM(CASE WHEN `diff_rend_data_primo_pre-esito` BETWEEN 11 AND 20 THEN 1 ELSE 0 END) AS `[diff_rend_data_primo_pre-esito] count_11_20gg`,
            SUM(CASE WHEN `diff_rend_data_primo_pre-esito` BETWEEN 21 AND 30 THEN 1 ELSE 0 END) AS `[diff_rend_data_primo_pre-esito] count_21_30gg`,
            SUM(CASE WHEN `diff_rend_data_primo_pre-esito` BETWEEN 31 AND 40 THEN 1 ELSE 0 END) AS `[diff_rend_data_primo_pre-esito] count_31_40gg`,
            SUM(CASE WHEN `diff_rend_data_primo_pre-esito` BETWEEN 41 AND 50 THEN 1 ELSE 0 END) AS `[diff_rend_data_primo_pre-esito] count_41_50gg`,
            SUM(CASE WHEN `diff_rend_data_primo_pre-esito` BETWEEN 51 AND 60 THEN 1 ELSE 0 END) AS `[diff_rend_data_primo_pre-esito] count_51_60gg`,
            SUM(CASE WHEN `diff_rend_data_primo_pre-esito` > 60 THEN 1 ELSE 0 END) AS `[diff_rend_data_primo_pre-esito] count_oltre_60gg`,
            SUM(CASE WHEN `diff_rend_data_primo_pre-esito` < 0 THEN 1 ELSE 0 END) AS `[diff_rend_data_primo_pre-esito] tempistiche_in_errore`,
            --- Conteggi per scaglione - tempistica 2
            SUM(CASE WHEN `diff_rend_rend_primo_ultimo_pre-esito` = 0 THEN 1 ELSE 0 END) AS `[diff_rend_rend_primo_ultimo_pre-esito] count_stesso_giorno`,
            SUM(CASE WHEN `diff_rend_rend_primo_ultimo_pre-esito` BETWEEN 1 AND 10 THEN 1 ELSE 0 END) AS `[diff_rend_rend_primo_ultimo_pre-esito] count_1_10gg`,
            SUM(CASE WHEN `diff_rend_rend_primo_ultimo_pre-esito` BETWEEN 11 AND 20 THEN 1 ELSE 0 END) AS `[diff_rend_rend_primo_ultimo_pre-esito] count_11_20gg`,
            SUM(CASE WHEN `diff_rend_rend_primo_ultimo_pre-esito` BETWEEN 21 AND 30 THEN 1 ELSE 0 END) AS `[diff_rend_rend_primo_ultimo_pre-esito] count_21_30gg`,
            SUM(CASE WHEN `diff_rend_rend_primo_ultimo_pre-esito` BETWEEN 31 AND 40 THEN 1 ELSE 0 END) AS `[diff_rend_rend_primo_ultimo_pre-esito] count_31_40gg`,
            SUM(CASE WHEN `diff_rend_rend_primo_ultimo_pre-esito` BETWEEN 41 AND 50 THEN 1 ELSE 0 END) AS `[diff_rend_rend_primo_ultimo_pre-esito] count_41_50gg`,
            SUM(CASE WHEN `diff_rend_rend_primo_ultimo_pre-esito` BETWEEN 51 AND 60 THEN 1 ELSE 0 END) AS `[diff_rend_rend_primo_ultimo_pre-esito] count_51_60gg`,
            SUM(CASE WHEN `diff_rend_rend_primo_ultimo_pre-esito` > 60 THEN 1 ELSE 0 END) AS `[diff_rend_rend_primo_ultimo_pre-esito] count_oltre_60gg`,
            SUM(CASE WHEN `diff_rend_rend_primo_ultimo_pre-esito` < 0 THEN 1 ELSE 0 END) AS `[diff_rend_rend_primo_ultimo_pre-esito] tempistiche_in_errore`
        FROM totale_affidi t
        LEFT JOIN base v
            ON v.recapitista = t.recapitista
        AND v.lotto = t.lotto
        AND v.mese_accettazione = t.mese_accettazione
        AND v.anno_accettazione = t.anno_accettazione
        GROUP BY 
            t.recapitista, 
            t.lotto, 
            t.mese_accettazione, 
            t.anno_accettazione, 
            t.totale_accettate
        ORDER BY 
            t.anno_accettazione, 
            t.mese_accettazione, 
            t.recapitista, 
            t.lotto;"""

    return {
        "dettaglio" : query_dettaglio,
        "aggregato": query_aggregato
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

# Questa funzione probabilmente può essere utilizzata solamente per il livello di dettaglio? vedere le righe della base aggregato
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
        ul_row = 1 if i == 0 else (start + 2)
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

    #Esecuzione dello staging per la tabella temp
    logging.info("Staging basequery...")
    staging_basequery(spark)

    # Costruisci tutte le query
    queries = build_queries()

    # Mappatura nome query --> sheet di destinazione
    # DA AGGIORNARE IN BASE ALLE MODIFICHE EFFETTUATE
    SHEET_MAP = {
        "dettaglio": { "sheet_id": SHEET_ID_DETTAGLIO, "sheet_name": "Estrazione Dettaglio" },
        "aggregato": { "sheet_id": SHEET_ID_AGGREGATO, "sheet_name": "Estrazione Aggregato" }
    }

    # 1. Calcolo una volta le date del job
    job_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    max_date_str = get_last_update_date(spark)

    update_df = pd.DataFrame({
        "Ultimo aggiornamento dati (MAX requesttimestamp)": [max_date_str],
        "Data esecuzione job": [job_time]
    })

   
    # 2. Per ogni query -> export + aggiornamento tab
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
