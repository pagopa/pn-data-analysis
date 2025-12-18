from pyspark.sql import SparkSession
from pdnd_google_utils import Sheet
import time
import pandas as pd
import json
import os
import logging
from datetime import datetime

# ---------------- CONFIGURAZIONE BASE ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"
SHEET_ID_INPUT = "18_8NM3x4Di-PIiSMgTn2aJoDll7ks5wiE6BO-2ALZkk"
SHEET_ID_OUTPUT = "1bbRz9syMe7u5KvdTg_Sa2Hea3gcIvdS1E8cd_nQmEa0"

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



def run_query(spark: SparkSession) -> dict:
    """Esegue due query Spark e restituisce un dizionario di DataFrame Pandas."""
    logging.info("Esecuzione query Key Client...")

    query_sql_pivot_completa = """
        WITH dati_gold_corretti AS (
            SELECT 
                *,
                CASE
                    WHEN codice_oggetto LIKE 'R14%' THEN 'Fulmine'
                    WHEN codice_oggetto LIKE '777%' OR codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
                    WHEN codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
                    WHEN codice_oggetto LIKE '69%' OR codice_oggetto LIKE '381%' OR codice_oggetto LIKE 'RB1%' THEN 'Poste'
                    ELSE recapitista_unificato
                END AS recapitista_unif
            FROM send.gold_postalizzazione_analytics
        ),
        aggregati AS (
            SELECT 
                g.senderpaid, 
                COUNT(CASE WHEN recapitista_unif = 'Fulmine' THEN 1 END) AS conteggio_fulmine,
                COUNT(CASE WHEN recapitista_unif = 'Poste' THEN 1 END) AS conteggio_poste,
                COUNT(CASE WHEN recapitista_unif = 'POST & SERVICE' THEN 1 END) AS conteggio_po_se,
                COUNT(CASE WHEN recapitista_unif = 'RTI Sailpost-Snem' THEN 1 END) AS conteggio_sailpost,
                COUNT(*) AS totale
            FROM dati_gold_corretti g
            LEFT JOIN send.silver_postalizzazione_denormalized s 
                ON g.requestid = s.requestid_computed
            WHERE g.requestid IN (
                SELECT requestid FROM input_critiche
            ) 
            GROUP BY g.senderpaid
        ),
        totale AS (
            SELECT 
                'TOTALE' AS senderpaid,
                SUM(conteggio_fulmine) AS conteggio_fulmine,
                SUM(conteggio_poste) AS conteggio_poste,
                SUM(conteggio_po_se) AS conteggio_po_se,
                SUM(conteggio_sailpost) AS conteggio_sailpost,
                SUM(totale) AS totale
            FROM aggregati
        )
        SELECT *
        FROM (
            SELECT * FROM aggregati
            UNION ALL
            SELECT * FROM totale
        ) t
        ORDER BY 
            CASE WHEN t.senderpaid = 'TOTALE' THEN 1 ELSE 0 END,
            t.senderpaid ASC;
        """
    
    query_sql_pivot_senza_chiuse = """
        WITH dati_gold_corretti AS (
            SELECT 
                *,
                CASE
                    WHEN codice_oggetto LIKE 'R14%' THEN 'Fulmine'
                    WHEN codice_oggetto LIKE '777%' OR codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
                    WHEN codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
                    WHEN codice_oggetto LIKE '69%' OR codice_oggetto LIKE '381%' OR codice_oggetto LIKE 'RB1%' THEN 'Poste'
                    ELSE recapitista_unificato
                END AS recapitista_unif
            FROM send.gold_postalizzazione_analytics
        ),
        aggregati AS (
            SELECT 
                g.senderpaid, 
                COUNT(CASE WHEN recapitista_unif = 'Fulmine' THEN 1 END) AS conteggio_fulmine,
                COUNT(CASE WHEN recapitista_unif = 'Poste' THEN 1 END) AS conteggio_poste,
                COUNT(CASE WHEN recapitista_unif = 'POST & SERVICE' THEN 1 END) AS conteggio_po_se,
                COUNT(CASE WHEN recapitista_unif = 'RTI Sailpost-Snem' THEN 1 END) AS conteggio_sailpost,
                COUNT(*) AS totale
            FROM dati_gold_corretti g
            LEFT JOIN send.silver_postalizzazione_denormalized s 
                ON g.requestid = s.requestid_computed
            WHERE flag_wi7_report_postalizzazioni_incomplete = 1 AND g.requestid IN (
                SELECT requestid FROM input_critiche
            ) 
            GROUP BY g.senderpaid
        ),
        totale AS (
            SELECT 
                'TOTALE' AS senderpaid,
                SUM(conteggio_fulmine) AS conteggio_fulmine,
                SUM(conteggio_poste) AS conteggio_poste,
                SUM(conteggio_po_se) AS conteggio_po_se,
                SUM(conteggio_sailpost) AS conteggio_sailpost,
                SUM(totale) AS totale
            FROM aggregati
        )
        SELECT *
        FROM (
            SELECT * FROM aggregati
            UNION ALL
            SELECT * FROM totale
        ) t
        ORDER BY 
            CASE WHEN t.senderpaid = 'TOTALE' THEN 1 ELSE 0 END,
            t.senderpaid ASC;
    
    """

    df_completa_spark = spark.sql(query_sql_pivot_completa)
    df_senza_chiuse_spark = spark.sql(query_sql_pivot_senza_chiuse)

    logging.info("Trasformazione in Pandas DataFrame...")

    return {
        "pivot_completa": df_completa_spark.toPandas(),
        "pivot_senza_chiuse": df_senza_chiuse_spark.toPandas()
    }



# FUNZIONI AUSILIARIE
def get_last_update_date(spark: SparkSession) -> str:
    """Estrae la data più recente (MAX(requesttimestamp)) dal dataset."""
    logging.info("Estrazione data ultimo aggiornamento...")
    
    max_date_df = spark.sql("SELECT MAX(requesttimestamp) AS max_ts FROM send.gold_postalizzazione_analytics") 
    max_date = max_date_df.collect()[0]["max_ts"] 

    if isinstance(max_date, datetime): 
        return max_date.strftime("%Y-%m-%d %H:%M:%S") 
    return str(max_date)


def export_to_sheets(df: pd.DataFrame, creds: dict, sheet_id: str, sheet_name: str):
    logging.info(f"Scrittura su Google Sheet: {sheet_name}")
    df = df.astype(str)
    sheet = Sheet(sheet_id=sheet_id, service_credentials=creds, id_mode='key')
    sheet.upload(sheet_name, df)
    logging.info("Scrittura completata.")


# ---------------- MAIN SCRIPT ----------------

def main():
    logging.info("Inizializzazione SparkSession...")
    spark = SparkSession.builder.appName("SSDA-477 Estrazione Critcità").getOrCreate()

    # Caricamento credenziali Google
    creds = load_google_credentials(GOOGLE_SECRET_PATH)

    # Lettura da Sheet di INPUT
    logging.info("Lettura elenco reqestid critici da Google Sheets...")
    sheet = Sheet(sheet_id=SHEET_ID_INPUT, service_credentials=creds, id_mode='key')
    df_input = sheet.download("input")

    if not df_input.empty:
        colonna_id = df_input.columns[0] #devo prendere la prima colonna del file contenente i requestid
        ids_input = df_input[colonna_id].dropna().astype(str).str.strip().unique().tolist()
        logging.info(f"Trovati {len(ids_input)} requestid in SSDA-477_Criticità_input")
        df_ids = spark.createDataFrame([(i,) for i in ids_input], ["requestid"])
        df_ids.createOrReplaceTempView("input_critiche")
    else:
        logging.warning("Sheet Criticità input vuoto")
        empty_df = spark.createDataFrame([], "requestid string")
        empty_df.createOrReplaceTempView("input_critiche")

    # Esecuzione della query
    df_output = run_query(spark)

    for sheet_name, df in df_output.items():
        logging.info(f"Scrittura su sheet: {sheet_name}")
        export_to_sheets(df, creds, SHEET_ID_OUTPUT, sheet_name)

    # Estrarre la data di aggiornamento e scriverla nella pagina dedicata
    max_date_str = get_last_update_date(spark)
    job_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logging.info(f"Ultimo aggiornamento dati gold_postalizzazione: {max_date_str}")
    logging.info(f"Data di esecuzione del job: {job_time}")

    update_df = pd.DataFrame({
        "Ultimo aggiornamento dati (MAX requesttimestamp)": [max_date_str],
        "Data esecuzione script (UTC)": [job_time]
    })

    # Scrittura della data di aggiornamento
    sheet = Sheet(sheet_id=SHEET_ID_OUTPUT, service_credentials=creds, id_mode='key')
    sheet.upload("Data aggiornamento", update_df)

    # Chiusura Spark
    spark.stop()
    logging.info("Processo completato con successo.")


if __name__ == "__main__":
    main()
