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
SHEET_ID = "1mkuK3FvVq5aLH5_qd5HxiOZYRr1kKkB0IzzLoh50FCs" #trovare il modo di stampare su terminale il percorso del file


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



def run_query(spark: SparkSession) -> pd.DataFrame:
    logging.info("Esecuzione query...")

    query_sql = """
        SELECT 
            gpa.attemptid, 
            gna.iun
        FROM 
            send.gold_postalizzazione_analytics gpa
        JOIN 
            (SELECT DISTINCT iun
             FROM send.gold_notification_analytics
             WHERE actual_status = 'DELIVERING'
               AND sentat < ADD_MONTHS(DATE_TRUNC('MONTH', NOW()), -10)
            ) gna
        ON gpa.iun = gna.iun
        WHERE 
            gpa.pcretry_rank = 1
        """

    df_spark = spark.sql(query_sql) 
    logging.info("Trasformazione in Pandas DataFrame...") 
    return df_spark.toPandas()



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
    """Esporta i dati su Google Sheet."""
    logging.info(f"Scrittura su Google Sheet: {sheet_name}")
    df = df.astype(str)
    sheet = Sheet(sheet_id=sheet_id, service_credentials=creds, id_mode='key')
    sheet.upload(sheet_name, df)
    logging.info("Scrittura completata.")


# ---------------- MAIN SCRIPT ----------------

def main():
    logging.info("Inizializzazione SparkSession...")
    spark = SparkSession.builder.appName("SSDA-507 Sentat maggiore di un anno non perfezionate").getOrCreate()

    # Caricamento credenziali Google
    creds = load_google_credentials(GOOGLE_SECRET_PATH)

    # Esecuzione della query
    df = run_query(spark)

    sheet_name = "Estrazione monthly SSDA-507"
    logging.info(f"Scrittura su sheet standard: {sheet_name}")
    # Scrittura dei dati su Google Sheets
    export_to_sheets(df, creds, SHEET_ID, sheet_name)

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
    sheet = Sheet(sheet_id=SHEET_ID, service_credentials=creds, id_mode='key')
    sheet.upload("Data aggiornamento", update_df)

    # Chiusura Spark
    spark.stop()
    logging.info("Processo completato con successo.")


if __name__ == "__main__":
    main()
