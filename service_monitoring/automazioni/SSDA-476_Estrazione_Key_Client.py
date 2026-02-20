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
SHEET_ID = "1lUXRHPi1TajWOMxWhDO_kef7eSENjjXMwQWjDzBND5c"

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
    """Esegue la query su Spark e restituisce un DataFrame Pandas."""
    logging.info("Esecuzione query Key Client...")

    query_sql = """
        WITH vista_conteggio AS (
            SELECT senderpaid,
                COUNT(*) AS totale_affidi,
                COUNT(CASE WHEN fine_recapito_stato IS NOT NULL THEN 1 END) AS completate,
                COUNT(CASE WHEN fine_recapito_stato IS NULL THEN 1 END) AS totale_in_corso,
                COUNT(CASE WHEN fine_recapito_stato IS NULL AND flag_wi7_report_postalizzazioni_incomplete = 0 THEN 1 END) AS in_corso,
                COUNT(CASE WHEN flag_wi7_report_postalizzazioni_incomplete = 1 AND DATEDIFF(NOW(), requesttimestamp) <= 30 THEN 1 END) AS fuori_sla_un_mese,
                COUNT(CASE WHEN flag_wi7_report_postalizzazioni_incomplete = 1 AND DATEDIFF(NOW(), requesttimestamp) > 30 AND DATEDIFF(NOW(), requesttimestamp) <= 60 THEN 1 END) AS fuori_sla_due_mesi,
                COUNT(CASE WHEN flag_wi7_report_postalizzazioni_incomplete = 1 AND DATEDIFF(NOW(), requesttimestamp) > 60 THEN 1 END) AS fuori_sla_piu_due_mesi
            FROM send.gold_postalizzazione_analytics
            WHERE accettazione_recapitista_data IS NOT NULL --AND requesttimestamp < '2025-06-23'
            AND requestid NOT IN ( --fixed
                SELECT requestid_computed
                FROM send.silver_postalizzazione_denormalized 
                WHERE statusrequest IN ('PN999', 'PN998')
            )
            GROUP BY senderpaid
        ), selfcare_view AS (
        SELECT DISTINCT internalistitutionid as paid,
            CONCAT_WS(' - ', institution.rootParent.description, institution.description) as senderdenomination
        FROM
            selfcare.silver_contracts
        WHERE
            product = 'prod-pn' AND state = 'ACTIVE' 
        ) SELECT v.senderpaid,
                s.senderdenomination,
                v.totale_affidi,
                v.completate,
                v.totale_in_corso,
                v.in_corso,
                v.fuori_sla_un_mese,
                v.fuori_sla_due_mesi,
                v.fuori_sla_piu_due_mesi,
                ROUND(((v.fuori_sla_un_mese + v.fuori_sla_due_mesi + v.fuori_sla_piu_due_mesi) / NULLIF((v.totale_in_corso), 0)),4) AS percentuale_fuori_sla_totale_in_corso
        FROM vista_conteggio v LEFT JOIN selfcare_view s ON (v.senderpaid = s.paid)
        WHERE totale_in_corso >= 1000 
        ORDER BY percentuale_fuori_sla_totale_in_corso DESC;
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
    spark = SparkSession.builder.appName("SSDA-476 Estrazione Key Client").getOrCreate()

    # Caricamento credenziali Google
    creds = load_google_credentials(GOOGLE_SECRET_PATH)

    # Esecuzione della query
    df = run_query(spark)

    sheet_name = "Estrazione SSDA-476"
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
