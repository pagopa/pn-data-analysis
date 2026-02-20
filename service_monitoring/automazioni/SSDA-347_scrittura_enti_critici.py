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
SHEET_ID = "1IeZrdTFF9TFFaQ9jBG3QSmlQSag6vILWUdGzYO1qGb0"

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
    logging.info("Esecuzione query Enti Critici...")

    query_sql = """
        WITH dati_gold_filtrati AS (
            SELECT
                g.senderpaid,
                sentat
            FROM send.gold_notification_analytics g
        ),
        vista_senderdenomination_affidi AS (
            SELECT DISTINCT 
                internalistitutionid,
                institution.rootParent.id AS rootparent_id,
                CONCAT_WS(' - ', institution.rootParent.description, institution.description) AS senderdenomination,
                institution.rootParent.description AS rootparent_description,
                t.sentat
            FROM selfcare.silver_contracts e
                LEFT JOIN dati_gold_filtrati t ON t.senderpaid = e.internalistitutionid
            WHERE internalistitutionid IN (SELECT internalistitutionid FROM send_dev.temp_enti_critici)
        ),
        vista_aggregata AS (
            SELECT
                COALESCE(rootparent_id, internalistitutionid) AS enteid,
                COALESCE(rootparent_description, senderdenomination) AS senderdenomination,
                COUNT(CASE WHEN MONTH(sentat)=10 AND YEAR(sentat) = 2025 THEN 1 END) AS conteggio_depositi_ottobre, 
                COUNT(CASE WHEN MONTH(sentat)=11 AND YEAR(sentat) = 2025 THEN 1 END) AS conteggio_depositi_novembre,
                COUNT(CASE WHEN MONTH(sentat)=12 AND YEAR(sentat) = 2025 THEN 1 END) AS conteggio_depositi_dicembre,
                COUNT(CASE WHEN sentat >= '2025-10-01' THEN 1 END) AS conteggio_depositi_totale_q4
            FROM vista_senderdenomination_affidi
            GROUP BY COALESCE(rootparent_id, internalistitutionid), COALESCE(rootparent_description, senderdenomination)
        )
        SELECT
            CASE 
                WHEN enteid = 'f05dd442-4bda-4116-ad15-54ec071c6fee' THEN '9a7c1b23-46a3-489b-8ed4-398ffb32b45a'
                ELSE enteid
            END AS enteid,
            CASE 
                WHEN senderdenomination = "Citta' Metropolitana di Roma Capitale" THEN 'ROMA CAPITALE'
                ELSE senderdenomination
            END AS senderdenomination,
            SUM(conteggio_depositi_ottobre) AS conteggio_depositi_ottobre,
            SUM(conteggio_depositi_novembre) AS conteggio_depositi_novembre,
            SUM(conteggio_depositi_dicembre) AS conteggio_depositi_dicembre,
            SUM(conteggio_depositi_totale_q4) AS conteggio_depositi_totale_q4
        FROM vista_aggregata
        GROUP BY
            CASE 
                WHEN enteid = 'f05dd442-4bda-4116-ad15-54ec071c6fee' THEN '9a7c1b23-46a3-489b-8ed4-398ffb32b45a'
                ELSE enteid
            END,
            CASE 
                WHEN senderdenomination = "Citta' Metropolitana di Roma Capitale" THEN 'ROMA CAPITALE'
                ELSE senderdenomination
            END
        ORDER BY senderdenomination ASC; """

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
    spark = SparkSession.builder.appName("Enti Critici Scrittura").getOrCreate()

    # Caricamento credenziali Google
    creds = load_google_credentials(GOOGLE_SECRET_PATH)
    sheet = Sheet(sheet_id=SHEET_ID, service_credentials=creds, id_mode='key')

    # Esecuzione query principale
    df = run_query(spark)

    # Determina il nome dello sheet di destinazione
    today = datetime.now()
    weekday = today.weekday()  # 0 = lunedì, 1 = martedì, ..., 6 = domenica
    if weekday == 0:  # Lunedì
        sheet_name = f"Estrazione Enti Critici {today.strftime('%Y-%m-%d')}"
        logging.info(f"Giornata odierna è lunedì. Scrittura su sheet: {sheet_name}")
    else:
        sheet_name = "Estrazione Enti Critici"
        logging.info(f"Scrittura su sheet standard: {sheet_name}")

    # Scrittura dei dati su Google Sheets
    export_to_sheets(df, creds, SHEET_ID, sheet_name)

    # Estrarre la data di aggiornamento e scriverla nella pagina dedicata
    max_date_str = get_last_update_date(spark)
    job_time = today.strftime("%Y-%m-%d %H:%M:%S")

    logging.info(f"Ultimo aggiornamento dati nel Data Lake: {max_date_str}")
    logging.info(f"Data di esecuzione del job: {job_time}")

    update_df = pd.DataFrame({
        "Ultimo aggiornamento dati (MAX requesttimestamp)": [max_date_str],
        "Data esecuzione script (UTC)": [job_time]
    })

    sheet.upload("Data aggiornamento", update_df)

    # Chiusura Spark
    spark.stop()
    logging.info("Processo completato con successo.")


if __name__ == "__main__":
    main()
