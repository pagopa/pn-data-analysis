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
SHEET_ID = "1k5W20xRqHtAdqxld7fIMfS4l-kdlS7YQJAjyTJ-o-o8"

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



def run_query(spark: SparkSession, data_limite: str) -> dict:
    """Esegue la query su Spark e restituisce un DataFrame Pandas."""

    logging.info(f"Esecuzione query con data limite = {data_limite}")

    query_base = """
        WITH base AS (
                SELECT
                    CASE
                        WHEN codice_oggetto LIKE 'R14%' THEN 'Fulmine'
                        WHEN codice_oggetto LIKE '777%' OR codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
                        WHEN codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
                        WHEN codice_oggetto LIKE '69%' OR codice_oggetto LIKE '381%' OR codice_oggetto LIKE 'RB1%' THEN 'Poste'
                        ELSE recapitista_unificato
                    END AS recapitista_unif,
                    CASE 
                        WHEN accettazione_recapitista_CON018_data IS NOT NULL 
                            THEN accettazione_recapitista_CON018_data
                        WHEN affido_recapitista_con016_data IS NOT NULL
                            THEN affido_recapitista_con016_data
                        ELSE requesttimestamp
                    END AS affido_accettazione_rec_data,
                    flag_wi7_report_postalizzazioni_incomplete
                FROM send.gold_postalizzazione_analytics d
                WHERE scarto_consolidatore_stato IS NULL
                      AND d.statusrequest NOT IN ('PN999', 'PN998')
            )"""

    trend_completo = f"""
        SELECT
            DATE_TRUNC('month', affido_accettazione_rec_data) AS mese,
            COUNT(CASE WHEN recapitista_unif = 'Poste' THEN 1 END) AS Poste_totale_affidi,
            COUNT(CASE WHEN recapitista_unif = 'Fulmine' THEN 1 END) AS Fulmine_totale_affidi,
            COUNT(CASE WHEN recapitista_unif = 'POST & SERVICE' THEN 1 END) AS PS_totale_affidi,
            COUNT(CASE WHEN recapitista_unif = 'RTI Sailpost-Snem' THEN 1 END) AS Sailpost_totale_affidi
        FROM base
        WHERE affido_accettazione_rec_data >= DATE '2024-12-01'
              AND affido_accettazione_rec_data <= DATE '{data_limite}'
        GROUP BY mese
        ORDER BY mese;
        """
    #totale affidi - trend
    
    trend_fuori_sla = f"""
        SELECT
            DATE_TRUNC('month', affido_accettazione_rec_data) AS mese,
            COUNT(CASE WHEN recapitista_unif = 'Poste' THEN 1 END) AS Poste_totale_affidi,
            COUNT(CASE WHEN recapitista_unif = 'Fulmine' THEN 1 END) AS Fulmine_totale_affidi,
            COUNT(CASE WHEN recapitista_unif = 'POST & SERVICE' THEN 1 END) AS PS_totale_affidi,
            COUNT(CASE WHEN recapitista_unif = 'RTI Sailpost-Snem' THEN 1 END) AS Sailpost_totale_affidi
        FROM base
        WHERE affido_accettazione_rec_data >= DATE '2024-12-01'
              AND affido_accettazione_rec_data <= DATE '{data_limite}'
              AND flag_wi7_report_postalizzazioni_incomplete = 1
        GROUP BY mese
        ORDER BY mese;
    """
    #totale fuori sla - trend

    trend_completo_spark = spark.sql(query_base + trend_completo)
    trend_fuori_sla_spark = spark.sql(query_base + trend_fuori_sla)

    # Conversione in Pandas
    df_trend_completo = trend_completo_spark.toPandas()
    df_trend_fuori_sla = trend_fuori_sla_spark.toPandas()

    # Restituzione del dizionario finale
    return {
        "trend_completo": df_trend_completo,
        "trend_fuori_sla": df_trend_fuori_sla
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
    """Esporta i dati su Google Sheet."""
    logging.info(f"Scrittura su Google Sheet: {sheet_name}")
    df = df.astype(str)
    sheet = Sheet(sheet_id=sheet_id, service_credentials=creds, id_mode='key')
    sheet.upload(sheet_name, df)
    logging.info("Scrittura completata.")


# ---------------- MAIN SCRIPT ----------------

def main():
    logging.info("Inizializzazione SparkSession...")
    spark = SparkSession.builder.appName("SSDA-485 Estrazione Settimanale - SAL Service Monitoring").getOrCreate()

    # Caricamento credenziali Google
    creds = load_google_credentials(GOOGLE_SECRET_PATH)

    # Data limite = oggi
    data_limite = datetime.today().strftime("%Y-%m-%d")
    logging.info(f"Data limite impostata a: {data_limite}")

    # Esecuzione della query
    df_dict = run_query(spark, data_limite)

    # Scrittura dei fogli usando le chiavi del dizionario come sheet name
    for sheet_name, dataframe in df_dict.items():
        logging.info(f"Scrittura su Google Sheet: {sheet_name}")
        export_to_sheets(dataframe, creds, SHEET_ID, sheet_name)

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
