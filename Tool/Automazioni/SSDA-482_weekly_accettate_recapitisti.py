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
SHEET_ID_SAILPOST = "1Ifxo5ba50MXRqxtSrr7tV0k0P0LNQOm9wlR9z7Vlf8Y"
SHEET_ID_POSE = "1JgOfEQ6WZmkabpFdA_ovhStMqZIDlFqUdZEdoQJIk4o"

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
    """Esegue la query su Spark e restituisce un DataFrame Pandas."""
    logging.info("Esecuzione query Accettate Recapitisti...")

    dati_gold_corretti = """WITH dati_gold_corretti AS (
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
        ),"""

    query_sql_base_Sailpost = """
        temp AS (
            SELECT 
                senderpaid,
                iun,
                requestID,
                requesttimestamp AS requestDateTime,
                prodotto,
                geokey,
                recapitista_unif,
                lotto,
                --- flag 30bis fixed, ovvero se io ho lotto 30 e regione puglia scrivo 30 Puglia, altrimenti 30 accordo
                CASE WHEN lotto = '30' AND c.regione NOT IN ('Puglia') THEN 1
                    ELSE 0
                END AS flag_30bis,
                c.regione,
                c.provincia,
                CONCAT ("'", codice_oggetto) AS codiceOggetto,
                affido_consolidatore_data,
                stampa_imbustamento_CON080_data,
                affido_recapitista_CON016_data,
                accettazione_recapitista_CON018_data,
                COALESCE(accettazione_recapitista_CON018_data, affido_recapitista_CON016_data) AS data_accettazione_CON018_CON016,
                affido_conservato_CON020_data,
                materialita_pronta_CON09A_data,
                scarto_consolidatore_stato,
                scarto_consolidatore_data,
                tentativo_recapito_stato,
                tentativo_recapito_data,
                tentativo_recapito_data_rendicontazione,
                messaingiacenza_recapito_stato,
                messaingiacenza_recapito_data,
                messaingiacenza_recapito_data_rendicontazione,
                certificazione_recapito_stato,
                certificazione_recapito_dettagli,
                certificazione_recapito_data,
                certificazione_recapito_data_rendicontazione,
                fine_recapito_stato,
                fine_recapito_data,
                fine_recapito_data_rendicontazione,
                accettazione_23L_RECAG012_data,
                accettazione_23L_RECAG012_data_rendicontazione,
                rend_23L_stato,
                rend_23L_data,
                rend_23L_data_rendicontazione,
                perfezionamento_data,
                perfezionamento_tipo,
                perfezionamento_notificationdate,
                perfezionamento_stato,
                perfezionamento_stato_dettagli,
                flag_wi7_report_postalizzazioni_incomplete, 
                wi7_cluster,
                CASE WHEN fine_recapito_stato IS NOT NULL THEN 'Completa' 
                     WHEN fine_recapito_stato IS NULL AND flag_wi7_report_postalizzazioni_incomplete = 0 THEN 'In_Corso'
                     ELSE 'Incompleta' 
                END AS stato_notifica
            FROM dati_gold_corretti g JOIN send_dev.cap_area_provincia_regione c ON (c.cap = g.geokey)
            WHERE scarto_consolidatore_stato IS NULL 
                  AND recapitista_unif= 'RTI Sailpost-Snem'
            ) 
        """
    query_sql_base_PoSe = """
        temp AS (
            SELECT 
                senderpaid,
                iun,
                requestID,
                requesttimestamp AS requestDateTime,
                prodotto,
                geokey,
                recapitista_unif,
                lotto,
                --- flag 30bis fixed, ovvero se io ho lotto 30 e regione puglia scrivo 30 Puglia, altrimenti 30 accordo
                CASE WHEN lotto = '30' AND c.regione NOT IN ('Puglia') THEN 1
                    ELSE 0
                END AS flag_30bis,
                c.regione,
                c.provincia,
                CONCAT ("'", codice_oggetto) AS codiceOggetto,
                affido_consolidatore_data,
                stampa_imbustamento_CON080_data,
                affido_recapitista_CON016_data,
                accettazione_recapitista_CON018_data,
                COALESCE(accettazione_recapitista_CON018_data, affido_recapitista_CON016_data) AS data_accettazione_CON018_CON016,
                affido_conservato_CON020_data,
                materialita_pronta_CON09A_data,
                scarto_consolidatore_stato,
                scarto_consolidatore_data,
                tentativo_recapito_stato,
                tentativo_recapito_data,
                tentativo_recapito_data_rendicontazione,
                messaingiacenza_recapito_stato,
                messaingiacenza_recapito_data,
                messaingiacenza_recapito_data_rendicontazione,
                certificazione_recapito_stato,
                certificazione_recapito_dettagli,
                certificazione_recapito_data,
                certificazione_recapito_data_rendicontazione,
                fine_recapito_stato,
                fine_recapito_data,
                fine_recapito_data_rendicontazione,
                accettazione_23L_RECAG012_data,
                accettazione_23L_RECAG012_data_rendicontazione,
                rend_23L_stato,
                rend_23L_data,
                rend_23L_data_rendicontazione,
                perfezionamento_data,
                perfezionamento_tipo,
                perfezionamento_notificationdate,
                perfezionamento_stato,
                perfezionamento_stato_dettagli,
                flag_wi7_report_postalizzazioni_incomplete, 
                wi7_cluster,
                CASE WHEN fine_recapito_stato IS NOT NULL THEN 'Completa' 
                     WHEN fine_recapito_stato IS NULL AND flag_wi7_report_postalizzazioni_incomplete = 0 THEN 'In_Corso'
                     ELSE 'Incompleta' 
                END AS stato_notifica
            FROM dati_gold_corretti g JOIN send_dev.cap_area_provincia_regione c ON (c.cap = g.geokey)
            WHERE scarto_consolidatore_stato IS NULL 
                  AND recapitista_unif= 'POST & SERVICE'
            ) 
        """
    
    vista_aggregata_lotto = """
        SELECT 
            t.lotto,
            SUM(CASE WHEN t.fine_recapito_stato IS NOT NULL THEN 1 ELSE 0 END) AS completate,
            SUM(CASE WHEN t.fine_recapito_stato IS NULL AND t.flag_wi7_report_postalizzazioni_incomplete = 0 THEN 1 ELSE 0 END) AS in_corso,
            SUM(CASE WHEN t.fine_recapito_stato IS NULL AND t.flag_wi7_report_postalizzazioni_incomplete = 1 THEN 1 ELSE 0 END) AS incomplete,
            COUNT(*) AS totale
        FROM temp t
        WHERE t.data_accettazione_CON018_CON016 >= '2025-01-01'
        AND t.requestid NOT IN (
                SELECT requestid_computed
                FROM send.silver_postalizzazione_denormalized
                WHERE statusrequest IN ('PN999', 'PN998')
            )
        AND flag_30bis = 0 
        GROUP BY GROUPING SETS (
            (t.lotto),  -- subtotale per lotto
            ()          -- totale generale
        )
        ORDER BY 
            t.lotto NULLS LAST;"""
    
    vista_aggregata_lotto_anno_mese = """
        SELECT 
            t.lotto,
            SUBSTRING(CAST(COALESCE(t.data_accettazione_CON018_CON016, t.requestDateTime) AS STRING), 1, 7) AS mese_anno_accettazione,
            SUM(CASE WHEN t.fine_recapito_stato IS NOT NULL THEN 1 ELSE 0 END) AS completate,
            SUM(CASE WHEN t.fine_recapito_stato IS NULL AND t.flag_wi7_report_postalizzazioni_incomplete = 0 THEN 1 ELSE 0 END) AS in_corso,
            SUM(CASE WHEN t.fine_recapito_stato IS NULL AND t.flag_wi7_report_postalizzazioni_incomplete = 1 THEN 1 ELSE 0 END) AS incomplete,
            COUNT(*) AS totale
        FROM temp t
        WHERE t.data_accettazione_CON018_CON016 >= '2025-01-01'
        AND t.requestid NOT IN (
                SELECT requestid_computed
                FROM send.silver_postalizzazione_denormalized
                WHERE statusrequest IN ('PN999', 'PN998')
            )
        AND flag_30bis = 0 
        GROUP BY GROUPING SETS (
            (t.lotto, SUBSTRING(CAST(COALESCE(t.data_accettazione_CON018_CON016, t.requestDateTime) AS STRING), 1, 7)), -- dettaglio
            (t.lotto),  -- subtotale per lotto
            ()          -- totale generale
        )
        ORDER BY 
            t.lotto NULLS LAST,
            mese_anno_accettazione NULLS LAST;
    
    """
    
    vista_aggregata_regione = """
        , conteggi AS (
                SELECT
                    t.lotto,
                    t.regione,
                    t.provincia,
                    COUNT(*) AS conteggio_provincia,
                    SUM(COUNT(*)) OVER (PARTITION BY t.lotto, t.regione) AS totale_regione
                FROM temp t
                WHERE t.data_accettazione_CON018_CON016 >= '2025-01-01'
                AND t.requestid NOT IN (
                        SELECT requestid_computed
                        FROM send.silver_postalizzazione_denormalized
                        WHERE statusrequest IN ('PN999', 'PN998')
                )
                AND flag_30bis = 0
                AND stato_notifica NOT IN ('Completa') 
                GROUP BY t.lotto, t.regione, t.provincia
            )
            SELECT
                lotto,
                provincia AS provincia_regione,
                conteggio_provincia AS conteggio,
                CONCAT(CAST(ROUND(100.0 * conteggio_provincia / totale_regione, 0) AS STRING), '%') AS percentuale -- togliere i due decimali 
            FROM conteggi
            UNION ALL
            -- Riga di somma per la REGIONE (100%)
            SELECT
                lotto,
                regione AS provincia_regione,
                totale_regione AS conteggio,
                '100%' AS percentuale
            FROM conteggi
            GROUP BY lotto, regione, totale_regione
            ORDER BY lotto ASC, percentuale DESC;"""
    
    # Query di Sailpost
    aggregato_Sailpost_lotto = dati_gold_corretti + query_sql_base_Sailpost + vista_aggregata_lotto
    aggregato_Sailpost_lotto_regione = dati_gold_corretti + query_sql_base_Sailpost + vista_aggregata_regione
    # Query di POST & SERVICE
    aggregato_PoSe_lotto_anno_mese = dati_gold_corretti + query_sql_base_PoSe + vista_aggregata_lotto_anno_mese
    aggregato_PoSe_lotto_regione = dati_gold_corretti + query_sql_base_PoSe + vista_aggregata_regione
    
    # Esecuzione delle query Spark - passaggio a df spark
    df_sailpost_lotto_spark = spark.sql(aggregato_Sailpost_lotto)
    df_sailpost_regione_spark = spark.sql(aggregato_Sailpost_lotto_regione)

    df_pose_lotto_mese_spark = spark.sql(aggregato_PoSe_lotto_anno_mese)
    df_pose_regione_spark = spark.sql(aggregato_PoSe_lotto_regione)

    logging.info("Trasformazione in Pandas DataFrame...")

    # Conversione in Pandas
    df_sailpost_lotto = df_sailpost_lotto_spark.toPandas()
    df_sailpost_regione = df_sailpost_regione_spark.toPandas()

    df_pose_lotto_mese = df_pose_lotto_mese_spark.toPandas()
    df_pose_regione = df_pose_regione_spark.toPandas()

    # Restituzione del dizionario finale
    return {
        "sailpost_lotto": df_sailpost_lotto,
        "sailpost_lotto_regione": df_sailpost_regione,
        "pose_lotto_anno_mese": df_pose_lotto_mese,
        "pose_lotto_regione": df_pose_regione,
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
    spark = SparkSession.builder.appName("SSDA-482 Estrazione Accettate Recapitisti").getOrCreate()

    # Caricamento credenziali Google
    creds = load_google_credentials(GOOGLE_SECRET_PATH)

    # Esecuzione delle query
    logging.info("Esecuzione query Sailpost / PoSe ...")
    results = run_query(spark)

    # SAILPOST
    logging.info("Scrittura su SHEET_ID_SAILPOST ...")

    export_to_sheets(
        results["sailpost_lotto"],
        creds,
        SHEET_ID_SAILPOST,
        "Sailpost - Lotto"
    )

    export_to_sheets(
        results["sailpost_lotto_regione"],
        creds,
        SHEET_ID_SAILPOST,
        "Sailpost - Regione"
    )

    # POSE 
    logging.info("Scrittura su SHEET_ID_POSE ...")

    export_to_sheets(
        results["pose_lotto_anno_mese"],
        creds,
        SHEET_ID_POSE,
        "PoSe - Lotto Mese"
    )

    export_to_sheets(
        results["pose_lotto_regione"],
        creds,
        SHEET_ID_POSE,
        "PoSe - Regione"
    )

    # Estrarre la data di aggiornamento
    max_date_str = get_last_update_date(spark)
    job_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logging.info(f"Ultimo aggiornamento dati gold_postalizzazione: {max_date_str}")
    logging.info(f"Data di esecuzione del job: {job_time}")

    update_df = pd.DataFrame({
        "Ultimo aggiornamento dati (MAX requesttimestamp)": [max_date_str],
        "Data esecuzione script (UTC)": [job_time]
    })

    # Aggiornamento SAILPOST
    sheet_sail = Sheet(sheet_id=SHEET_ID_SAILPOST, service_credentials=creds, id_mode='key')
    sheet_sail.upload("Data aggiornamento", update_df)

    # Aggiornamento POSE
    sheet_pose = Sheet(sheet_id=SHEET_ID_POSE, service_credentials=creds, id_mode='key')
    sheet_pose.upload("Data aggiornamento", update_df)

    # Chiusura Spark
    spark.stop()
    logging.info("Processo completato con successo.")


if __name__ == "__main__":
    main()
