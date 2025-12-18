import configparser
import logging
import math
import uuid

from pyspark.sql.functions import lit, to_date, month, years, col
from pyspark.sql import SparkSession, DataFrameWriter
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta

######################################### Configuration and initialization

spark = SparkSession.builder.getOrCreate()

query = """
        WITH fatturazione AS (
            SELECT DISTINCT
                iun,
                category,
                get_json_object(details, '$.recIndex') AS recindex,
                CAST(notificationSentAt AS TIMESTAMP)  AS data_deposito,
                CAST(invoincingTimestamp AS TIMESTAMP) AS data_fatturazione
            FROM send.silver_invoicing_timeline
            WHERE CAST(invoincingTimestamp AS TIMESTAMP) BETWEEN CAST('2025-01-01 00:00:00' AS TIMESTAMP)
                    AND CAST('2025-12-12 23:59:59' AS TIMESTAMP)
            AND paid = '53b40136-65f2-424b-acfb-7fae17e35c60'
            AND category IN ('REFINEMENT', 'NOTIFICATION_VIEWED', 'ANALOG_WORKFLOW_RECIPIENT_DECEASED')
        ),notif AS (
            SELECT DISTINCT
                f.iun,
                f.recindex,
                f.category,
                f.data_deposito,
                f.data_fatturazione,
                n.type_notif
            FROM fatturazione f
            JOIN send.gold_notification_analytics n ON f.iun = n.iun
            WHERE n.type_notif = 'ANALOG' 
        ),
        analog_attempts AS (
            SELECT
                t.iun,
                 CAST(t.`timestamp` AS TIMESTAMP) AS analog_feedback_tms,
                ROW_NUMBER() OVER (PARTITION BY t.iun ORDER BY CAST(regexp_extract(t.timelineelementid, 'ATTEMPT_([0-9]+)', 1) AS INT) DESC) AS rn,
                SUBSTRING(t.timelineelementid, 22, 50) AS req_id
            FROM send.silver_timeline t
            WHERE t.category = 'SEND_ANALOG_FEEDBACK'
        ),
        gpa_dedup AS (
            SELECT DISTINCT SUBSTRING(requestid, 25, 50) AS req_id,
                requestid,
                lotto,
                prodotto,
                prezzo_ente,
                costo_recapitista,
                costo_consolidatore,
                CASE
                        WHEN codice_oggetto LIKE 'R14%' THEN 'Fulmine'
                        WHEN codice_oggetto LIKE '777%' OR codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
                        WHEN codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
                        WHEN codice_oggetto LIKE '69%'  OR codice_oggetto LIKE '381%' OR codice_oggetto LIKE 'RB1%' THEN 'Poste'
                        ELSE recapitista_unificato
                    END AS recapitista_unif
            FROM send.gold_postalizzazione_analytics
            WHERE pcretry_rank = 1
        )
        SELECT
            n.iun,
            g.requestid,
            n.recindex,
            n.category,
            n.type_notif,
            n.data_deposito,
            n.data_fatturazione,
            a.analog_feedback_tms,
            LEAST(a.analog_feedback_tms,n.data_fatturazione) AS end_date,
            g.lotto,
            g.prodotto,
            g.recapitista_unif AS recapitista,
            g.prezzo_ente,
            g.costo_recapitista,
            g.costo_consolidatore,
            concat(
                cast(year(n.data_fatturazione) AS string),
                '_Q',
                cast(quarter(n.data_fatturazione) AS string)
            ) AS quarter_fatturazione
        FROM analog_attempts a
        JOIN notif n ON a.iun = n.iun
        LEFT JOIN gpa_dedup g ON a.req_id = g.req_id
        WHERE a.rn = 1
        """

# Esecuzione della query spark e salvataggio in un dataframe
df1 = spark.sql(query)

df1.createOrReplaceTempView("gold_postalizzazione")

######################################### Conteggio dei record -- TO DO: ranking unico

record_count_df = spark.sql("""
    SELECT                       
        COUNT(*) AS total_records
    FROM 
        gold_postalizzazione
    """)

# Dataframe delle festività -- N.B. da aggiungere presto quelle del 2026
festivita = [
    ('2023-01-01', 'Capodanno'),
    ('2023-01-06', 'Epifania'),
    ('2023-04-09', 'Pasqua'),
    ('2023-04-10', 'Lunedì dell\'Angelo'),
    ('2023-04-25', 'Festa della Liberazione'),
    ('2023-05-01', 'Festa dei Lavoratori'),
    ('2023-06-02', 'Festa della Repubblica'),
    ('2023-08-15', 'Ferragosto'),
    ('2023-11-01', 'Tutti i Santi'),
    ('2023-12-08', 'Immacolata Concezione'),
    ('2023-12-25', 'Natale'),
    ('2023-12-26', 'Santo Stefano'),

    ('2024-01-01', 'Capodanno'),
    ('2024-01-06', 'Epifania'), 
    ('2024-04-01', 'Lunedì dell\'Angelo'),
    ('2024-03-31', 'Pasqua'),
    ('2024-04-25', 'Festa della Liberazione'),
    ('2024-05-01', 'Festa dei Lavoratori'),
    ('2024-06-02', 'Festa della Repubblica'),
    ('2024-08-15', 'Ferragosto'),
    ('2024-11-01', 'Tutti i Santi'),
    ('2024-12-25', 'Natale'),
    ('2024-12-26', 'Santo Stefano'),

    ('2025-01-01', 'Capodanno'),
    ('2025-01-06', 'Epifania'),
    ('2025-04-20', 'Pasqua'),
    ('2025-04-21', 'Lunedì dell\'Angelo'),
    ('2025-04-25', 'Festa della Liberazione'),
    ('2025-05-01', 'Festa dei Lavoratori'),
    ('2025-06-02', 'Festa della Repubblica'),
    ('2025-08-15', 'Ferragosto'),
    ('2025-11-01', 'Ognissanti'),
    ('2025-12-08', 'Immacolata Concezione'),
    ('2025-12-25', 'Natale'),
    ('2025-12-26', 'Santo Stefano')
]

holiday_dates = {datetime.strptime(date, '%Y-%m-%d').date() for date, _ in festivita}

festivita_df = spark.createDataFrame(festivita, ["data", "descrizione"])

festivita_df = festivita_df.withColumn("data", to_date(festivita_df["data"], "yyyy-MM-dd"))

festivita_df.createOrReplaceTempView("FestivitaView")

#festivita_df.show(10)


######################################### Funzione custom per il calcolo dei giorni lavorativi

def datediff_workdays(start_date, end_date):
    try:
        if start_date is None or end_date is None:
            return None

        if end_date < start_date:
            return 0

        start_date_next_day = start_date.replace(hour=0, minute=0, second=0) + timedelta(days=1)

        total_days = (end_date.date() - start_date_next_day.date()).days

        days_list = [start_date_next_day + timedelta(days=i) for i in range(total_days + 1)]

        working_days = [d for d in days_list if d.weekday() < 5 and d.date() not in holiday_dates]

        total_working_days = len(working_days)

        return math.floor(total_working_days)

    except Exception as e:
        return None

datediff_workdays_udf = udf(datediff_workdays, IntegerType())

########################################## Inizio prima query di dettaglio

festivita = spark.table("FestivitaView")
holidays = festivita.select(F.collect_list("data").alias("holidays")).collect()[0]["holidays"]


################################## calcolo gg lavorativi
logging.info("Calcolo t_completamento_analogico (UDF)")

df1 = df1.withColumn(
    "t_completamento_analogico",
    datediff_workdays_udf(F.col("data_deposito"), F.col("end_date"))
)

################################## calcolo ritardo SLA
logging.info("Calcolo Ritardo SLA")

df1 = df1.withColumn(
    "diff_gg_SLA",
    F.col("t_completamento_analogico") - 70
)

################################# Se ho un ritardo negativo allora forzo a 0

df1 = df1.withColumn(
    "Ritardo SLA",
    F.when( F.col("diff_gg_SLA") < 0, 0).otherwise(F.col("diff_gg_SLA"))
)

######################################### Filtraggio dei dati per ottenere solo i record con ritardo_recapito non misurabili - questo ci serve per il ranking
ranking_data = df1.filter(
    (F.col("Ritardo SLA").isNotNull())
)

######################################### Ranking

window_spec = Window.orderBy( # FIX: niente finestra di partizione ma semplicemente un order by globale
    F.col("Ritardo SLA").asc(),
    F.col("end_date").asc(),
    F.col("iun").asc()
)

ranking_data = ranking_data.withColumn("ranking", F.row_number().over(window_spec))

ranking_data = ranking_data.select("iun", "ranking")

df1 = df1.join(
    ranking_data,
    on="iun",
    how="left"
)

#aggiungo la colonna total_records su tutte le righe di df1 tramite la cross join
df1 = df1.crossJoin(record_count_df)

################################## calcolo ranking
"""
logging.info("Calcolo ranking globale (window function)")

df1 = df1.withColumn(
    "ranking",
    F.row_number().over(
        Window.orderBy(
            F.col("Ritardo SLA").asc(),
            F.col("end_date").asc()
        )
    )
)

logging.info("Ranking globale calcolato")
"""
################################# percentuale oggetti ordinata

logging.info("Calcolo percentuale_oggetti_ordinata")

df1 = df1.withColumn(
    "percentuale_oggetti_ordinata",
    F.col("ranking") / F.col("total_records")
)

################################## corrispettivo_penale_analogica --- da Approfondire


logging.info("Calcolo corrispettivo_penale_analogica")

df1 = df1.withColumn(
    "flag_penale_analogica",
        F.when( (F.col("percentuale_oggetti_ordinata") < 0.97) & (F.col("Ritardo SLA") > 0), #Se rientro nel 97% e ho un ritardo allora calcolo la penale 
               1 #FIX
        ).otherwise(0) #FIX
)
    
################################## Creazione della vista temporanea 
df1.createOrReplaceTempView("analogicheDettaglio")

logging.info("Scrittura tabella dettaglio")

################################## Estrazione dettaglio - scrittura in tabella --- N.B. non va estratta ma serve solamente per le verifiche
spark.sql("""SELECT * FROM analogicheDettaglio""").writeTo("send_dev.inps_penali_analogiche_dettaglio")\
                                        .using("iceberg")\
                                        .tableProperty("format-version","2")\
                                        .tableProperty("engine.hive.enabled","true")\
                                        .createOrReplace()

################################## Aggregato

logging.info("Calcolo aggregato")


df_aggregato = (
    df1
    .groupBy(
        "quarter_fatturazione",
        "recapitista"
    )
    .agg(
        F.count("*").alias("Notifiche_Totali"),
        F.sum( F.when(F.col("Ritardo SLA") == 0, 1).otherwise(0)).alias("Notifiche_In_SLA"),
        F.sum( F.when(F.col("Ritardo SLA") > 0, 1).otherwise(0)).alias("Notifiche_Fuori_SLA"),
        F.sum( F.when( (F.col("flag_penale_analogica") == 0) & (F.col("Ritardo SLA") > 0), 1 ).otherwise(0) ).alias("Notifiche_Fuori_SLA_in_Franchigia"),
        F.sum( F.when(F.col("flag_penale_analogica") == 1, 1).otherwise(0) ).alias("Notifiche_Fuori_SLA_in_penale")
    )
    .orderBy("quarter_fatturazione")
)

#aggiunta delle colonn sulla percentuale
df_aggregato = (
    df_aggregato
    .withColumn("percentuale_conformita", F.col("Notifiche_In_SLA") / F.col("Notifiche_Totali"))
    .withColumn("percentuale_non_conformita_fuori_SLA_totali", F.col("Notifiche_Fuori_SLA") / F.col("Notifiche_Totali"))
    .withColumn("percentuale_non_conformita_fuori_SLA_in_penale", F.col("Notifiche_Fuori_SLA_in_penale") / F.col("Notifiche_Totali"))
    .orderBy("quarter_fatturazione")
)

#aggiungere riga "totale" --> campi quarter quarter_fatturazione e recapitista dovranno essere NULL, per le altre colonne conteggio tatale e percentuale totale
df_totale = (
    df_aggregato
    .agg(
        F.sum("Notifiche_Totali").alias("Notifiche_Totali"),
        F.sum("Notifiche_In_SLA").alias("Notifiche_In_SLA"),
        F.sum("Notifiche_Fuori_SLA").alias("Notifiche_Fuori_SLA"),
        F.sum("Notifiche_Fuori_SLA_in_Franchigia").alias("Notifiche_Fuori_SLA_in_Franchigia"),
        F.sum("Notifiche_Fuori_SLA_in_penale").alias("Notifiche_Fuori_SLA_in_penale")
    )
    # colonne di raggruppamento a NULL
    .withColumn("quarter_fatturazione", F.lit(None))
    .withColumn("recapitista", F.lit(None))
)

# Aggiunta delle percentuali totali -- con round a 5 cifre decimali
df_totale = (
    df_totale
    .withColumn(
        "percentuale_conformita",
        F.round(
            F.col("Notifiche_In_SLA") / F.col("Notifiche_Totali"),
            5
        )
    )
    .withColumn(
        "percentuale_non_conformita_fuori_SLA_totali",
        F.round(
            F.col("Notifiche_Fuori_SLA") / F.col("Notifiche_Totali"),
            5
        )
    )
    .withColumn(
        "percentuale_non_conformita_fuori_SLA_in_penale",
        F.round(
            F.col("Notifiche_Fuori_SLA_in_penale") / F.col("Notifiche_Totali"),
            5
        )
    )
)

# riportata nel dataframe originale
df_finale = (
    df_aggregato
    .unionByName(df_totale)
    .orderBy("quarter_fatturazione")
)

################################## Creazione della vista temporanea

logging.info("Scrittura aggregato")

df_finale.createOrReplaceTempView("analogicheAggregato")

################################## Estrazione aggregato - scrittura in tabella 
spark.sql("""SELECT * FROM analogicheAggregato""").writeTo("send_dev.inps_penali_analogiche_aggregato")\
                                                 .using("iceberg")\
                                                 .tableProperty("format-version","2")\
                                                 .tableProperty("engine.hive.enabled","true")\
                                                 .createOrReplace()

logging.info("Job completato con successo")

spark.stop()