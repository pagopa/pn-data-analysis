import configparser
import uuid
import os
from typing import Dict
from pyspark.sql.functions import to_date, col
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrameWriter
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta, date
from pyspark.sql.functions import col, ceil, when

spark = SparkSession.builder.getOrCreate()


######################################################## Configurazione iniziale


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



def datediff_workdays(start_date, end_date):
    try:
        if start_date is None or end_date is None:
            return None

        if end_date < start_date:
            return 0

        start_date_next_day = start_date.replace(hour=0, minute=0, second=0) + timedelta(days=1)

        total_days = (end_date - start_date_next_day).days

        days_list = [start_date_next_day + timedelta(days=i) for i in range(total_days + 1)]

        working_days = [d for d in days_list if d.weekday() < 5 and d.date() not in holiday_dates]

        total_working_days = len(working_days)

        return total_working_days

    except Exception as e:
        return None

datediff_workdays_udf = udf(datediff_workdays, IntegerType())

######################################################## Inizio Query

df_start = spark.sql("""
    SELECT 
        gpa.senderpaid,
        gpa.requestID, 
        gpa.requesttimestamp, 
        gpa.prodotto,
        gpa.geokey,
        CASE 
            WHEN gpa.affido_consolidatore_data IS NULL 
            THEN gpa.requesttimestamp 
            ELSE gpa.affido_consolidatore_data 
        END AS affido_consolidatore_data,
        gpa.stampa_imbustamento_con080_data,
        gpa.materialita_pronta_CON09A_data,
        gpa.affido_recapitista_con016_data,
        gpa.accettazione_recapitista_con018_data
    FROM 
        send.gold_postalizzazione_analytics AS gpa
    JOIN 
        send.gold_notification_analytics AS gna
        ON gpa.iun = gna.iun
    WHERE 
        gpa.accettazione_recapitista_con018_data IS NULL
        AND gpa.scarto_consolidatore_stato IS NULL
        AND gpa.pcretry_rank = 1
        AND gpa.tentativo_recapito_stato IS NULL 
        AND gpa.messaingiacenza_recapito_stato IS NULL 
        AND gpa.certificazione_recapito_stato IS NULL 
        AND gpa.fine_recapito_stato IS NULL 
        AND gpa.demat_23l_ar_stato IS NULL
        AND gpa.demat_plico_stato IS NULL
        AND gpa.iun NOT IN (
            SELECT iun 
            FROM send.silver_incident_iun
        )
        AND gna.actual_status <> 'CANCELLED' AND gpa.ultimo_evento_stato NOT IN ('P008','P010')
""")


df_start = df_start.withColumn("Cluster",
                                F.when(F.col("stampa_imbustamento_con080_data").isNull() & 
                                           (datediff_workdays_udf(F.col("affido_consolidatore_data"),F.current_timestamp()) >= 2), "Stampa Imbustamento") 
                                 .when(F.col("materialita_pronta_CON09A_data").isNull() & 
                                       (datediff_workdays_udf(F.col("stampa_imbustamento_con080_data"),F.current_timestamp()) >= 2), "Materialita Pronta")
                                 .when(F.col("affido_recapitista_con016_data").isNull() &
                                       (datediff_workdays_udf(F.col("stampa_imbustamento_con080_data"),F.current_timestamp()) >= 2), "Pick Up")
                                 .when(F.col("accettazione_recapitista_con018_data").isNull() & 
                                       (datediff_workdays_udf(F.col("affido_recapitista_con016_data"), F.current_timestamp()) >= 1), "Accettazione Recapitista"))
                                 
df_start = df_start.filter(F.col("Cluster").isNotNull())

df_start = df_start.withColumn("Priority", 
                                F.when(F.col("Cluster") == "Stampa Imbustamento", 4)
                                 .when(F.col("Cluster") == "Materialita Pronta", 3)
                                 .when(F.col("Cluster") == "Pick Up", 2)
                                 .when(F.col("Cluster") == "Accettazione Recapitista", 1))
								 
								 
windowSpec = Window.partitionBy("requestID").orderBy("Priority")

df_start_with_rank = df_start.withColumn("rank", F.row_number().over(windowSpec))


df_filtered = df_start_with_rank.filter(F.col("rank") == 1).drop("rank")

# Aggiunta della colonna per calcolare la differenza tra gg lavorativi start date - end date
df_filtered = df_filtered.withColumn(
    "differenza_gg_lavorativi",
    F.when(
        F.col("Cluster") == "Stampa Imbustamento",
        datediff_workdays_udf(F.col("affido_consolidatore_data"), F.current_timestamp())
    ).when(
        F.col("Cluster") == "Materialita Pronta",
        datediff_workdays_udf(F.col("stampa_imbustamento_con080_data"), F.current_timestamp())
    ).when(
        F.col("Cluster") == "Pick Up",
        datediff_workdays_udf(F.col("stampa_imbustamento_con080_data"), F.current_timestamp())
    ).when(
        F.col("Cluster") == "Accettazione Recapitista",
        datediff_workdays_udf(F.col("affido_recapitista_con016_data"), F.current_timestamp())
    )
)

# Aggiunta della colonna per il calcolo dei giorni effettivi fuori SLA
df_filtered = df_filtered.withColumn(
    "gg_fuori_sla",
    F.when(
        F.col("Cluster") == "Stampa Imbustamento",
        datediff_workdays_udf(F.col("affido_consolidatore_data"), F.current_timestamp()) - 2 
    ).when(
        F.col("Cluster") == "Materialita Pronta",
        datediff_workdays_udf(F.col("stampa_imbustamento_con080_data"), F.current_timestamp()) - 2 
    ).when(
        F.col("Cluster") == "Pick Up",
        datediff_workdays_udf(F.col("stampa_imbustamento_con080_data"), F.current_timestamp()) - 2
    ).when(
        F.col("Cluster") == "Accettazione Recapitista",
        datediff_workdays_udf(F.col("affido_recapitista_con016_data"), F.current_timestamp()) - 1 
    )
)

########################################################Fine Query

df_filtered.createOrReplaceTempView("DF_OUTPUT")


spark.sql("""SELECT * FROM DF_OUTPUT""").writeTo("send_dev.wi7_consolidatore")\
                .using("iceberg")\
                .tableProperty("format-version","2")\
                .tableProperty("engine.hive.enabled","true")\
                .createOrReplace()
#print(datetime.now()-start)

spark.stop()
                                 
