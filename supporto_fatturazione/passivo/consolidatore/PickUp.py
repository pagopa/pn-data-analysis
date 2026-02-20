
import configparser
import uuid
import os
from typing import Dict
from pyspark.sql.functions import lit, to_date, month, years, col
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrameWriter
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, BooleanType, DateType
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import calendar
from pyspark.sql.types import FloatType
from datetime import datetime, timedelta, date
from pyspark.sql import functions as sparkf
from pyspark.sql.functions import col, ceil, when, floor


######################################### Configuration

spark = SparkSession.builder.getOrCreate()


unique_uuid: str = str(uuid.uuid4())

df = spark.sql("""
   SELECT * FROM send.gold_postalizzazione_analytics     
""")

df.createOrReplaceTempView("gold_postalizzazione")


######################################### Creazione del DataFrame con le festività

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

        total_days = (end_date - start_date).days

        days_list = [start_date + timedelta(days=i) for i in range(1, total_days + 1)]

        working_days = [d for d in days_list if d.weekday() < 5 and d.strftime('%Y-%m-%d') not in holiday_dates]

        total_working_days = 0

        if working_days:

            total_working_days += len(working_days)

        return total_working_days

    except Exception as e:
        return None

datediff_workdays_udf = udf(datediff_workdays, IntegerType())

#########################################  Funzione per calcolare la data slittata
def calcola_data_lavorativa(data):
    if data is None:
        return None

    def is_working_day(date):
        return date.weekday() < 5 and date not in holiday_dates  

    def check_consecutive_holidays(date):
        if date in holiday_dates:
            next_day = date + timedelta(days=1)
            if next_day in holiday_dates:
                return next_day 
        return None

    while not is_working_day(data):
        next_valid_day = check_consecutive_holidays(data)
        if next_valid_day:
            data = next_valid_day
        else:
            if data.weekday() == 5:
                data += timedelta(days=2)
            elif data.weekday() == 6:
                data += timedelta(days=1)
            elif data in holiday_dates:
                data += timedelta(days=1)

    return data

calcola_data_lavorativa_udf = F.udf(
    lambda date: calcola_data_lavorativa(date), DateType()
)

festivita = spark.table("FestivitaView")
holidays = festivita.select(F.collect_list("data").alias("holidays")).collect()[0]["holidays"]

df = spark.table("gold_postalizzazione")

df_filtered = df.withColumn("affido_consolidatore_data_cast", F.to_date("affido_consolidatore_data"))

df_filtered = df_filtered.withColumn("stampa_imbustamento_con080_data_cast", F.to_date("stampa_imbustamento_con080_data"))
df_filtered = df_filtered.withColumn("materialita_pronta_con09a_data_cast", F.to_date("materialita_pronta_con09a_data"))

######################################### Logica di slittamento sulla data iniziale di pickup
df_filtered = df_filtered.withColumn(
    "stampa_imbustamento_con080_data_effettiva",
    calcola_data_lavorativa_udf(F.col("stampa_imbustamento_con080_data_cast"))
)

######################################### Filtraggio degli scarti
df_filtered = df_filtered.filter(F.col("scarto_consolidatore_stato").isNull())


######################################### Inizio calcolo penale di PickUp

window_spec_pickup = Window.partitionBy("stampa_imbustamento_con080_data_effettiva").orderBy(
    F.coalesce("materialita_pronta_con09a_data_cast", F.lit("31/12/9999")), 
    "requestID"
)

df_grouped_pickup = df_filtered.groupBy("stampa_imbustamento_con080_data_effettiva", "materialita_pronta_con09a_data_cast").agg(F.count("*").alias("count_occorrenze_pickup"))

window_spec_pickup = Window.partitionBy("stampa_imbustamento_con080_data_effettiva").orderBy(F.desc("count_occorrenze_pickup"), F.asc_nulls_last("materialita_pronta_con09a_data_cast"))

######################################### Ranking

df_grouped_2 = df_grouped_pickup.withColumn("rank", F.rank().over(window_spec_pickup))

df_grouped_2 = df_grouped_2.withColumn("rank2", F.dense_rank().over(window_spec_pickup))

df_most_frequent_pickup = df_grouped_2.filter(F.col("rank") == 1).drop("rank")

df_rank_2_pickup = df_grouped_2.filter(F.col("rank2") == 2) \
                               .select( "stampa_imbustamento_con080_data_effettiva", "materialita_pronta_con09a_data_cast" ) \
                               .withColumnRenamed("materialita_pronta_con09a_data_cast", "affido_recapitista_rank_2")

df_result_pickup = df_most_frequent_pickup.join(df_rank_2_pickup, on="stampa_imbustamento_con080_data_effettiva", how="left")

df_result_pickup = df_result_pickup.withColumnRenamed("materialita_pronta_con09a_data_cast", "affido_recapitista_rank_1")

######################################### Creazione della colonna finale di affido recapitista - calcolata a partire dalla moda

df_result_pickup = df_result_pickup.withColumn(
    "affido_recapitista_moda",
    F.when(F.col("affido_recapitista_rank_1").isNull(), F.col("affido_recapitista_rank_2"))
     .otherwise(F.col("affido_recapitista_rank_1"))
)


######################################### Calcolo SLA PickUp

df_final_pickup = df_result_pickup.withColumn(
        "Ritardo Pick Up",
        ceil(                                      
            datediff_workdays_udf(
                col("stampa_imbustamento_con080_data_effettiva"), #start date
                col("affido_recapitista_moda")                    #end date
            )
        ).cast(IntegerType())
)


df_final_pickup = df_final_pickup.withColumn(
                    "Ritardo Pick Up",
                    col("Ritardo Pick Up") - 2
                )

######################################### Restituire NULL se il risultato è <= 0
df_final_pickup = df_final_pickup.withColumn(
                    "Ritardo Pick Up",
                    when(col("Ritardo Pick Up") <= 0, None).otherwise(col("Ritardo Pick Up"))
                )


######################################### Calcolo penale di PickUp
df_output_pickup = df_final_pickup.withColumn(
    "Penale_Pick_Up",
    F.when(col("Ritardo Pick Up") > 0, col("Ritardo Pick Up") * 150).otherwise(None)
)

df_output_pickup = df_output_pickup.withColumn("mese_stampa", F.month(col("stampa_imbustamento_con080_data_effettiva")))
df_output_pickup = df_output_pickup.withColumn("anno_stampa", F.year(col("stampa_imbustamento_con080_data_effettiva")))

df_output_pickup = df_output_pickup.withColumn(
    "trimestre_stampa",
    (F.floor((F.month(col("stampa_imbustamento_con080_data_effettiva")) - 1) / 3) + 1)
)

df_riepilogo_pickup = df_output_pickup.select(
    "stampa_imbustamento_con080_data_effettiva",
    "affido_recapitista_moda", 
    "Ritardo Pick Up",
    "Penale_Pick_Up",
    "mese_stampa",
    "anno_stampa",
    "trimestre_stampa"
)

df_riepilogo_pickup.createOrReplaceTempView("riepilogo_pickup")

######################################################## Estrazione aggregato, scrittura in tabella
spark.sql("""SELECT * FROM riepilogo_pickup""").writeTo("send_dev.penali_consolidatore_pickup")\
                                        .using("iceberg")\
                                        .tableProperty("format-version","2")\
                                        .tableProperty("engine.hive.enabled","true")\
                                        .createOrReplace()

spark.stop()