from typing import Dict
from pyspark.sql.functions import to_date, col
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from pyspark.sql.functions import col, when

######################################################## Configurazione

spark = SparkSession.builder.getOrCreate()

######################################################## Creazione di un DataFrame con le festività
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

def datediff_workdays(start_date, end_date):
    try:

        if start_date is None or end_date is None:
            return None

        if end_date < start_date:
            return 0

        total_days = (end_date - start_date).days

        days_list = [start_date + timedelta(days=i) for i in range(1, total_days + 1)]

        working_days = [d for d in days_list if d.weekday() < 5 and d not in holiday_dates]

        total_working_days = 0

        if working_days:
            total_working_days += len(working_days)

        return total_working_days

    except Exception as e:
        return None

datediff_workdays_udf = udf(datediff_workdays, IntegerType())



def is_working_day(date):
    return date.weekday() < 5 and date not in holiday_dates

def check_consecutive_holidays(date):
    if date in holiday_dates:
        next_day = date + timedelta(days=1)
        if next_day in holiday_dates:
            return next_day
    return None

def calcola_data_lavorativa(data):
    if data is None:
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

calcola_data_lavorativa_udf = F.udf(calcola_data_lavorativa, DateType())


######################################################## Import da tabella sorgente

df_supporto_con_somma = spark.sql( """   
                           SELECT 	*, 
                            		SUM(numero_pagine) OVER(PARTITION BY data_slittamento) AS somma_pagine_totali  
                           FROM send_dev.silver_sla_stampa_imbustamento"""   
                        ) 

df_supporto_con_somma.createOrReplaceTempView("DF_SOMMA")

df_supporto_con_moda = spark.sql("""
                            WITH V AS (
                                SELECT 
                                    data_slittamento, 
                                    data_stampa_imbustamento, 
                                    COUNT(*) AS count_occorrenze
                                FROM send_dev.silver_sla_stampa_imbustamento
                                GROUP BY data_slittamento, data_stampa_imbustamento
                            ),
                            Ranked AS (
                                SELECT *,
                                       ROW_NUMBER() OVER (PARTITION BY data_slittamento ORDER BY count_occorrenze DESC) AS rn
                                FROM V
                            )
                            SELECT data_slittamento, data_stampa_imbustamento, count_occorrenze
                            FROM Ranked
                            WHERE rn = 1;""")

df_supporto_con_moda.createOrReplaceTempView("DF_MODA")

df_final = spark.sql("""
                        SELECT DISTINCT 
                               m.data_slittamento,
                               m.data_stampa_imbustamento,
                               s.somma_pagine_totali
                        FROM DF_MODA m LEFT JOIN DF_SOMMA s ON (m.data_slittamento = s.data_slittamento)
                    """)

######################################################## Calcolo dei ritardi - J+2

df_final = df_final.withColumn(
    "ritardo_stampa_imbustamento",
    datediff_workdays_udf(col("data_slittamento"), col("data_stampa_imbustamento"))
)

df_final = df_final.withColumn(
    "ritardo_stampa_imbustamento",
    col("ritardo_stampa_imbustamento") - 2
)

######################################################## Condizione per restituire NULL se il risultato è <= 0
df_final = df_final.withColumn(
    "ritardo_stampa_imbustamento",
    when(col("ritardo_stampa_imbustamento") <= 0, None).otherwise(col("ritardo_stampa_imbustamento"))
)



######################################################## Calcolo delle Penali

df_final = df_final.withColumn(
    "penale_stampa_imbustamento",
    F.when(col("ritardo_stampa_imbustamento") <= 3, col("ritardo_stampa_imbustamento") * 300)
    .when(col("ritardo_stampa_imbustamento") > 3, F.lit(900) + (col("ritardo_stampa_imbustamento") - 3) * 500)
    .otherwise(None)
)

df_output = df_final.withColumn("mese_affido", F.month(col("data_slittamento")))
df_output = df_output.withColumn("anno_affido", F.year(col("data_slittamento")))

df_output = df_output.withColumn(
    "trimestre_affido",
    (F.floor((F.month(col("data_slittamento")) - 1) / 3) + 1)
)

######################################################## Estrazione db di supporto, scrittura in tabella

df_output.createOrReplaceTempView("DF_OUTPUT")

spark.sql("""SELECT * FROM DF_OUTPUT""").writeTo("send_dev.gold_sla_stampa_imbustamento")\
                .using("iceberg")\
                .tableProperty("format-version","2")\
                .tableProperty("engine.hive.enabled","true")\
                .createOrReplace()

spark.stop()