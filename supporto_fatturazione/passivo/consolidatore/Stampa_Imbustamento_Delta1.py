from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, IntegerType, DateType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from pyspark.sql.functions import col, when

spark = SparkSession.builder.getOrCreate()

######################################################## Configurazione

df_filtered_gold = spark.sql( """   
                                   SELECT requestID, 
                                          numero_pagine, 
                                          CASE 
                                                WHEN affido_consolidatore_data IS NULL THEN requesttimestamp 
                                                ELSE affido_consolidatore_data 
                                          END affido_consolidatore_data, 
                                          stampa_imbustamento_CON080_data
                                   FROM send.gold_postalizzazione_analytics
                                   WHERE stampa_imbustamento_CON080_data IS NOT NULL
                                   AND requestdate < "2025-02-09"
                             """   
                                )

######################################################## impostare i parametri corretti per le run precedenti, to-do: parametrizzare da interfaccia

df_filtered_gold.createOrReplaceTempView("DF_GOLD")

######################################################## Creazione dataframe per le festività
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


df_supporto = spark.sql( """   
                            SELECT *
                            FROM send_dev.silver_sla_stampa_imbustamento """   
                        ) 

df_supporto.createOrReplaceTempView("DF_SUPPORTO")

df_filtered_new = spark.sql(""" SELECT *
                                FROM DF_GOLD
                                WHERE requestID NOT IN (SELECT requestid FROM DF_SUPPORTO) """)

######################################################## Cast a date

df_filtered = df_filtered_new.withColumn("affido_consolidatore_data_cast", F.to_date("affido_consolidatore_data"))
df_filtered = df_filtered.withColumn("stampa_imbustamento_con080_data_cast", F.to_date("stampa_imbustamento_con080_data"))

######################################################## CUT OFF

df_filtered = df_filtered.withColumn(
    "affido_consolidatore_data_cut_off",
    F.when(
        F.col("affido_consolidatore_data").cast("timestamp").substr(12, 8) <= "23:59", 
        F.col("affido_consolidatore_data_cast")
    ).otherwise(F.col("affido_consolidatore_data_cast") + F.expr("INTERVAL 1 DAY"))
)

######################################################## SLITTAMENTO DATE EFFETTIVE

df_filtered = df_filtered.withColumn(
    "affido_consolidatore_data_effettiva",
    calcola_data_lavorativa_udf(F.col("affido_consolidatore_data_cut_off"))
)

######################################################## INIZIALIZZAZIONE COLONNE CICLO WHILE

df_filtered = df_filtered.withColumn("data_slittamento", F.col("affido_consolidatore_data_effettiva"))
df_filtered = df_filtered.withColumn("flag", F.lit("KO"))
df_filtered = df_filtered.withColumn("somma_cumulativa_fogli", F.lit(0))


######################################################## IMPLEMENTAZIONE LOGICA A DELTA

df_max = spark.sql( """   
                       SELECT 
                              MAX(ranking) AS max_ranking,
                              MAX(data_slittamento) AS max_data_slittamento
                       FROM send_dev.silver_sla_stampa_imbustamento """   
                    ) 

row = df_max.collect()[0]

data_max = row["max_data_slittamento"] 
ranking_max = row["max_ranking"]
limite_pagine = 180000


if data_max is None:
    print("La tabella è vuota...")

    df_filtered = df_filtered.orderBy("data_slittamento", "stampa_imbustamento_con080_data_cast","requestid")

    window_spec = Window.orderBy("data_slittamento", "stampa_imbustamento_con080_data_cast", "requestid")

    df_filtered_with_ranking = df_filtered.withColumn("ranking", F.rank().over(window_spec))

    df_ko_min = df_filtered_with_ranking.filter(col("flag") == "KO").agg(F.min("ranking").alias("ranking_primo_ko")).first()

    schema = StructType([
        StructField("requestid", IntegerType(), True),
        StructField("ranking", IntegerType(), True),
        StructField("data_slittamento", DateType(), True),
        StructField("data_stampa_imbustamento", DateType(), True),
        StructField("numero_pagine", IntegerType(), True)
    ])

    df_supporto = spark.createDataFrame([], schema)

else:
    print("La tabella è valorizzata...")
    

    df_filtered = df_filtered.withColumn(
            "data_slittamento",
            when(col("data_slittamento") <= data_max,
                 data_max)
            .otherwise(col("data_slittamento"))
        )


    df_filtered = df_filtered.orderBy("data_slittamento", "stampa_imbustamento_con080_data_cast","requestid")

    ############################################### Verificare la Capienza dell'ultimo giorno che ho a disposizione

    df_somma_data_max = spark.sql( """   
                                    SELECT somma_pagine_totali
                                    FROM send_dev.gold_sla_stampa_imbustamento
                                    WHERE data_slittamento = (SELECT MAX(data_slittamento) FROM send_dev.gold_sla_stampa_imbustamento) """   
                                    ) 

    row = df_somma_data_max.collect()[0]

    somma_data_max = row["somma_pagine_totali"]

    window_spec = Window.orderBy("data_slittamento", "stampa_imbustamento_con080_data_cast", "requestid")

    df_filtered_with_ranking = df_filtered.withColumn("ranking", F.rank().over(window_spec) + ranking_max )

    df_ko_min = df_filtered_with_ranking.filter(col("flag") == "KO").agg(F.min("ranking").alias("ranking_primo_ko")).first()

    ############################################### PASSAGGIO PER CAPIENZA DATA MAX

    if limite_pagine - somma_data_max > 0:
        # STEP 1: Partizionamento e somma cumulativa
        window_spec = Window.partitionBy("data_slittamento").orderBy("ranking")
        df_filtered_with_ranking = df_filtered_with_ranking.withColumn("somma_cumulativa", F.sum("numero_pagine").over(window_spec))

        # STEP 2: Se la somma_cumulativa supera il limite di pagine, flag KO, altrimenti OK
        df_filtered_with_ranking = df_filtered_with_ranking.withColumn(
            "flag",
            when(col("somma_cumulativa") > limite_pagine - somma_data_max, "KO").otherwise("OK")
        )

        # STEP 3: Memorizzare il primo KO (ranking_primo_ko)
        df_ko_min = df_filtered_with_ranking.filter(col("flag") == "KO").agg(F.min("ranking").alias("ranking_primo_ko")).first()

        if df_ko_min and df_ko_min["ranking_primo_ko"] is not None:
            rk_primo_ko = df_ko_min["ranking_primo_ko"]

            # STEP 4: Salvataggio nel df_supporto tutti i record prima del primo KO
            df_da_salvare = df_filtered_with_ranking.filter(col("ranking") < rk_primo_ko) \
                .select("requestid","ranking", "data_slittamento","stampa_imbustamento_con080_data_cast", "numero_pagine")
            df_supporto = df_supporto.union(df_da_salvare)

            # STEP 5: Aggiornare max_id e max_data_slittamento, con le relative date associate
            ranking_max = rk_primo_ko - 1

            data_max = df_filtered_with_ranking.filter(col("ranking") == ranking_max) \
                .select("data_slittamento") \
                .first()

            if data_max:
                data_max = data_max["data_slittamento"]


######################################################## CICLO WHILE

while not df_filtered_with_ranking.rdd.isEmpty():

    # STEP 0: Filtraggio iniziale se ranking_max è valorizzato
    if ranking_max is not None:
        df_filtered_with_ranking = df_filtered_with_ranking.filter(col("ranking") > ranking_max)

    # STEP 0.5: Impostare data_slittamento se affido_consolidatore_data_effettiva == data_max
    if data_max is not None:
        df_filtered_with_ranking = df_filtered_with_ranking.withColumn(
            "data_slittamento",
            when(col("data_slittamento") == data_max,
                 calcola_data_lavorativa(data_max + timedelta(days=1)))
            .otherwise(col("data_slittamento"))
        )

    # STEP 1: Partizionamento e somma cumulativa
    window_spec = Window.partitionBy("data_slittamento").orderBy("ranking")
    df_filtered_with_ranking = df_filtered_with_ranking.withColumn("somma_cumulativa", F.sum("numero_pagine").over(window_spec))

    # STEP 2: Se la somma_cumulativa supera il limite di pagine, flag KO, altrimenti OK
    df_filtered_with_ranking = df_filtered_with_ranking.withColumn(
        "flag",
        when(col("somma_cumulativa") > limite_pagine, "KO").otherwise("OK")
    )

    # STEP 3: Memorizzare il primo KO (ranking_primo_ko)
    df_ko_min = df_filtered_with_ranking.filter(col("flag") == "KO").agg(F.min("ranking").alias("ranking_primo_ko")).first()

    if df_ko_min and df_ko_min["ranking_primo_ko"] is not None:  # Controllo che df_ko_min non sia vuoto
        rk_primo_ko = df_ko_min["ranking_primo_ko"]

        # STEP 4: Salvataggio nel df_supporto tutti i record prima del primo KO
        df_da_salvare = df_filtered_with_ranking.filter(col("ranking") < rk_primo_ko) \
            .select("requestid","ranking", "data_slittamento","stampa_imbustamento_con080_data_cast", "numero_pagine")
        df_supporto = df_supporto.union(df_da_salvare) ###unisco i risultati appena generati

        # STEP 5: Aggiornare max_id e max_data_slittamento, con le relative date associate
        ranking_max = rk_primo_ko - 1

        data_max = df_filtered_with_ranking.filter(col("ranking") == ranking_max) \
            .select("data_slittamento") \
            .first()

        if data_max:
            data_max = data_max["data_slittamento"]
        else:
            data_max = None  

    else: # Caso in cui non si trova un "KO": viene aggiunto tutto in df_supporto
        print("Nessun KO trovato. Aggiungo tutti i dati restanti al supporto.")
        df_supporto = df_supporto.union(df_filtered_with_ranking.select("requestid", "ranking","data_slittamento", "stampa_imbustamento_con080_data_cast", "numero_pagine"))

        ranking_max = None
        data_max = None
        break


######################################################## Estrazione db di supporto, scrittura in tabella

df_supporto.createOrReplaceTempView("DF_SUPPORTO")

spark.sql("""SELECT * FROM DF_SUPPORTO""").writeTo("send_dev.silver_sla_stampa_imbustamento")\
            .using("iceberg")\
            .tableProperty("format-version","2")\
            .tableProperty("engine.hive.enabled","true")\
            .createOrReplace()

spark.stop()