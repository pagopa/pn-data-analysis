import configparser
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

######################################### Configuration

spark = SparkSession.builder.getOrCreate()


df_filtrato = spark.sql("""
   SELECT  
    senderpaid, 
    iun, 
    requestid, 
    requesttimestamp, 
    prodotto, 
    geokey, 
    CASE
        WHEN recapitista = 'FSU' AND prodotto = 'AR' THEN 'FSU - AR'
        WHEN recapitista = 'FSU' AND prodotto = '890' THEN 'FSU - 890'
        WHEN recapitista = 'FSU' AND prodotto = 'RS' THEN 'FSU - RS'
        ELSE recapitista
    END AS recapitista, 
    lotto, 
    codice_oggetto, 
    prezzo_ente, 
    grammatura, 
    numero_pagine, 
    costo_consolidatore, 
    costo_recapitista, 
    affido_consolidatore_data, 
    stampa_imbustamento_con080_data, 
    affido_recapitista_con016_data, 
    CASE 
        WHEN accettazione_recapitista_con018_data IS NULL THEN affido_recapitista_con016_data + INTERVAL 1 DAY
        ELSE accettazione_recapitista_con018_data
    END AS accettazione_recapitista_con018_data,
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
    accettazione_23L_RECAG012_data_rendicontazione
FROM send.gold_postalizzazione_analytics
WHERE fine_recapito_data_rendicontazione IS NOT NULL
  AND fine_recapito_stato NOT IN ('RECRS006', 'RECRS013','RECRN006', 'RECRN013', 'RECAG004', 'RECAG013', 'RECRSI005', 'RECRI005')                               
  AND recapitista IN ('Poste', 'FSU - AR', 'FSU - 890', 'FSU - RS', 'FSU')
  AND (
      -- Calcolare il trimestre da accettazione_recapitista_con018_data se non è NULL
      (accettazione_recapitista_con018_data IS NOT NULL AND CEIL(MONTH(accettazione_recapitista_con018_data) / 3) = 1)
      -- Se con018 è NULL, calcolare il trimestre su affido_recapitista_con016_data, se non è NULL
      OR (accettazione_recapitista_con018_data IS NULL AND affido_recapitista_con016_data IS NOT NULL AND CEIL(MONTH(affido_recapitista_con016_data + INTERVAL 1 DAY) / 3) = 1)
  )
  AND (
      -- Anno da accettazione_recapitista_con018_data se non è NULL, altrimenti da affido_recapitista_con016_data
      YEAR(CASE 
          WHEN accettazione_recapitista_con018_data IS NULL THEN affido_recapitista_con016_data + INTERVAL 1 DAY
          ELSE accettazione_recapitista_con018_data
      END) = 2024
  )
      
""")



df_filtrato.createOrReplaceTempView("gold_postalizzazione")

######################################### Conteggio dei record

record_count_filtrato_df = spark.sql("""
SELECT 
    lotto, 
    prodotto, 
    COUNT(*) AS total_records
FROM 
    gold_postalizzazione
WHERE tentativo_recapito_data IS NOT NULL
GROUP BY 
    lotto, 
    prodotto;
""")

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

#festivita_df.show(10)

######################################### CSV cap_zona

schema = StructType([
    StructField("CAP", StringType(), True), 
    StructField("Zona", StringType(), True)
])

df_cap_zona = spark.read.csv("cap_zona.csv", header= True, sep= ";", schema = schema)
df_cap_zona.createOrReplaceTempView("CAP_ZONA")


######################################### Funzione custom per il calcolo dei giorni lavorativi

def datediff_workdays(start_date, end_date, holidays_list):
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

reportsla = spark.table("gold_postalizzazione")
cap_zona = spark.table("CAP_ZONA")

######################################### Aggiunta della colonna zona e calcolo di tempo_recapito usando datediff_workdays

calcolo_tempo_recapito = (
    reportsla
    .join(cap_zona, reportsla.geokey == cap_zona.CAP, "inner")
    .withColumn(
        "tempo_recapito",
        F.when(
            # Caso 1: accettazione null e affido non null
            F.col("accettazione_recapitista_con018_data").isNull() & F.col("affido_recapitista_con016_data").isNotNull(),
            datediff_workdays_udf(
                F.expr("affido_recapitista_con016_data + INTERVAL 1 DAY"),
                F.col("tentativo_recapito_data"),
                F.array([F.lit(h) for h in holidays])
            )
        ).when(
            # Caso 2: affido null e accettazione non null
            F.col("affido_recapitista_con016_data").isNull() & F.col("accettazione_recapitista_con018_data").isNotNull(),
            datediff_workdays_udf(
                F.col("accettazione_recapitista_con018_data"),
                F.col("tentativo_recapito_data"),
                F.array([F.lit(h) for h in holidays])
            )
        ).when(
            # Caso 3: stato consolidatore è CON995
            F.col("scarto_consolidatore_stato") == 'CON995',
            datediff_workdays_udf(
                F.col("accettazione_recapitista_con018_data"),
                F.col("tentativo_recapito_data"),
                F.array([F.lit(h) for h in holidays])
            )
        ).when(
            # Caso 4: accettazione prima dell'affido + 1 giorno
            F.col("accettazione_recapitista_con018_data") <= F.expr("affido_recapitista_con016_data + INTERVAL 1 DAY"),
            datediff_workdays_udf(
                F.col("accettazione_recapitista_con018_data"),
                F.col("tentativo_recapito_data"),
                F.array([F.lit(h) for h in holidays])
            )
        ).when(
            # Caso 5: accettazione dopo l'affido + 1 giorno
            F.col("accettazione_recapitista_con018_data") > F.expr("affido_recapitista_con016_data + INTERVAL 1 DAY"),
            datediff_workdays_udf(
                F.expr("affido_recapitista_con016_data + INTERVAL 1 DAY"),
                F.col("tentativo_recapito_data"),
                F.array([F.lit(h) for h in holidays])
            )
        ).when(
            # Caso 6: accettazione e affido entrambi null
            F.col("accettazione_recapitista_con018_data").isNull() & F.col("affido_recapitista_con016_data").isNull(),
            F.lit(None) 
        ).otherwise(F.lit(None))
    )
    .withColumn("tempo_recapito", F.col("tempo_recapito").cast("int"))
    .withColumn("zona", F.col("Zona"))
)

######################################### SLA STANDARD 
######################################### integrazione del lotto 30BIS

calcolo_tempo_recapito = calcolo_tempo_recapito.withColumn(
    "sla_standard",
    F.when(
        F.col("tempo_recapito").isNull(), None
    ).when(
        F.col("tempo_recapito") <= F.when( # Rilassamento per regione Puglia
            (F.col("recapitista") == 'Poste') &                           
            (F.col("senderpaid") == '135100c9-d464-4abf-a9b1-a10f5d7903b7') &
            (F.col("affido_consolidatore_data").between(F.lit('2023-12-01'), F.lit('2024-01-31'))),
            45
        )
        .when( # Rilassamento lotto 27 Fulmine nei periodi giugno - ottobre
            (F.col("lotto") == '27') &
            (F.col("accettazione_recapitista_con018_data").between(F.lit('2024-06-01'), F.lit('2024-11-01'))),
            30)
        .when(
            (F.col("lotto").isin(['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20'])) &
            (F.col("prodotto") == '890') &
            F.col("zona").isin(['AM', 'CP', 'EU']),
            7
        ).when(
            (F.col("lotto") == '99') &
            (F.col("prodotto") == '890') &
            (F.col("zona") == 'EU'),
            7
        ).when(
            (F.col("lotto") == '21') &
            (F.col("prodotto") == 'RIS') &
            (F.col("zona") == 'ZONE_1'),
            10
        ).when(
            (F.col("lotto") == '21') &
            (F.col("prodotto") == 'RIS') &
            F.col("zona").isin(['ZONE_2', 'ZONE_3']),
            20
        ).when(
            (F.col("lotto") == '26') &
            (F.col("prodotto") == 'RIR') &
            (F.col("zona") == 'ZONE_1'),
            10
        ).when(
            (F.col("lotto") == '26') &
            (F.col("prodotto") == 'RIR') &
            F.col("zona").isin(['ZONE_2', 'ZONE_3']),
            20
        ).otherwise(None),
        True
    ).when(
        F.col("tempo_recapito") <= F.when(
            (F.col("lotto").isin(['21', '22', '23', '24', '25'])) &
            (F.col("prodotto") == 'RS') &
            F.col("zona").isin(['AM', 'CP', 'EU']),
            6
        )
        .when( # Rilassamento lotto 30 aprile & agosto-settembre
            (F.col('lotto') == '30') &
            (
                    F.col('affido_recapitista_con016_data').between(F.lit('2024-04-01'), F.lit('2024-05-01')) |
                    F.col('affido_recapitista_con016_data').between(F.lit('2024-08-01'), F.lit('2024-10-01'))
            ),
            30
        )
        .when( # Rilassamento lotto 30 maggio - luglio
            (F.col("lotto") == '30') &
            (F.col("affido_recapitista_con016_data").between(F.lit('2024-05-01'), F.lit('2024-07-31'))),
        50
        )
        .when(
            (F.col("lotto").isin(['26', '27', '28', '29','30'])) &
            (F.col("prodotto") == 'AR') &
            F.col("zona").isin(['AM', 'CP', 'EU']),
            6
        ).when(
            (F.col("lotto") == '97') &
            (F.col("prodotto") == 'RS') &
            F.col("zona").isin(['AM', 'CP', 'EU']),
            6
        ).when(
            (F.col("lotto") == '98') &
            (F.col("prodotto") == 'AR') &
            (F.col("zona").isin(['AM', 'CP', 'EU'])),
            6
        ).when(
            (F.col("lotto") == '30BIS') &
            (F.col("prodotto") == 'AR') &
            (F.col("zona").isin(['AM', 'CP', 'EU'])),
            6
        ).otherwise(None),
        True
    ).otherwise(False)
)

######################################### SLA MIGLIORATIVA
######################################### Integrazione del lotto 30BIS

calcolo_tempo_recapito = calcolo_tempo_recapito.withColumn(
    "sla_migliorativa",
    F.when(F.col("tempo_recapito").isNull(), None)
    .when(
        (F.col("tempo_recapito") <= F.when( # Rilassamento per regione Puglia
            (F.col("recapitista") == 'Poste') &
            (F.col("senderpaid") == '135100c9-d464-4abf-a9b1-a10f5d7903b7') &
            (F.col("affido_consolidatore_data").between(F.lit('2023-12-01'), F.lit('2024-01-31'))), 45
        )
        .when(  # Rilassamento lotto 27 Fulmine nei periodi giugno - ottobre
            (F.col("lotto") == '27') &
            (F.col("accettazione_recapitista_con018_data").between(F.lit('2024-06-01'), F.lit('2024-11-01'))),
            30)
        .when((F.col("lotto").isin(['1', '2', '5', '7', '9']) & (F.col("prodotto") == '890') & F.col("zona").cast("string").isin(['AM', 'CP', 'EU'])), 4)
        .when(F.col("lotto").isin(['3', '4', '6', '8']) & (F.col("prodotto") == '890') & F.col("zona").cast("string").isin(['AM', 'CP']), 4)
        .when(F.col("lotto").isin(['3', '4', '6', '8']) & (F.col("prodotto") == '890') & (F.col("zona").cast("string") == 'EU'), 999)
        .when((F.col("lotto") == '10') & (F.col("prodotto") == '890') & (F.col("zona").cast("string") == 'AM'), 2)
        .when((F.col("lotto") == '10') & (F.col("prodotto") == '890') & F.col("zona").cast("string").isin(['CP', 'EU']), 4)
        .when((F.col("lotto") == '11') & (F.col("prodotto") == '890') & F.col("zona").cast("string").isin(['AM', 'CP', 'EU']), 999)
        .when((F.col("lotto") == '12') & (F.col("prodotto") == '890') & F.col("zona").cast("string").isin(['AM', 'CP']), 4)
        .when((F.col("lotto") == '12') & (F.col("prodotto") == '890') & (F.col("zona").cast("string") == 'EU'), 999)
        .when((F.col("lotto").isin(['13', '14']) & (F.col("prodotto") == '890') & F.col("zona").cast("string").isin(['AM', 'CP'])), 2)
        .when((F.col("lotto").isin(['13', '14']) & (F.col("prodotto") == '890') & (F.col("zona").cast("string") == 'EU')), 4)
        .when((F.col("lotto") == '15') & (F.col("prodotto") == '890') & F.col("zona").cast("string").isin(['AM', 'CP', 'EU']), 4)
        .when((F.col("lotto") == '16') & (F.col("prodotto") == '890') & (F.col("zona").cast("string") == 'AM'), 2)
        .when((F.col("lotto") == '16') & (F.col("prodotto") == '890') & F.col("zona").cast("string").isin(['CP', 'EU']), 4)
        .when((F.col("lotto") == '17') & (F.col("prodotto") == '890') & F.col("zona").cast("string").isin(['AM', 'CP', 'EU']), 4)
        .when((F.col("lotto") == '18') & (F.col("prodotto") == '890') & F.col("zona").cast("string").isin(['AM', 'CP']), 4)
        .when((F.col("lotto") == '18') & (F.col("prodotto") == '890') & (F.col("zona").cast("string") == 'EU'), 999)
        .when((F.col("lotto") == '19') & (F.col("prodotto") == '890') & (F.col("zona").cast("string") == 'AM'), 2)
        .when((F.col("lotto") == '19') & (F.col("prodotto") == '890') & F.col("zona").cast("string").isin(['CP', 'EU']), 4)
        .when((F.col("lotto") == '20') & (F.col("prodotto") == '890') & F.col("zona").cast("string").isin(['AM', 'CP']), 4)
        .when((F.col("lotto") == '20') & (F.col("prodotto") == '890') & (F.col("zona").cast("string") == 'EU'), 999)
        .when((F.col("lotto") == '99') & (F.col("prodotto") == '890') & F.col("zona").cast("string").isin(['AM', 'CP', 'EU']), 999)
        # Prodotti RS
        .when((F.col("lotto") == '21') & (F.col("prodotto") == 'RS') & F.col("zona").cast("string").isin(['AM', 'CP', 'EU']), 4)
        .when((F.col("lotto") == '22') & (F.col("prodotto") == 'RS') & (F.col("zona").cast("string") == 'AM'), 3)
        .when((F.col("lotto") == '22') & (F.col("prodotto") == 'RS') & F.col("zona").cast("string").isin(['CP', 'EU']), 999)
        .when((F.col("lotto") == '23') & (F.col("prodotto") == 'RS') & F.col("zona").cast("string").isin(['AM', 'CP', 'EU']), 4)
        .when((F.col("lotto") == '24') & (F.col("prodotto") == 'RS') & F.col("zona").cast("string").isin(['AM', 'CP', 'EU']), 4)
        .when((F.col("lotto") == '25') & (F.col("prodotto") == 'RS') & (F.col("zona").cast("string") == 'AM'), 5)
        .when((F.col("lotto") == '25') & (F.col("prodotto") == 'RS') & F.col("zona").cast("string").isin(['CP', 'EU']), 999)
        .when((F.col("lotto") == '97') & (F.col("prodotto") == 'RS') & F.col("zona").cast("string").isin(['AM', 'CP', 'EU']), 999)
        # Prodotti AR
        .when((F.col("lotto") == '26') & (F.col("prodotto") == 'AR') & F.col("zona").cast("string").isin(['AM', 'CP', 'EU']), 4)
        .when((F.col("lotto") == '27') & (F.col("prodotto") == 'AR') & (F.col("zona").cast("string") == 'AM'), 3)
        .when((F.col("lotto") == '27') & (F.col("prodotto") == 'AR') & F.col("zona").cast("string").isin(['CP', 'EU']), 999)
        .when((F.col("lotto") == '28') & (F.col("prodotto") == 'AR') & F.col("zona").cast("string").isin(['AM', 'CP', 'EU']), 4)
        .when((F.col("lotto") == '29') & (F.col("prodotto") == 'AR') & F.col("zona").cast("string").isin(['AM', 'CP', 'EU']), 4)
        .when((F.col("lotto") == '30BIS') & (F.col("prodotto") == 'AR') & F.col("zona").cast("string").isin(['AM', 'CP', 'EU']), 999)    #Integrazione
        .when(  # Rilassamento lotto 30 aprile & agosto-settembre
                (F.col('lotto') == '30') &
                (
                        F.col('affido_recapitista_con016_data').between(F.lit('2024-04-01'), F.lit('2024-05-01')) |
                        F.col('affido_recapitista_con016_data').between(F.lit('2024-08-01'), F.lit('2024-10-01'))
                ),
                30
            )
        .when(  # Rilassamento lotto 30 maggio-luglio
        (F.col("lotto") == '30') &
        (F.col("affido_recapitista_con016_data").between(F.lit('2024-05-01'), F.lit('2024-07-31'))),
        50
        )
        .when((F.col("lotto") == '30') & (F.col("prodotto") == 'AR') & F.col("zona").cast("string").isin(['AM', 'CP', 'EU']), 3)
        .when((F.col("lotto") == '98') & (F.col("prodotto") == 'AR') & F.col("zona").cast("string").isin(['AM', 'CP', 'EU']), 999)
        # Prodotti RIS e RIR
        .when((F.col("lotto") == '21') & (F.col("prodotto") == 'RIS') & F.col("zona").cast("string").isin(['ZONE_1', 'ZONE_2', 'ZONE_3']), 999)
        .when((F.col("lotto") == '26') & (F.col("prodotto") == 'RIR') & F.col("zona").cast("string").isin(['ZONE_1', 'ZONE_2', 'ZONE_3']), 999)
        ), True)
    .otherwise(False)
)

######################################### DEFINIZIONE GIORNI OFFERTA STANDARD

calcolo_tempo_recapito = calcolo_tempo_recapito.withColumn(
    "giorni_offerta_standard",
    F.when(  # Rilassamento per regione Puglia
            (F.col("recapitista") == 'Poste') &
            (F.col("senderpaid") == '135100c9-d464-4abf-a9b1-a10f5d7903b7') &
            (F.col("affido_consolidatore_data").between(F.lit('2023-12-01'), F.lit('2024-01-31'))), 45
        )
     .when( # Rilassamento lotto 27 nei periodi giugno - ottobre 
            (F.col("lotto") == '27') &
            (F.col("accettazione_recapitista_con018_data").between(F.lit('2024-06-01'), F.lit('2024-11-01'))),
            30)
  .when( # Rilassamento lotto 30 aprile & agosto - settembre
            (F.col('lotto') == '30') &
            (
                F.col('affido_recapitista_con016_data').between(F.lit('2024-04-01'), F.lit('2024-05-01')) |
                F.col('affido_recapitista_con016_data').between(F.lit('2024-08-01'), F.lit('2024-10-01'))
            ),
            30
        )
    .when( # Rilassamento lotto 30 aprile & maggio - luglio
            (F.col("lotto") == '30') &
            (F.col("affido_recapitista_con016_data").between(F.lit('2024-05-01'), F.lit('2024-07-31'))),
            # solo fino al 31 luglio
            50
        )
    .when(
            (F.col("lotto").isin(['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20'])) & (F.col("prodotto") == '890') & F.col("zona").isin(
            ['AM', 'CP', 'EU']), 7  
        )
    .when(
            (F.col("lotto").isin(['21', '22', '23', '24', '25'])) & (F.col("prodotto") == 'RS') & F.col("zona").isin(
                ['AM', 'CP', 'EU']), 6  
        )
    .when(
            (F.col("lotto").isin(['26', '27', '28', '29', '30', '30BIS'])) & (F.col("prodotto") == 'AR') & F.col("zona").isin(  #integrazione
                ['AM', 'CP', 'EU']), 6  
        )
    .when(
            (F.col("lotto") == '21') & (F.col("prodotto") == 'RIS') & (F.col("zona") == 'ZONE_1'), 10
        )
    .when(
            (F.col("lotto") == '21') & (F.col("prodotto") == 'RIS') & F.col("zona").isin(['ZONE_2', 'ZONE_3']), 20
        )
    .when(
            (F.col("lotto") == '26') & (F.col("prodotto") == 'RIR') & (F.col("zona") == 'ZONE_1'), 10
        )
    .when(
            (F.col("lotto") == '26') & (F.col("prodotto") == 'RIR') & F.col("zona").isin(['ZONE_2', 'ZONE_3']), 20
        )
    .when(
            (F.col("lotto") == '97') & (F.col("prodotto") == 'RS') & F.col("zona").isin(['AM', 'CP', 'EU']), 6
        )
    .when(
            (F.col("lotto") == '98') & (F.col("prodotto") == 'AR') & F.col("zona").isin(['AM', 'CP', 'EU']), 6
        )
    .when(
            (F.col("lotto") == '99') & (F.col("prodotto") == '890') & (F.col("zona") == 'EU'), 7
        )
    .otherwise(F.lit(None))
)

######################################### DEFINIZIONE GIORNI OFFERTA MIGLIORATIVA

calcolo_tempo_recapito = calcolo_tempo_recapito.withColumn(
    "giorni_offerta_migliorativa",
    F.when( # Rilassamento Regione Puglia
            (F.col("recapitista") == 'Poste') &
            (F.col("senderpaid") == '135100c9-d464-4abf-a9b1-a10f5d7903b7') &
            (F.col("affido_consolidatore_data").between(F.lit('2023-12-01'), F.lit('2024-01-31'))), 45
    )
    .when( # Rilassamento lotto 27 nei periodi giugno - ottobre
            (F.col("lotto") == '27') &
            (F.col("accettazione_recapitista_con018_data").between(F.lit('2024-06-01'), F.lit('2024-11-01'))),
            30
    )
    .when( # Rilassamento lotto 30 aprile & agosto-settembre
            (F.col('lotto') == '30') &
            (
                F.col('affido_recapitista_con016_data').between(F.lit('2024-04-01'), F.lit('2024-05-01')) |
                F.col('affido_recapitista_con016_data').between(F.lit('2024-08-01'), F.lit('2024-10-01'))
            ),
            30
    )
    .when( # Rilassamento lotto 30 maggio - luglio
            (F.col("lotto") == '30') &
            (F.col("affido_recapitista_con016_data").between(F.lit('2024-05-01'), F.lit('2024-07-31'))),
            50
    )
    .when(
        (F.col("lotto").isin(['1','2', '5', '7', '9'])) & (F.col("prodotto") == '890') & F.col("zona").isin(['AM', 'CP', 'EU']), 4
    ).when(
        F.col("lotto").isin(['3','4','6','8'])& (F.col("prodotto") == '890') & F.col("zona").isin(['AM', 'CP']), 4
    )
    .when((F.col("lotto") == '10') & (F.col("prodotto") == '890') & (F.col("zona") == 'AM'), 2)
    .when((F.col("lotto") == '10') & (F.col("prodotto") == '890') & F.col("zona").isin(['CP', 'EU']), 4)
    .when((F.col("lotto") == '12') & (F.col("prodotto") == '890') & F.col("zona").isin(['AM', 'CP']), 4)
    .when((F.col("lotto") == '13') & (F.col("prodotto") == '890') & F.col("zona").isin(['AM', 'CP']), 2)
    .when((F.col("lotto") == '13') & (F.col("prodotto") == '890') & (F.col("zona") == 'EU'), 4)
    .when((F.col("lotto") == '14') & (F.col("prodotto") == '890') & F.col("zona").isin(['AM', 'CP']), 2)
    .when((F.col("lotto") == '14') & (F.col("prodotto") == '890') & (F.col("zona") == 'EU'), 4)
    .when((F.col("lotto") == '15') & (F.col("prodotto") == '890') & F.col("zona").isin(['AM', 'CP', 'EU']), 4)
    .when((F.col("lotto") == '16') & (F.col("prodotto") == '890') & (F.col("zona") == 'AM'), 2)
    .when((F.col("lotto") == '16') & (F.col("prodotto") == '890') & F.col("zona").isin(['CP', 'EU']), 4)
    .when((F.col("lotto") == '17') & (F.col("prodotto") == '890') & F.col("zona").isin(['AM', 'CP', 'EU']), 4)
    .when((F.col("lotto") == '18') & (F.col("prodotto") == '890') & F.col("zona").isin(['AM', 'CP']), 4)
    .when((F.col("lotto") == '19') & (F.col("prodotto") == '890') & (F.col("zona") == 'AM'), 2)
    .when((F.col("lotto") == '19') & (F.col("prodotto") == '890') & F.col("zona").isin(['CP', 'EU']), 4)
    .when((F.col("lotto") == '20') & (F.col("prodotto") == '890') & F.col("zona").isin(['AM', 'CP']), 4)
    .when((F.col("lotto") == '21') & (F.col("prodotto") == 'RS') & F.col("zona").isin(['AM', 'CP', 'EU']), 4)
    .when((F.col("lotto") == '22') & (F.col("prodotto") == 'RS') & (F.col("zona") == 'AM'), 3)
    .when((F.col("lotto") == '23') & (F.col("prodotto") == 'RS') & F.col("zona").isin(['AM', 'CP', 'EU']), 4)
    .when((F.col("lotto") == '24') & (F.col("prodotto") == 'RS') & F.col("zona").isin(['AM', 'CP', 'EU']), 4)
    .when((F.col("lotto") == '25') & (F.col("prodotto") == 'RS') & (F.col("zona") == 'AM'), 5)
    .when((F.col("lotto") == '26') & (F.col("prodotto") == 'AR') & F.col("zona").isin(['AM', 'CP', 'EU']), 4)
    .when((F.col("lotto") == '27') & (F.col("prodotto") == 'AR') & (F.col("zona") == 'AM'), 3)
    .when((F.col("lotto") == '28') & (F.col("prodotto") == 'AR') & F.col("zona").isin(['AM', 'CP', 'EU']), 4)
    .when((F.col("lotto") == '29') & (F.col("prodotto") == 'AR') & F.col("zona").isin(['AM', 'CP', 'EU']), 4)
    .when((F.col("lotto") == '30') & (F.col("prodotto") == 'AR') & F.col("zona").isin(['AM', 'CP', 'EU']), 3)
    .otherwise(F.lit(None))
)


calcolo_tempo_recapito = calcolo_tempo_recapito.withColumn(
    "ritardo_recapito",
    F.when( F.col("tempo_recapito").isNull(), None)
    .otherwise(
        F.when(
                F.col("giorni_offerta_migliorativa").isNull(),
                F.when(F.col("tempo_recapito") < F.col("giorni_offerta_standard"), 0).otherwise(F.col("tempo_recapito") - F.col("giorni_offerta_standard"))
                ).otherwise(
                    F.when(F.col("tempo_recapito") < F.col("giorni_offerta_migliorativa"), 0).otherwise(F.col("tempo_recapito") - F.col("giorni_offerta_migliorativa"))
                    )
            )
    )

# Aggiungo la colonna "lotto_effettivo" con condizione per "sailpost" ----non mi convince
calcolo_tempo_recapito = calcolo_tempo_recapito.withColumn(
    "lotto",
    F.when(F.col("recapitista") == "RTI Sailpost-Snem", None).otherwise(F.col("lotto"))
)

######################################### Gesione casistiche null
calcolo_tempo_recapito = calcolo_tempo_recapito.withColumn(
    "fine_recapito_data_rendicontazione",
    F.coalesce(F.col("fine_recapito_data_rendicontazione"), F.lit("1970-01-01 00:00:00").cast("timestamp"))
)

######################################### Filtraggio dei dati per ottenere solo i record con ritardo_recapito non misurabili - questo ci serve per il ranking
ranking_data = calcolo_tempo_recapito.filter(
    (F.col("ritardo_recapito").isNotNull())
)

######################################### Ranking

window_spec = Window.partitionBy("lotto", "prodotto").orderBy(
    F.col("ritardo_recapito").asc(),
    F.col("fine_recapito_data_rendicontazione").asc(),
    F.col("requestid").asc()
)

ranking_data = ranking_data.withColumn("ranking", F.row_number().over(window_spec))

ranking_data = ranking_data.select("requestid", "ranking")

calcolo_tempo_recapito = calcolo_tempo_recapito.join(
    ranking_data,
    on="requestid",
    how="left"
)

calcolo_tempo_recapito = calcolo_tempo_recapito.join(
    record_count_filtrato_df,
    on=["lotto", "prodotto"],
    how="left"
)

######################################### Calcolo della percentuale per lotto_effettivo

calcolo_tempo_recapito = calcolo_tempo_recapito.withColumn(
    "percentuale_oggetti_ordinata",
   (F.col("ranking") /  F.col("total_records"))
)


calcolo_tempo_recapito.createOrReplaceTempView("reportSlaModificato")
#calcolo_tempo_recapito.select("tempo_recapito").show(10)

report_sla_modificato = spark.table("reportSlaModificato")

######################################### Calcolo delle penali

calcolo_penale = report_sla_modificato.withColumn(
    "corrispettivo_penale_recapito",
    F.round(
            F.when(
                (F.col("sla_standard") == True) & (F.col("sla_migliorativa") == False) & (F.col("percentuale_oggetti_ordinata") < 0.9),
                    F.when(
                        (F.col("lotto").isin(['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20'])) & (F.col("prodotto") == "890"),
                        (F.col("costo_recapitista") * 2.0) / 100.0
                    ).when(
                        (F.col("lotto").isin(['21','22','23','24','25'])) & (F.col("prodotto") == "RS"),
                        (F.col("costo_recapitista") / 100.0) * F.col("ritardo_recapito")
                    ).when(
                        (F.col("lotto").isin(['26','27','28','29','30'])) & (F.col("prodotto") == "AR"),
                        (F.col("costo_recapitista") / 100.0) * F.col("ritardo_recapito")
                    ).when(
                        (F.col("lotto") == '30BIS') & (F.col("prodotto") == "AR"),        #integrazione
                        (F.col("costo_recapitista") / 100.0) * F.col("ritardo_recapito")
                    ).when(
                        (F.col("lotto") == '21') & (F.col("prodotto") == "RIS"),
                        F.col("ritardo_recapito") * 0.01
                    ).when(
                        (F.col("lotto") == '26') & (F.col("prodotto") == "RIR"),
                        F.col("ritardo_recapito") * 0.01
                    ).when(
                        (F.col("lotto") == '97') & (F.col("prodotto") == "RS"),
                        ((F.col("costo_recapitista") /100) * 0.001) * F.col("ritardo_recapito")
                    ).when(
                        (F.col("lotto") == '98') & (F.col("prodotto") == "AR"),
                        ((F.col("costo_recapitista") /100) * 0.001) * F.col("ritardo_recapito")
                    ).when(
                        (F.col("lotto") == '99') & (F.col("prodotto") == "890"),
                        ((F.col("costo_recapitista") /100) * 0.001) * F.col("ritardo_recapito")
                    ).otherwise(0)
            ).when(
                (F.col("sla_standard") == False) & (F.col("percentuale_oggetti_ordinata") < 0.98),
                    F.when(
                        (F.col("lotto").isin(['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20'])) & (F.col("prodotto") == "890"),
                        (F.col("costo_recapitista") * 2.0) / 100.0
                    ).when(
                        (F.col("lotto").isin(['21','22','23','24','25'])) & (F.col("prodotto") == "RS"),
                        (F.col("costo_recapitista") / 100.0) * F.col("ritardo_recapito")
                    ).when(
                        (F.col("lotto").isin(['26','27','28','29','30'])) & (F.col("prodotto") == "AR"),
                        (F.col("costo_recapitista") / 100.0) * F.col("ritardo_recapito")
                    ).when(
                        (F.col("lotto") == '30BIS') & (F.col("prodotto") == "AR"),        #integrazione
                        (F.col("costo_recapitista") / 100.0) * F.col("ritardo_recapito")
                    ).when(
                        (F.col("lotto") == '21') & (F.col("prodotto") == "RIS"),
                        F.col("ritardo_recapito") * 0.01
                    ).when(
                        (F.col("lotto") == '26') & (F.col("prodotto") == "RIR"),
                        F.col("ritardo_recapito") * 0.01
                    ).when(
                        (F.col("lotto") == '97') & (F.col("prodotto") == "RS"),
                        ((F.col("costo_recapitista") /100) * 0.001) * F.col("ritardo_recapito")
                    ).when(
                        (F.col("lotto") == '98') & (F.col("prodotto") == "AR"),
                        ((F.col("costo_recapitista") /100) * 0.001) * F.col("ritardo_recapito")
                    ).when(
                        (F.col("lotto") == '99') & (F.col("prodotto") == "890"),
                        ((F.col("costo_recapitista") /100) * 0.001) * F.col("ritardo_recapito")
                    ).otherwise(0)
            )
       , 2 )
    )
    

######################################### Creazione della vista temporanea reportPenali
calcolo_penale.createOrReplaceTempView("reportPenali")

######################################### Estrazione dettaglio - scrittura in tabella
spark.sql("""SELECT * FROM reportPenali""").writeTo("send_dev.penali_recapito_dettaglio")\
                                        .using("iceberg")\
                                        .tableProperty("format-version","2")\
                                        .tableProperty("engine.hive.enabled","true")\
                                        .createOrReplace()

report_penali = spark.table("reportPenali")

######################################### Calcolo aggregato penali

calcolo_riepilogo = report_penali.groupBy(
    "recapitista",
    F.when(F.col("recapitista") == "RTI Sailpost-Snem", None).otherwise(F.col("lotto")).alias("Lotto"),
    F.when(F.col("recapitista") == "RTI Sailpost-Snem", None).otherwise(F.col("prodotto")).alias("Prodotto"),
    F.when(
        F.col("accettazione_recapitista_con018_data").isNotNull(),
        F.when(F.month("accettazione_recapitista_con018_data").between(1, 3), 1)
        .when(F.month("accettazione_recapitista_con018_data").between(4, 6), 2)
        .when(F.month("accettazione_recapitista_con018_data").between(7, 9), 3)
        .otherwise(4)
    ).otherwise(
        F.when(F.month(F.date_add(F.col("affido_recapitista_con016_data"), 1)).between(1, 3), 1)
        .when(F.month(F.date_add(F.col("affido_recapitista_con016_data"), 1)).between(4, 6), 2)
        .when(F.month(F.date_add(F.col("affido_recapitista_con016_data"), 1)).between(7, 9), 3)
        .otherwise(4)
    ).alias("trimestre"),
    F.when(
        F.col("accettazione_recapitista_con018_data").isNotNull(),
        F.year(F.to_date(F.substring(F.col("accettazione_recapitista_con018_data"), 1, 10), 'yyyy-MM-dd'))
    ).otherwise(
        F.year(F.to_date(F.substring(F.date_add(F.col("affido_recapitista_con016_data"), 1), 1, 10), 'yyyy-MM-dd')) #integrazione - aggiunto il +1 sull'anno del trimestre
    ).alias("Anno")
).agg(
    F.sum(F.when((F.col("corrispettivo_penale_recapito") > 0), 1).otherwise(0)).alias("Recapito_con_violazione_SLA"), #fix: quando trovo il corrispettivo_penale_recapito > 0 allora ho una violazione
    F.count("*").alias("Recapiti_Totali"),
    F.round(
        F.sum(F.col("corrispettivo_penale_recapito"))
        , 2
    ).alias("Penale_Recapito")
)

######################################### Creazione della vista temporanea riepilogoMese
calcolo_riepilogo.createOrReplaceTempView("riepilogoTrimestre")

######################################### Estrazione aggregato - scrittura in tabella 
spark.sql("""SELECT * FROM riepilogoTrimestre""").writeTo("send_dev.penali_recapito_aggregato")\
                                                 .using("iceberg")\
                                                 .tableProperty("format-version","2")\
                                                 .tableProperty("engine.hive.enabled","true")\
                                                 .createOrReplace()

spark.stop()