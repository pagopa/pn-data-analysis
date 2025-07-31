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

#fix: codice oggetto corretto per i recapitisti
df_filtrato = spark.sql("""
   SELECT  
    g.iun,
    g.requestid,
    g.requesttimestamp,
    g.prodotto,
    g.senderpaid,
    g.geokey,
    c.area,
    c.provincia,
    c.regione,
 CASE
    WHEN COALESCE(i.recapitista_corretto, g.recapitista) = 'FSU' AND g.prodotto = 'AR' THEN 'FSU - AR'
    WHEN COALESCE(i.recapitista_corretto, g.recapitista) = 'FSU' AND g.prodotto = '890' THEN 'FSU - 890'
    WHEN COALESCE(i.recapitista_corretto, g.recapitista) = 'FSU' AND g.prodotto = 'RS' THEN 'FSU - RS'
    ELSE COALESCE(i.recapitista_corretto, g.recapitista)
 END AS recapitista,
    COALESCE(i.lotto_corretto, g.lotto) AS lotto,
    g.codice_oggetto,
    g.costo_recapitista,
    affido_consolidatore_data,
    stampa_imbustamento_con080_data,
    affido_recapitista_con016_data,
    CASE 
        WHEN accettazione_recapitista_CON018_data IS NULL THEN affido_recapitista_CON016_data + INTERVAL 1 DAY
        ELSE accettazione_recapitista_CON018_data
    END AS accettazione_recapitista_CON018_data,
    affido_conservato_con020_data,
    materialita_pronta_con09a_data,
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
    accettazione_23l_recag012_data,
    accettazione_23l_recag012_data_rendicontazione,
    rend_23l_stato,
    rend_23l_data,
    rend_23l_data_rendicontazione,
    causa_forza_maggiore_dettagli,
    causa_forza_maggiore_data,
    causa_forza_maggiore_data_rendicontazione,
    demat_23l_ar_data_rendicontazione,
    demat_plico_data_rendicontazione
FROM send.gold_postalizzazione_analytics g 
LEFT JOIN send_dev.temp_incident i ON (g.requestid = i.requestid)
LEFT JOIN send_dev.cap_area_provincia_regione c ON (c.cap = g.geokey)
WHERE fine_recapito_data_rendicontazione IS NOT NULL 
  AND fine_recapito_stato NOT IN ('RECRS006', 'RECRS013','RECRN006', 'RECRN013', 'RECAG004', 'RECAG013')
  AND COALESCE(i.recapitista_corretto, g.recapitista) IN ('RTI Fulmine - Sol. Camp.', 'RTI Fulmine - Forgilu') 
  --- Impostare il numero del trimestre
  AND CEIL(MONTH(fine_recapito_data_rendicontazione) / 3) = 3 
  --- Impostare l'anno
  AND YEAR(fine_recapito_data_rendicontazione) = 2024
  AND  g.requestid NOT IN (
          SELECT requestid_computed
          FROM send.silver_postalizzazione_denormalized
          WHERE statusrequest IN ('PN999', 'PN998')
      )
""")
#fix PN999 e PN998

df_filtrato.createOrReplaceTempView("gold_postalizzazione")

######################################### Conteggio dei record

record_count_filtrato_df = spark.sql("""
SELECT 
    lotto, 
    prodotto, 
    COUNT(*) AS total_records
FROM 
    gold_postalizzazione
WHERE tentativo_recapito_data IS NOT NULL AND (accettazione_recapitista_con018_data IS NOT NULL OR affido_recapitista_con016_data IS NOT NULL)
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

######################################### CSV cap_zona -- fix: ci importiamo direttamenet la tabella di cap_area_provincia_regione
"""
schema = StructType([
    StructField("CAP", StringType(), True), 
    StructField("Zona", StringType(), True)
])

df_cap_zona = spark.read.csv("cap_zona.csv", header= True, sep= ";", schema = schema)
df_cap_zona.createOrReplaceTempView("CAP_ZONA")
"""

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
#cap_zona = spark.table("CAP_ZONA")

######################################### Aggiunta della colonna zona e calcolo di tempo_recapito usando datediff_workdays

calcolo_tempo_recapito = (
    reportsla
    #.join(cap_zona, reportsla.geokey == cap_zona.CAP, "inner")
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
    .withColumn("zona", F.col("area"))
    #.withColumn("zona", F.col("Zona"))
)

######################################### SLA STANDARD 

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
        .when( # Rilassamento lotto 22,27 Fulmine nei periodi giugno - ottobre-dicembre
            (F.col("lotto").isin(22,27)) &
            (F.col("accettazione_recapitista_con018_data").between(F.lit('2024-06-01'), F.lit('2024-11-01')) |
             (F.col("accettazione_recapitista_con018_data").between(F.lit('2024-12-01'), F.lit('2025-01-01')))),
            30)
        .when( # Rilassamento lotto 22,27 Fulmine Campania - giugno 25
            (F.col("lotto").isin(22,27)) & (F.col("regione").isin('Campania')) &
            (F.col("accettazione_recapitista_con018_data").between(F.lit('2025-06-01'), F.lit('2025-07-01'))), #vedere casi limite 1 luglio e 30 giugno se è incluso
            45)
        .when( # Rilassamento lotto 22,27 Fulmine Campania - luglio 25
            (F.col("lotto").isin(22,27)) & (F.col("regione").isin('Campania')) &
            (F.col("accettazione_recapitista_con018_data").between(F.lit('2025-07-01'), F.lit('2025-07-08'))), #vedere casi limite 7 luglio se è incluso
           30)
        .when(
            (F.col("lotto").isin([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20])) &
            (F.col("prodotto") == '890') &
            F.col("Zona").isin(['AM', 'CP', 'EU']),
            7
        ).when(
            (F.col("lotto") == 99) &
            (F.col("prodotto") == '890') &
            (F.col("Zona") == 'EU'),
            7
        ).when(
            (F.col("lotto") == 21) &
            (F.col("prodotto") == 'RIS') &
            (F.col("Zona") == 'ZONE_1'),
            10
        ).when(
            (F.col("lotto") == 21) &
            (F.col("prodotto") == 'RIS') &
            F.col("Zona").isin(['ZONE_2', 'ZONE_3']),
            20
        ).when(
            (F.col("lotto") == 26) &
            (F.col("prodotto") == 'RIR') &
            (F.col("Zona") == 'ZONE_1'),
            10
        ).when(
            (F.col("lotto") == 26) &
            (F.col("prodotto") == 'RIR') &
            F.col("Zona").isin(['ZONE_2', 'ZONE_3']),
            20
        ).otherwise(None),
        True
    ).when(
        F.col("tempo_recapito") <= F.when(
            (F.col("lotto").isin([21, 22, 23, 24, 25])) &
            (F.col("prodotto") == 'RS') &
            F.col("Zona").isin(['AM', 'CP', 'EU']),
            6
        )
        .when( # Rilassamento lotto 30 aprile & agosto-settembre
            (F.col('lotto') == 30) &
            (
                    F.col('affido_recapitista_con016_data').between(F.lit('2024-04-01'), F.lit('2024-05-01')) |
                    F.col('affido_recapitista_con016_data').between(F.lit('2024-08-01'), F.lit('2024-10-01'))
            ),
            30
        )
        .when( # Rilassamento lotto 30 maggio - luglio
            (F.col("lotto") == 30) &
            (F.col("affido_recapitista_con016_data").between(F.lit('2024-05-01'), F.lit('2024-07-31'))),
        50
        )
        .when(
            (F.col("lotto").isin([26, 27, 28, 29, 30])) &
            (F.col("prodotto") == 'AR') &
            F.col("Zona").isin(['AM', 'CP', 'EU']),
            6
        ).when(
            (F.col("lotto") == 97) &
            (F.col("prodotto") == 'RS') &
            F.col("Zona").isin(['AM', 'CP', 'EU']),
            6
        ).when(
            (F.col("lotto") == 98) &
            (F.col("prodotto") == 'AR') &
            (F.col("Zona").isin(['AM', 'CP', 'EU'])),
            6
        ).otherwise(None),
        True
    ).otherwise(False)
)

######################################### SLA MIGLIORATIVA

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
            (F.col("lotto").isin(22, 27)) &
            (F.col("accettazione_recapitista_con018_data").between(F.lit('2024-06-01'), F.lit('2024-11-01'))
             &
             (F.col("accettazione_recapitista_con018_data").between(F.lit('2024-12-01'), F.lit('2025-01-01')))),
            30)
        .when( # Rilassamento lotto 22,27 Fulmine Campania - giugno 25
            (F.col("lotto").isin(22,27)) & (F.col("regione").isin('Campania')) &
            (F.col("accettazione_recapitista_con018_data").between(F.lit('2025-06-01'), F.lit('2025-07-01'))), #vedere casi limite 1 luglio e 30 giugno se è incluso
            45)
        .when( # Rilassamento lotto 22,27 Fulmine Campania - luglio 25
            (F.col("lotto").isin(22,27)) & (F.col("regione").isin('Campania')) &
            (F.col("accettazione_recapitista_con018_data").between(F.lit('2025-07-01'), F.lit('2025-07-08'))), #vedere casi limite 7 luglio se è incluso
           30)
        .when(
            (F.col("lotto").isin([1, 2, 5, 7, 9]) & (F.col("prodotto") == '890') & F.col("Zona").cast("string").isin(['AM', 'CP', 'EU'])), 4)
            .when(F.col("lotto").isin([3, 4, 6, 8]) & (F.col("prodotto") == '890') & F.col("Zona").cast("string").isin(['AM', 'CP']), 4)
            .when(F.col("lotto").isin([3, 4, 6, 8]) & (F.col("prodotto") == '890') & (F.col("Zona").cast("string") == 'EU'), 999)
            .when((F.col("lotto") == 10) & (F.col("prodotto") == '890') & (F.col("Zona").cast("string") == 'AM'), 2)
            .when((F.col("lotto") == 10) & (F.col("prodotto") == '890') & F.col("Zona").cast("string").isin(['CP', 'EU']), 4)
            .when((F.col("lotto") == 11) & (F.col("prodotto") == '890') & F.col("Zona").cast("string").isin(['AM', 'CP', 'EU']), 999)
            .when((F.col("lotto") == 12) & (F.col("prodotto") == '890') & F.col("Zona").cast("string").isin(['AM', 'CP']), 4)
            .when((F.col("lotto") == 12) & (F.col("prodotto") == '890') & (F.col("Zona").cast("string") == 'EU'), 999)
            .when((F.col("lotto").isin([13, 14]) & (F.col("prodotto") == '890') & F.col("Zona").cast("string").isin(['AM', 'CP'])), 2)
            .when((F.col("lotto").isin([13, 14]) & (F.col("prodotto") == '890') & (F.col("Zona").cast("string") == 'EU')), 4)
            .when((F.col("lotto") == 15) & (F.col("prodotto") == '890') & F.col("Zona").cast("string").isin(['AM', 'CP', 'EU']), 4)
            .when((F.col("lotto") == 16) & (F.col("prodotto") == '890') & (F.col("Zona").cast("string") == 'AM'), 2)
            .when((F.col("lotto") == 16) & (F.col("prodotto") == '890') & F.col("Zona").cast("string").isin(['CP', 'EU']), 4)
            .when((F.col("lotto") == 17) & (F.col("prodotto") == '890') & F.col("Zona").cast("string").isin(['AM', 'CP', 'EU']), 4)
            .when((F.col("lotto") == 18) & (F.col("prodotto") == '890') & F.col("Zona").cast("string").isin(['AM', 'CP']), 4)
            .when((F.col("lotto") == 18) & (F.col("prodotto") == '890') & (F.col("Zona").cast("string") == 'EU'), 999)
            .when((F.col("lotto") == 19) & (F.col("prodotto") == '890') & (F.col("Zona").cast("string") == 'AM'), 2)
            .when((F.col("lotto") == 19) & (F.col("prodotto") == '890') & F.col("Zona").cast("string").isin(['CP', 'EU']), 4)
            .when((F.col("lotto") == 20) & (F.col("prodotto") == '890') & F.col("Zona").cast("string").isin(['AM', 'CP']), 4)
            .when((F.col("lotto") == 20) & (F.col("prodotto") == '890') & (F.col("Zona").cast("string") == 'EU'), 999)
            .when((F.col("lotto") == 99) & (F.col("prodotto") == '890') & F.col("Zona").cast("string").isin(['AM', 'CP', 'EU']), 999)
            # Prodotti RS
            .when((F.col("lotto") == 21) & (F.col("prodotto") == 'RS') & F.col("Zona").cast("string").isin(['AM', 'CP', 'EU']), 4)
            .when((F.col("lotto") == 22) & (F.col("prodotto") == 'RS') & (F.col("Zona").cast("string") == 'AM'), 3)
            .when((F.col("lotto") == 22) & (F.col("prodotto") == 'RS') & F.col("Zona").cast("string").isin(['CP', 'EU']), 999)
            .when((F.col("lotto") == 23) & (F.col("prodotto") == 'RS') & F.col("Zona").cast("string").isin(['AM', 'CP', 'EU']), 4)
            .when((F.col("lotto") == 24) & (F.col("prodotto") == 'RS') & F.col("Zona").cast("string").isin(['AM', 'CP', 'EU']), 4)
            .when((F.col("lotto") == 25) & (F.col("prodotto") == 'RS') & (F.col("Zona").cast("string") == 'AM'), 5)
            .when((F.col("lotto") == 25) & (F.col("prodotto") == 'RS') & F.col("Zona").cast("string").isin(['CP', 'EU']), 999)
            .when((F.col("lotto") == 97) & (F.col("prodotto") == 'RS') & F.col("Zona").cast("string").isin(['AM', 'CP', 'EU']), 999)
            # Prodotti AR
            .when((F.col("lotto") == 26) & (F.col("prodotto") == 'AR') & F.col("Zona").cast("string").isin(['AM', 'CP', 'EU']), 4)
            .when((F.col("lotto") == 27) & (F.col("prodotto") == 'AR') & (F.col("Zona").cast("string") == 'AM'), 3)
            .when((F.col("lotto") == 27) & (F.col("prodotto") == 'AR') & F.col("Zona").cast("string").isin(['CP', 'EU']), 999)
            .when((F.col("lotto") == 28) & (F.col("prodotto") == 'AR') & F.col("Zona").cast("string").isin(['AM', 'CP', 'EU']), 4)
            .when((F.col("lotto") == 29) & (F.col("prodotto") == 'AR') & F.col("Zona").cast("string").isin(['AM', 'CP', 'EU']), 4)
         .when(  # Rilassamento lotto 30 aprile & agosto-settembre
            (F.col('lotto') == 30) &
            (
                    F.col('affido_recapitista_con016_data').between(F.lit('2024-04-01'), F.lit('2024-05-01')) |
                    F.col('affido_recapitista_con016_data').between(F.lit('2024-08-01'), F.lit('2024-10-01'))
            ),
            30
         )
         .when(  # Rilassamento lotto 30 maggio-luglio
            (F.col("lotto") == 30) &
            (F.col("affido_recapitista_con016_data").between(F.lit('2024-05-01'), F.lit('2024-07-31'))),
            50
         )
        .when((F.col("lotto") == 30) & (F.col("prodotto") == 'AR') & F.col("Zona").cast("string").isin(['AM', 'CP', 'EU']), 3)
        .when((F.col("lotto") == 98) & (F.col("prodotto") == 'AR') & F.col("Zona").cast("string").isin(['AM', 'CP', 'EU']), 999)
        # Prodotti RIS e RIR
        .when((F.col("lotto") == 21) & (F.col("prodotto") == 'RIS') & F.col("Zona").cast("string").isin(['ZONE_1', 'ZONE_2', 'ZONE_3']), 999)
        .when((F.col("lotto") == 26) & (F.col("prodotto") == 'RIR') & F.col("Zona").cast("string").isin(['ZONE_1', 'ZONE_2', 'ZONE_3']), 999)
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
     .when( # Rilassamento lotto 27 nei periodi giugno - ottobre-dicembre
            (F.col("lotto").isin(22,27)) &
            (F.col("accettazione_recapitista_con018_data").between(F.lit('2024-06-01'), F.lit('2024-11-01')) |
             (F.col("accettazione_recapitista_con018_data").between(F.lit('2024-12-01'), F.lit('2025-01-01')))),
            30)
      .when( # Rilassamento lotto 22,27 Fulmine Campania - giugno 25
            (F.col("lotto").isin(22,27)) & (F.col("regione").isin('Campania')) &
            (F.col("accettazione_recapitista_con018_data").between(F.lit('2025-06-01'), F.lit('2025-07-01'))), #vedere casi limite 1 luglio e 30 giugno se è incluso
            45)
      .when( # Rilassamento lotto 22,27 Fulmine Campania - luglio 25
            (F.col("lotto").isin(22,27)) & (F.col("regione").isin('Campania')) &
            (F.col("accettazione_recapitista_con018_data").between(F.lit('2025-07-01'), F.lit('2025-07-08'))), #vedere casi limite 7 luglio se è incluso
           30)
  .when( # Rilassamento lotto 30 aprile & agosto - settembre
            (F.col('lotto') == 30) &
            (
                F.col('affido_recapitista_con016_data').between(F.lit('2024-04-01'), F.lit('2024-05-01')) |
                F.col('affido_recapitista_con016_data').between(F.lit('2024-08-01'), F.lit('2024-10-01'))
            ),
            30
        )
    .when( # Rilassamento lotto 30 aprile & maggio - luglio
            (F.col("lotto") == 30) &
            (F.col("affido_recapitista_con016_data").between(F.lit('2024-05-01'), F.lit('2024-07-31'))),
            # solo fino al 31 luglio
            50
        )
    .when(
            (F.col("lotto").isin([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20])) & (F.col("prodotto") == '890') & F.col("Zona").isin(
            ['AM', 'CP', 'EU']), 7  
        )
    .when(
            (F.col("lotto").isin([21, 22, 23, 24, 25])) & (F.col("prodotto") == 'RS') & F.col("Zona").isin(
                ['AM', 'CP', 'EU']), 6  
        )
    .when(
            (F.col("lotto").isin([26, 27, 28, 29, 30])) & (F.col("prodotto") == 'AR') & F.col("Zona").isin(
                ['AM', 'CP', 'EU']), 6  
        )
    .when(
            (F.col("lotto") == 21) & (F.col("prodotto") == 'RIS') & (F.col("Zona") == 'ZONE_1'), 10
        )
    .when(
            (F.col("lotto") == 21) & (F.col("prodotto") == 'RIS') & F.col("Zona").isin(['ZONE_2', 'ZONE_3']), 20
        )
    .when(
            (F.col("lotto") == 26) & (F.col("prodotto") == 'RIR') & (F.col("Zona") == 'ZONE_1'), 10
        )
    .when(
            (F.col("lotto") == 26) & (F.col("prodotto") == 'RIR') & F.col("Zona").isin(['ZONE_2', 'ZONE_3']), 20
        )
    .when(
            (F.col("lotto") == 97) & (F.col("prodotto") == 'RS') & F.col("Zona").isin(['AM', 'CP', 'EU']), 6
        )
    .when(
            (F.col("lotto") == 98) & (F.col("prodotto") == 'AR') & F.col("Zona").isin(['AM', 'CP', 'EU']), 6
        )
    .when(
            (F.col("lotto") == 99) & (F.col("prodotto") == '890') & (F.col("Zona") == 'EU'), 7
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
     .when( # Rilassamento lotto 22, 27 nei periodi giugno - ottobre-dicembre
            (F.col("lotto").isin(22,27)) &
            (F.col("accettazione_recapitista_con018_data").between(F.lit('2024-06-01'), F.lit('2024-11-01'))  |
             (F.col("accettazione_recapitista_con018_data").between(F.lit('2024-12-01'), F.lit('2025-01-01'))))
        ,
            30
     )
     .when( # Rilassamento lotto 22,27 Fulmine Campania - giugno 25
            (F.col("lotto").isin(22,27)) & (F.col("regione").isin('Campania')) &
            (F.col("accettazione_recapitista_con018_data").between(F.lit('2025-06-01'), F.lit('2025-07-01'))), #vedere casi limite 1 luglio e 30 giugno se è incluso
            45)
        .when( # Rilassamento lotto 22,27 Fulmine Campania - luglio 25
            (F.col("lotto").isin(22,27)) & (F.col("regione").isin('Campania')) &
            (F.col("accettazione_recapitista_con018_data").between(F.lit('2025-07-01'), F.lit('2025-07-08'))), #vedere casi limite 7 luglio se è incluso
           30)
 .when( # Rilassamento lotto 30 aprile & agosto-settembre
            (F.col('lotto') == 30) &
            (
                F.col('affido_recapitista_con016_data').between(F.lit('2024-04-01'), F.lit('2024-05-01')) |
                F.col('affido_recapitista_con016_data').between(F.lit('2024-08-01'), F.lit('2024-10-01'))
            ),
            30
    )
    .when( # Rilassamento lotto 30 maggio - luglio
            (F.col("lotto") == 30) &
            (F.col("affido_recapitista_con016_data").between(F.lit('2024-05-01'), F.lit('2024-07-31'))),
            50
    )
    .when(
        (F.col("lotto").isin([1, 2, 5, 7, 9])) & (F.col("prodotto") == '890') & F.col("Zona").isin(['AM', 'CP', 'EU']), 4
    ).when(
        F.col("lotto").isin([3, 4, 6, 8])& (F.col("prodotto") == '890') & F.col("Zona").isin(['AM', 'CP']), 4
    )
    .when((F.col("lotto") == 10) & (F.col("prodotto") == '890') & (F.col("Zona") == 'AM'), 2)
    .when((F.col("lotto") == 10) & (F.col("prodotto") == '890') & F.col("Zona").isin(['CP', 'EU']), 4)
    .when((F.col("lotto") == 12) & (F.col("prodotto") == '890') & F.col("Zona").isin(['AM', 'CP']), 4)
    .when((F.col("lotto") == 13) & (F.col("prodotto") == '890') & F.col("Zona").isin(['AM', 'CP']), 2)
    .when((F.col("lotto") == 13) & (F.col("prodotto") == '890') & (F.col("Zona") == 'EU'), 4)
    .when((F.col("lotto") == 14) & (F.col("prodotto") == '890') & F.col("Zona").isin(['AM', 'CP']), 2)
    .when((F.col("lotto") == 14) & (F.col("prodotto") == '890') & (F.col("Zona") == 'EU'), 4)
    .when((F.col("lotto") == 15) & (F.col("prodotto") == '890') & F.col("Zona").isin(['AM', 'CP', 'EU']), 4)
    .when((F.col("lotto") == 16) & (F.col("prodotto") == '890') & (F.col("Zona") == 'AM'), 2)
    .when((F.col("lotto") == 16) & (F.col("prodotto") == '890') & F.col("Zona").isin(['CP', 'EU']), 4)
    .when((F.col("lotto") == 17) & (F.col("prodotto") == '890') & F.col("Zona").isin(['AM', 'CP', 'EU']), 4)
    .when((F.col("lotto") == 18) & (F.col("prodotto") == '890') & F.col("Zona").isin(['AM', 'CP']), 4)
    .when((F.col("lotto") == 19) & (F.col("prodotto") == '890') & (F.col("Zona") == 'AM'), 2)
    .when((F.col("lotto") == 19) & (F.col("prodotto") == '890') & F.col("Zona").isin(['CP', 'EU']), 4)
    .when((F.col("lotto") == 20) & (F.col("prodotto") == '890') & F.col("Zona").isin(['AM', 'CP']), 4)
    .when((F.col("lotto") == 21) & (F.col("prodotto") == 'RS') & F.col("Zona").isin(['AM', 'CP', 'EU']), 4)
    .when((F.col("lotto") == 22) & (F.col("prodotto") == 'RS') & (F.col("Zona") == 'AM'), 3)
    .when((F.col("lotto") == 23) & (F.col("prodotto") == 'RS') & F.col("Zona").isin(['AM', 'CP', 'EU']), 4)
    .when((F.col("lotto") == 24) & (F.col("prodotto") == 'RS') & F.col("Zona").isin(['AM', 'CP', 'EU']), 4)
    .when((F.col("lotto") == 25) & (F.col("prodotto") == 'RS') & (F.col("Zona") == 'AM'), 5)
    .when((F.col("lotto") == 26) & (F.col("prodotto") == 'AR') & F.col("Zona").isin(['AM', 'CP', 'EU']), 4)
    .when((F.col("lotto") == 27) & (F.col("prodotto") == 'AR') & (F.col("Zona") == 'AM'), 3)
    .when((F.col("lotto") == 28) & (F.col("prodotto") == 'AR') & F.col("Zona").isin(['AM', 'CP', 'EU']), 4)
    .when((F.col("lotto") == 29) & (F.col("prodotto") == 'AR') & F.col("Zona").isin(['AM', 'CP', 'EU']), 4)
    .when((F.col("lotto") == 30) & (F.col("prodotto") == 'AR') & F.col("Zona").isin(['AM', 'CP', 'EU']), 3)
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


######################################### Gestione casistiche null
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

######################################### Calcolo delle penali ---- parte modificata

######################################### Corrispettivo penale proporzionale in base ai giorni di ritardo 
calcolo_penale = report_sla_modificato.withColumn(
    "corrispettivo_penale_proporzionale",
    F.round(
            F.when(
                (F.col("sla_standard") == True) & (F.col("sla_migliorativa") == False) & (F.col("percentuale_oggetti_ordinata") < 0.9), # soglia del 90% perchè sto nello sla standard
                    # qui dobbiamo calcolare il corrispettivo penale  proporzionale basato sui giorni di ritardo
                    # se ritardo compreso tra 1 e 60 gg --> penale = (costo recapitista/100)* (gg ritardo / 30 --> arrotondati a 2 decimali)
                    # altrimenti se ritardo > 60gg --> penale = (costo recapitista/100)*2 --> applico il tetto massimo
                    
                    F.when(
                        F.col("ritardo_recapito") < 60,
                        F.round((F.col("costo_recapitista") / 100) * (F.col("ritardo_recapito") / 30), 2)
                    ).when(
                        F.col("ritardo_recapito") >= 60,
                        F.round((F.col("costo_recapitista") / 100) * 2, 2)
                    ).otherwise(0) #quando rientro in questa casistica? 
                        
            ).when(
                (F.col("sla_standard") == False) & (F.col("percentuale_oggetti_ordinata") < 0.98),
                    # qui dobbiamo calcolare il corrispettivo penale  proporzionale basato sui giorni di ritardo
                    # se ritardo compreso tra 1 e 60 gg --> penale = (costo recapitista/100)* (gg ritardo / 30 --> arrotondati a 2 decimali)
                    # altrimenti se ritardo > 60gg --> penale = (costo recapitista/100)*2 --> applico il tetto massimo 
                    
                   F.when(
                        F.col("ritardo_recapito") < 60,
                        F.round((F.col("costo_recapitista") / 100) * (F.col("ritardo_recapito") / 30), 2)
                    ).when(
                        F.col("ritardo_recapito") >= 60,
                        F.round((F.col("costo_recapitista") / 100) * 2, 2)
                    ).otherwise(0) #quando rientro in questa casistica? 
            ).otherwise(0)
       , 2 )
    )
    
######################################### Corrispettivo penale pesato per l'integrazione con la penale di rendicontazione 
calcolo_penale = calcolo_penale.withColumn(
    "corrispettivo_penale_pesato",
    # Tutto quello che ha la colonna corrispettivo_penale_proporzionale valorizzato --> moltiplicalo * 0.5
    F.round(
        F.when(
            (F.col("corrispettivo_penale_proporzionale").isNotNull()) &
            (F.col("corrispettivo_penale_proporzionale") > 0),
            F.col("corrispettivo_penale_proporzionale") * 0.5
        ).otherwise(0),
        2
    )
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
        F.col("fine_recapito_data_rendicontazione").isNotNull(),
        F.when(F.month("fine_recapito_data_rendicontazione").between(1, 3), 1)
        .when(F.month("fine_recapito_data_rendicontazione").between(4, 6), 2)
        .when(F.month("fine_recapito_data_rendicontazione").between(7, 9), 3)
        .otherwise(4)
    ).alias("trimestre"),
    F.when(
        F.col("fine_recapito_data_rendicontazione").isNotNull(),
        F.year(F.to_date(F.substring(F.col("fine_recapito_data_rendicontazione"), 1, 10), 'yyyy-MM-dd'))
    ).alias("Anno")
).agg(
    F.sum(F.when((F.col("corrispettivo_penale_pesato") > 0), 1).otherwise(0)).alias("Recapito_con_violazione_SLA"), #fix: quando trovo il corrispettivo_penale_recapito > 0 allora ho una violazione
    F.count("*").alias("Recapiti_Totali"),
    F.round(
        F.sum(F.col("corrispettivo_penale_pesato")) # fix: qui sommo direttamente il corrispettivo pesato
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