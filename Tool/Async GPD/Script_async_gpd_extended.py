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

################## Configuration

spark = SparkSession.builder.getOrCreate()

################## CSV IN INPUT 
schema = StructType([
    StructField("codice_fiscale_ente", StringType(), True), 
    StructField("iuv", StringType(), True),
    StructField("nav", StringType(), True)
])

df_csv = spark.read.csv("SEND_tracciato_IUV.csv", header= True, sep= ";", schema = schema)

df_csv.createOrReplaceTempView("DF_CSV")

################# Prima CTE gold_notification_analytics

df = spark.table("send.gold_notification_analytics")

df1 = df.select(
    "iun", 
    "pagopaintmode", 
    "recipients_size", 
    "actual_status",
    (F.col("recipients_size") > 1).alias("multidestinatario"),
    F.posexplode("recipients").alias("recIndex", "recipient")
)
# Mostra il risultato
# df1.show(truncate=False)

df2 = df1.withColumn("payment", F.explode("recipient.payments"))

# Accesso ai campi delle strutture nidificate
df_flattened = df2.select(
    "iun",
    "multidestinatario",  
    "pagopaintmode",
    "recIndex",  
    F.col("recipient.recipientid").alias("recipientid"),
    F.col("recipient.recipienttype").alias("recipienttype"),
    F.col("payment.creditortaxid").alias("creditortaxid"),
    F.col("payment.noticecode").alias("noticecode"),
    F.col("payment.applycost").alias("applycost"),
    (F.col("actual_status") == "REFUSED").alias("refused"),
    (F.col("actual_status") == "CANCELLED").alias("cancelled")
)


################# Seconda CTE silver_timeline

silver_timeline = spark.table("send.silver_timeline")

analogCost = (
    silver_timeline
    .filter(F.col("category").isin("SEND_ANALOG_DOMICILE", "SEND_SIMPLE_REGISTERED_LETTER"))
    .select(
        "iun",
        F.get_json_object("details", "$.recIndex").alias("recindex"),
        F.get_json_object("details", "$.analogCost").cast("int").alias("analogCost")
    )
    .groupBy("iun", "recindex")
    .agg(F.sum("analogCost").alias("analogCost"))
)


################# Terza CTE silver_notification
silver_notification = spark.table("send.silver_notification")

costDetails = (
    df_flattened.alias("a")
    .join(
        silver_notification.alias("n"),
        F.col("a.iun") == F.col("n.iun"),
        how="left"
    )
    .join(
        analogCost.alias("c"),
        (F.col("a.iun") == F.col("c.iun")) &
        (F.col("a.recIndex") == F.col("c.recindex").cast("bigint")),
        how="left"
    )
    .filter(F.year("n.sentat") > 2023)
    .filter(F.col("a.noticecode").isNotNull())
    .select(
        F.col("n.senderpaid"),
        F.col("a.*"),
        F.col("n.sentat").alias("data_creazione_notifica"),
        F.col("n.notificationfeepolicy"),
        F.coalesce("n.amount", F.lit(0)).alias("notifica_costo_imputato_pa"),
        F.col("n.amount").isNotNull().alias("notifica_costo_imputato_pa_valorizzato"),
        F.coalesce("n.pafee", F.lit(0)).alias("notifica_costi_pa_eurocent"),
        F.coalesce("n.vat", F.lit(22)).alias("notifica_iva_pa"),
        F.coalesce("c.analogCost", F.lit(0)).alias("costi_spedizione_eurocent"),
        F.row_number().over(
            Window.partitionBy("a.creditortaxid", "a.noticecode").orderBy(F.col("n.sentat").desc())
        ).alias("indice")
    )
)

################ SELECT FINALE

df_final = (
    costDetails
    .filter(F.col("indice") == 1)
    .withColumn(
        "costi_notifica_cittadino_eurocent",
        F.when(~F.col("applycost"), F.lit(0))
        .when(F.col("refused"), F.lit(0))
        .when(F.col("cancelled"), F.lit(0))
        .when(F.col("notificationfeepolicy") == "FLAT_RATE", F.lit(0))
        .otherwise(
            F.round(
                F.col("notifica_costi_pa_eurocent") + 100 +
                (F.col("costi_spedizione_eurocent") * (1 + F.col("notifica_iva_pa") / 100))
            ).cast("int")
        )
    )
    .orderBy(F.col("data_creazione_notifica").desc(), F.col("iun"))
)

df_final.createOrReplaceTempView("DF_FINAL")


############### JOIN con il csv

df_filtered = spark.sql(""" 
                    SELECT s.*
                    FROM DF_FINAL s JOIN DF_CSV c ON (c.codice_fiscale_ente = s.creditorTaxId AND s.noticecode = c.nav)
					""")

############### Creazione della vista temporanea
df_filtered.createOrReplaceTempView("DF_OUTPUT")

############### Estrazione aggregato - scrittura in tabella 
spark.sql("""SELECT * FROM DF_OUTPUT""").writeTo("send_dev.temp_estrazione_async_gpd")\
                .using("iceberg")\
                .tableProperty("format-version","2")\
                .tableProperty("engine.hive.enabled","true")\
                .createOrReplace()


############### Prendere quelli che NON hanno corrispondenza (LEFT ANTI JOIN)

df_unmatched = spark.sql(""" 
                    SELECT c.codice_fiscale_ente,
                           c.iuv,
                           c.nav,
                           s.creditorTaxId,
                           s.noticecode
                    FROM DF_CSV c
                    LEFT JOIN DF_OUTPUT s ON (c.codice_fiscale_ente = s.creditorTaxId AND c.nav = s.noticecode)
                    WHERE s.noticecode IS NULL 
""")

############### Creazione della vista temporanea riepilogoMese
df_unmatched.createOrReplaceTempView("DF_UNMATCHED")

############### Estrazione aggregato - scrittura in tabella 
spark.sql("""SELECT * FROM DF_UNMATCHED""").writeTo("send_dev.temp_estrazione_async_gpd_unmatched")\
                .using("iceberg")\
                .tableProperty("format-version","2")\
                .tableProperty("engine.hive.enabled","true")\
                .createOrReplace()

spark.stop()