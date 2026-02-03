import configparser
import uuid
from pyspark.sql.functions import col, regexp_replace
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrameWriter
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F



spark = SparkSession.builder.getOrCreate()

dataInput  = spark.read.csv("20250331_Matrice_Fatturazione_Aggiornamento.csv", header=True, sep=";", inferSchema=True)

zone  = spark.read.csv("20240625_zones.csv", header=True, sep=";", inferSchema=True)

def clean_and_convert(column_name):
    return (
        regexp_replace(col(column_name).cast("string"), ",", ".")
        .cast("decimal(10,2)") * 100
    ).cast("int")




######################### INIZIO QUERY##################################

rowWithArray = dataInput.select(

    F.when(~F.col("CAP").like("ZONE%"), F.lpad(F.col("CAP"), 5, '0')).otherwise(F.col("CAP")).alias("CAP"),

    # Creazione della struttura di 'product' e 'costo_scaglioni' per 'RS'
    F.array(
        F.struct(
            F.lit("RS").alias("product"),
            F.col("RS_Recapitista").alias("recapitista"),
            F.col("RS_Lotto").alias("lotto"),
            F.col("RS_Plico").alias("costo_plico"),
            F.col("RS_Foglio").alias("costo_foglio"),
            F.col("RS_dem").alias("costo_demat"),
            F.array(
                F.struct(F.lit(1).alias("min"), F.lit(20).alias("max"), F.col("RS_20").alias("costo"),
                         F.col("RS_20").alias("costo_base_20gr")),
                F.struct(F.lit(21).alias("min"), F.lit(50).alias("max"), F.col("RS_21_50").alias("costo"),
                         F.col("RS_20").alias("costo_base_20gr")),
                F.struct(F.lit(51).alias("min"), F.lit(100).alias("max"), F.col("RS_51_100").alias("costo"),
                         F.col("RS_20").alias("costo_base_20gr")),
                F.struct(F.lit(101).alias("min"), F.lit(250).alias("max"), F.col("RS_101_250").alias("costo"),
                         F.col("RS_20").alias("costo_base_20gr")),
                F.struct(F.lit(251).alias("min"), F.lit(350).alias("max"), F.col("RS_251_350").alias("costo"),
                         F.col("RS_20").alias("costo_base_20gr")),
                F.struct(F.lit(351).alias("min"), F.lit(1000).alias("max"), F.col("RS_351_1000 ").alias("costo"),
                         F.col("RS_20").alias("costo_base_20gr")),
                F.struct(F.lit(1001).alias("min"), F.lit(2000).alias("max"), F.col("RS_1001_2000").alias("costo"),
                         F.col("RS_20").alias("costo_base_20gr"))
            ).alias("costo_scaglioni")
        ),
        # Creazione della struttura per 'AR'
        F.struct(
            F.lit("AR").alias("product"),
            F.col("AR_Recapitista").alias("recapitista"),
            F.col("AR_Lotto").alias("lotto"),
            F.col("AR_Plico").alias("costo_plico"),
            F.col("AR_Foglio").alias("costo_foglio"),
            F.col("AR_dem").alias("costo_demat"),
            F.array(
                F.struct(F.lit(1).alias("min"), F.lit(20).alias("max"), F.col("AR_20").alias("costo"),
                         F.col("AR_20").alias("costo_base_20gr")),
                F.struct(F.lit(21).alias("min"), F.lit(50).alias("max"), F.col("AR_21_50").alias("costo"),
                         F.col("AR_20").alias("costo_base_20gr")),
                F.struct(F.lit(51).alias("min"), F.lit(100).alias("max"), F.col("AR_51_100").alias("costo"),
                         F.col("AR_20").alias("costo_base_20gr")),
                F.struct(F.lit(101).alias("min"), F.lit(250).alias("max"), F.col("AR_101_250").alias("costo"),
                         F.col("AR_20").alias("costo_base_20gr")),
                F.struct(F.lit(251).alias("min"), F.lit(350).alias("max"), F.col("AR_251_350").alias("costo"),
                         F.col("AR_20").alias("costo_base_20gr")),
                F.struct(F.lit(351).alias("min"), F.lit(1000).alias("max"), F.col("AR_351_1000").alias("costo"),
                         F.col("AR_20").alias("costo_base_20gr")),
                F.struct(F.lit(1001).alias("min"), F.lit(2000).alias("max"), F.col("AR_1001_2000").alias("costo"),
                         F.col("AR_20").alias("costo_base_20gr"))
            ).alias("costo_scaglioni")
        ),
        # Creazione della struttura per '890'
        F.struct(
            F.lit("890").alias("product"),
            F.col("890_Recapitista").alias("recapitista"),
            F.col("890_Lotto").alias("lotto"),
            F.col("890_Plico").alias("costo_plico"),
            F.col("890_Foglio").alias("costo_foglio"),
            F.col("890_dem").alias("costo_demat"),
            F.array(
                F.struct(F.lit(1).alias("min"), F.lit(20).alias("max"), F.col("890_20").alias("costo"),
                         F.col("890_20").alias("costo_base_20gr")),
                F.struct(F.lit(21).alias("min"), F.lit(50).alias("max"), F.col("890_21_50").alias("costo"),
                         F.col("890_20").alias("costo_base_20gr")),
                F.struct(F.lit(51).alias("min"), F.lit(100).alias("max"), F.col("890_51_100").alias("costo"),
                         F.col("890_20").alias("costo_base_20gr")),
                F.struct(F.lit(101).alias("min"), F.lit(250).alias("max"), F.col("890_101_250").alias("costo"),
                         F.col("890_20").alias("costo_base_20gr")),
                F.struct(F.lit(251).alias("min"), F.lit(350).alias("max"), F.col("890_251_350").alias("costo"),
                         F.col("890_20").alias("costo_base_20gr")),
                F.struct(F.lit(351).alias("min"), F.lit(1000).alias("max"), F.col("890_351_1000").alias("costo"),
                         F.col("890_20").alias("costo_base_20gr")),
                F.struct(F.lit(1001).alias("min"), F.lit(2000).alias("max"), F.col("890_1001_2000").alias("costo"),
                         F.col("890_20").alias("costo_base_20gr"))
            ).alias("costo_scaglioni")
        )
    ).alias("products")
)





partialPivot = rowWithArray.select(
    "CAP",
    F.explode("products").alias("product_struct")
)




partialPivot = partialPivot.select(
    "CAP",
    F.col("product_struct.product").alias("product"),
    F.trim(F.col("product_struct.recapitista")).alias("recapitista"),
    F.col("product_struct.lotto").alias("lotto"),
    F.col("product_struct.costo_plico").alias("costo_plico"),
    F.col("product_struct.costo_foglio").alias("costo_foglio"),
    F.col("product_struct.costo_demat").alias("costo_demat"),
    F.explode("product_struct.costo_scaglioni").alias("costoScaglioni")
)



geokeyPivot = partialPivot.join(
    zone,
    partialPivot.CAP == zone.zone,
    "left"
).select(
    F.coalesce(zone.countryIt, partialPivot.CAP).alias("geokey"),
    F.when(
        (zone.countryIt.isNotNull()) & (partialPivot.product == "RS"), "RIS"
    ).when(
        (zone.countryIt.isNotNull()) & (partialPivot.product == "AR"), "RIR"
    ).when(
        (zone.countryIt.isNotNull()) & (partialPivot.product == "890"), "delete"
    ).otherwise(partialPivot.product).alias("product"),
    "recapitista",
    "lotto",
    # Conversione dei costi
    clean_and_convert("costo_plico").alias("costo_plico"),
    clean_and_convert("costo_foglio").alias("costo_foglio"),
    clean_and_convert("costo_demat").alias("costo_demat"),
    "costoScaglioni.min",
    "costoScaglioni.max",
    # Conversione del costo
    clean_and_convert("costoScaglioni.costo").alias("costo"),
    clean_and_convert("costoScaglioni.costo_base_20gr").alias("costo_base_20gr"),
    F.lit("2025-03-31T22:00:00.000Z").alias("startDate"),  # Data di inizio
    F.lit("2999-01-01T23:59:59.999Z").alias("endDate")  # Data di fine
)


finalData = geokeyPivot.filter(col("product") != "delete")

finalData.createOrReplaceTempView("reportFinale")

################################FINE QUERY##########################################

######################################### Estrazione aggregato - scrittura in tabella 
spark.sql("""SELECT * FROM reportFinale""").writeTo("send_dev.temp_matrice_costi")\
                                                 .using("iceberg")\
                                                 .tableProperty("format-version","2")\
                                                 .tableProperty("engine.hive.enabled","true")\
                                                 .createOrReplace()

spark.stop()