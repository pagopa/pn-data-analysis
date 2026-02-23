import configparser
import math
import uuid
#import boto3
import os
from typing import Dict
from pyspark.sql.functions import lit, to_date, month, years, col
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrameWriter
from pyspark.sql.dataframe import DataFrame

from pyspark.sql.functions import udf
import base64
from pyspark.sql.types import IntegerType, BooleanType, DateType, StringType, DecimalType
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import calendar
from pyspark.sql.types import FloatType
from datetime import datetime, timedelta, date
from pyspark.sql import functions as sparkf
from pyspark.sql.functions import col, ceil, when, floor
from pyspark.sql.functions import col, regexp_replace

# Load the configuration file
config = configparser.ConfigParser()
config.read('./config/app_config_MatriceCosti.ini')

# Extract configurations from the ini file
spark_config_dict: Dict[str, str] = config['spark']
input_config_dict: Dict[str, str] = config['input']
output_config_dict: Dict[str, str] = config['output']

SPARK_APP_NAME = spark_config_dict.get('name', 'spark-app')

# Set authorization through SSO profile
# session = boto3.Session(profile_name='tunnel')
#credentials = session.get_credentials()

# Spark session configuration
os.environ['SPARK_MEM'] = spark_config_dict.get('memory', '24g')

spark_conf = SparkConf()
spark_conf.set('spark.driver.memory', spark_config_dict.get('driver.memory', '4g'))
spark_conf.set('spark.executor.memory', spark_config_dict.get('executor.memory', '5g'))
spark_conf.set('spark.executor.cores', spark_config_dict.get('executor.cores', '3'))
spark_conf.set('spark.executor.instances', spark_config_dict.get('executor.instances', '4'))
spark_conf.set('spark.dynamicAllocation.enabled', spark_config_dict.get('dynamicAllocation.enabled', 'false'))

"""
spark_conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
spark_conf.set('spark.hadoop.fs.s3a.path.style.access', 'true')
spark_conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4')
spark_conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
spark_conf.set("spark.hadoop.fs.s3a.secret.key", credentials.secret_key)
spark_conf.set("spark.hadoop.fs.s3a.access.key", credentials.access_key)
spark_conf.set("spark.hadoop.fs.s3a.session.token", credentials.token)"""

# Configure and start new Spark Session
spark_session = (SparkSession.builder
                .appName(name=SPARK_APP_NAME)
                .master(master=spark_config_dict.get('master', 'local'))
                .config(conf=spark_conf)
                .getOrCreate())

spark_session.sparkContext.setLogLevel(spark_config_dict.get('logLevel', 'WARN'))
unique_uuid: str = str(uuid.uuid4())

zone: DataFrame = spark_session.read.csv(input_config_dict.get('source_local'), header=True, inferSchema=True, sep=';')

dataInput: DataFrame = spark_session.read.csv(input_config_dict.get('source_local2'), header=True, inferSchema=True, sep=';')

from pyspark.sql import functions as F

rowWithArray = dataInput.select(
    # Modifica di 'CAP' con logica simile a 'when'
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

# Esplodiamo 'products' in un array di strutture
partialPivot = rowWithArray.select(
    "CAP",
    F.explode("products").alias("product_struct")  # Esplodere 'products' in 'product_struct'
)

partialPivot.printSchema()

# 2. Selezioniamo i singoli campi e esplodiamo 'costo_scaglioni'
partialPivot = partialPivot.select(
    "CAP",
    F.col("product_struct.product").alias("product"),  # Seleziona 'product' usando la notazione a punto
    F.trim(F.col("product_struct.recapitista")).alias("recapitista"),
    F.col("product_struct.lotto").alias("lotto"),
    F.col("product_struct.costo_plico").alias("costo_plico"),
    F.col("product_struct.costo_foglio").alias("costo_foglio"),
    F.col("product_struct.costo_demat").alias("costo_demat"),
    F.explode("product_struct.costo_scaglioni").alias("costoScaglioni")  # Esplodiamo 'costo_scaglioni'
)

def clean_and_convert(column_name):
    return (
        regexp_replace(col(column_name).cast("string"), ",", ".")
        .cast("decimal(10,2)") * 100
    ).cast("int")

zone.printSchema()

geokeyPivot = partialPivot.join(
    zone,
    partialPivot.CAP == zone.zone,
    "left"
).select(
    F.coalesce(zone.countryIt, partialPivot.CAP).alias("geokey"),  # Seleziona 'geokey' con coalesce
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
    F.lit("2025-07-27T22:00:00.000Z").alias("startDate"),  # Data di inizio, da modificare volta per volta
    F.lit("2999-01-01T23:59:59.999Z").alias("endDate")  # Data di fine
)


finalData = geokeyPivot.filter(col("product") != "delete")

output_path: str = os.path.join(output_config_dict.get('path'), unique_uuid, "report_matrice") + '.csv'
dataframe_writer: DataFrameWriter = finalData.repartition(1).write.mode(saveMode='overwrite')
dataframe_writer.options(header='True', delimiter=';').csv(output_path)

spark_session.stop()