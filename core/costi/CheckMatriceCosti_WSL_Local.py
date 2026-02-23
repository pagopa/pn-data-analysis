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
from pyspark.sql.functions import row_number
from pyspark.sql.functions import coalesce
from functools import reduce
from pyspark.sql.functions import udf
import base64
from pyspark.sql.types import IntegerType, BooleanType, DateType, StringType, DecimalType, StructType, StructField
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import calendar
from pyspark.sql.types import FloatType
from datetime import datetime, timedelta, date
from pyspark.sql import functions as sparkf
from pyspark.sql.functions import col, ceil, when, floor
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.functions import sha2, concat_ws, col
from pyspark.sql.functions import concat, col
from pyspark.sql.functions import lpad, col
from pyspark.sql.functions import round


# Load the configuration file
config = configparser.ConfigParser()
config.read('./config/app_config_MatriceCosti.ini')

# Extract configurations from the ini file
spark_config_dict: Dict[str, str] = config['spark']
input_config_dict: Dict[str, str] = config['input']
output_config_dict: Dict[str, str] = config['output']

SPARK_APP_NAME = spark_config_dict.get('name', 'spark-app')

# Set authorization through SSO profile
#session = boto3.Session(profile_name='tunnel')
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

######################################### CSV zone

#versione con inferschema - attualmente non funzionante
#zone: DataFrame = spark_session.read.csv(input_config_dict.get('source_local'), header=True, inferSchema=True, sep=';')

#versione senza inferschema
schema_zone = StructType([
    StructField("zone", StringType(), True), 
    StructField("countryIt", StringType(), True),
    StructField("countryEn", StringType(), True)
])

zone: DataFrame = spark_session.read.csv(input_config_dict.get('source_local'), header=True, schema = schema_zone, sep=';')

zone = zone.withColumnRenamed("countryIt","geokey")

zone.printSchema()

######################################### CSV Test Massivi 
#versione con inferschema
#testMassivi: DataFrame = spark_session.read.csv(input_config_dict.get('source_local_testMassivi'), header=True, inferSchema=True, sep=';')

#versione senza inferschema
schema_testMassivi = StructType([
    StructField("CAP", StringType(), True),
    StructField("PRODOTTO", StringType(), True),
    StructField("ID GARA", StringType(), True),
    StructField("PESO", StringType(), True),
    StructField("NUMERO PAGINE", StringType(), True),
    StructField("NUMERO FACCIATE", StringType(), True),
    StructField("FRONTE/RETRO", StringType(), True),
    StructField("Costo pagine", StringType(), True),
    StructField("Costo lavorazione plico", StringType(), True),
    StructField("Costo Demat", StringType(), True),
    StructField("Lotto", StringType(), True),
    StructField("Zona", StringType(), True),
    StructField("Concatena", StringType(), True),
    StructField("Peso_id", StringType(), True),
    StructField("COSTO NOTIFICA PESO", StringType(), True),
    StructField("COSTO", StringType(), True),
    StructField("COSTO + EURO DIGITALE", StringType(), True),
    StructField("COSTO EUROCENTS", StringType(), True)
])

testMassivi: DataFrame = spark_session.read.csv(input_config_dict.get('source_local_testMassivi'), header=True, schema = schema_testMassivi, sep=';')

testMassivi.printSchema()

testMassivi = testMassivi.withColumn(
    "COSTO NOTIFICA PESO",
    regexp_replace(col("COSTO NOTIFICA PESO"), ",", ".").cast("double")
)

testMassivi = testMassivi.withColumn(
    "Costo lavorazione plico",
    regexp_replace(col("Costo lavorazione plico"), ",", ".").cast("double")
)

testMassivi = testMassivi.withColumn(
    "Costo Demat",
    regexp_replace(col("Costo Demat"), ",", ".").cast("double")
)

# CONVERSIONE DI Costo pagine e NUMERO PAGINE 
testMassivi = testMassivi.withColumn(
    "Costo pagine",
    regexp_replace(col("Costo pagine"), ",", ".").cast("double")
)

testMassivi = testMassivi.withColumn(
    "NUMERO PAGINE",
    col("NUMERO PAGINE").cast("int")
)

# Calcolo della colonna costo_singola_pagina --- dubbio: costo singola pagina ha già i input il costo arrotondato delle pagine complessivo, è corretto oppure devo arrotondare dopo?
testMassivi = testMassivi.withColumn(
    "costo_singola_pagina",
    when((col("NUMERO PAGINE") - 1) > 0, col("Costo pagine") / (col("NUMERO PAGINE") - 1)) # questo da ricontrollare -- funziona perchè attualmente il caso 10g non lo consideriamo
    .otherwise(None)
)

# Tolgo tutti i test sul perso 10g
testMassivi = testMassivi.filter(col("PESO") != 10)

# Creazione della colonna di identificativo Peso per la chiave di join 
testMassivi = testMassivi.withColumn(
    "Peso_id",
    when((col("PESO") >= 1) & (col("PESO") <= 20), 1)
    .when((col("PESO") >= 21) & (col("PESO") <= 50), 2)
    .when((col("PESO") >= 51) & (col("PESO") <= 100), 3)
    .when((col("PESO") >= 101) & (col("PESO") <= 250), 4)
    .when((col("PESO") >= 251) & (col("PESO") <= 350), 5)
    .when((col("PESO") >= 351) & (col("PESO") <= 1000), 6)
    .when((col("PESO") >= 1001) & (col("PESO") <= 2000), 7)
    .otherwise(None)  # o 0 o altro valore se fuori da questi range
)

### DIVISIONE DEI CAP NAZIONALI DA QUELLI INTERNAZIONALI

# CAP solo numerici (nazionali)
testMassiviNazionali = testMassivi.filter(col("CAP").rlike("^[0-9]+$"))

# CAP senza numeri (internazionali)
testMassiviInternazionali = testMassivi.filter(col("CAP").rlike("^[^0-9]+$"))


################################### GESTIONE DEI NAZIONALI 

# Aggiunta degli 000 ai CAP -> solo sui nazionali 
testMassiviNazionali = testMassiviNazionali.withColumn("CAP", lpad(col("CAP"), 5, "0"))

#testMassiviNazionali .printSchema()

testMassiviNazionali = testMassiviNazionali .withColumn(
    "Concatena_CAP_Prodotto_Lotto_Peso",
    concat(
        col("CAP").cast("string"),
        col("PRODOTTO"),
        col("Lotto").cast("string"),
        col("Peso_id").cast("string")
    )
)

###il peso id va da 1 a 8 
#abbiamo un duplicato sul range 1 --> 0-20 viene testata due volte (nel caso della busta da 20g con pagine aggiuntive 2 | caso della busta con 10g senza pagine aggiuntive)

####################################### GESTIONE DEGLI INTERNAZIONALI --> nei test massivi ci sono solo 3 righe

testMassiviInternazionali = testMassiviInternazionali .withColumn(
    "Concatena_Zona_Prodotto_Lotto_Peso",
    concat(
        col("Zona"),
        col("PRODOTTO"),
        col("Lotto").cast("string"),
        col("Peso_id").cast("string")
    )
)

#testMassivi.printSchema()

######################################### CSV MatriceCosti

#versione con inferschema 
#matriceCosti: DataFrame = spark_session.read.csv(input_config_dict.get('source_local_matriceCosti'), header=True, inferSchema=True, sep=';')

#versione senza inferschema
schema_MatriceCosti = StructType([
    StructField("geokey", StringType(), True),
    StructField("product", StringType(), True),
    StructField("recapitista", StringType(), True),
    StructField("lotto", StringType(), True),
    StructField("costo_plico", StringType(), True),
    StructField("costo_foglio", StringType(), True),
    StructField("costo_demat", StringType(), True),
    StructField("min", StringType(), True),
    StructField("max", StringType(), True),
    StructField("costo", StringType(), True),
    StructField("costo_base_20gr", StringType(), True),
    StructField("startDate", StringType(), True),
    StructField("endDate", StringType(), True)
])

matriceCosti: DataFrame = spark_session.read.csv(input_config_dict.get('source_local_matriceCosti'), header=True, schema = schema_MatriceCosti, sep=';')

matriceCosti.printSchema()

#Mi porto dietro la zona
matriceCosti = matriceCosti.join(zone, on="geokey",  how="left")

matriceCosti = matriceCosti.withColumn(
    "Peso",
    when((col("min") >= 1) & (col("max") <= 20), 1)
    .when((col("min") >= 21) & (col("max") <= 50), 2)
    .when((col("min") >= 51) & (col("max") <= 100), 3)
    .when((col("min") >= 101) & (col("max") <= 250), 4)
    .when((col("min") >= 251) & (col("max") <= 350), 5)
    .when((col("min") >= 351) & (col("max") <= 1000), 6)
    .when((col("min") >= 1001) & (col("max") <= 2000), 7)
    .otherwise(None)  # o 0 o altro valore se fuori da questi range
)

### DIVIDERE LE GEOKEY  NAZIONALI DA QUELLE INTERNAZIONALI ->

# geokey solo numerici (nazionali)
matriceCostiNazionale = matriceCosti.filter(col("geokey").rlike("^[0-9]+$"))

# geokey senza numeri (internazionali)
matriceCostiInternazionale = matriceCosti.filter(col("geokey").rlike("^[^0-9]+$"))


# Aggiunta degli 000 ai CAP
matriceCostiNazionale = matriceCostiNazionale.withColumn("geokey", lpad(col("geokey"), 5, "0"))

############# GESTIONE NAZIONALE MATRICE COSTI 

#matriceCosti.printSchema()

matriceCostiNazionale = matriceCostiNazionale.withColumn(
    "Concatena_CAP_Prodotto_Lotto_Peso",
    concat(
        col("geokey").cast("string"),
        col("product"),
        col("lotto").cast("string"),
        col("Peso").cast("string")
    )
)

############# GESTIONE INTERNAZIONALE MATRICE COSTI 

#matriceCosti.printSchema()

matriceCostiInterazionale = matriceCostiInternazionale.withColumn(
    "Concatena_Zona_Prodotto_Lotto_Peso",
    concat(
        col("zone"), #verificare che mi porto dietro la zona nella join
        col("product"),
        col("lotto").cast("string"),
        col("Peso").cast("string")
    )
)


#### gestire sia i casi nazionali che quelli internazionali


### JOIN sui nazionali 
df_joined_nazionale = matriceCostiNazionale.alias("m").join(
    testMassiviNazionali.select("Concatena_CAP_Prodotto_Lotto_Peso", "COSTO NOTIFICA PESO", "Costo Demat", "Costo lavorazione plico", "costo_singola_pagina").alias("t"),
    on="Concatena_CAP_Prodotto_Lotto_Peso",
    how="left"
)

###################################################################################### FLAG

### Flag Costo Peso Corretto -- se è true allora è corretto tra i due file
df_joined_nazionale = df_joined_nazionale.withColumn(
    "costo_peso_int",
    round(col("COSTO NOTIFICA PESO") * 100).cast("int")
)


df_joined_nazionale = df_joined_nazionale.withColumn(
    "flag_costo_peso_corretto",
    (col("costo_peso_int") == col("costo").cast("int")).cast("boolean")
)


### Flag Costo Demat Corretto -- se è true allora è corretto tra i due file
df_joined_nazionale = df_joined_nazionale.withColumn(
    "costo_demat_int",
    round(col("Costo Demat") * 100, 0).cast("int")
)

df_joined_nazionale = df_joined_nazionale.withColumn(
    "flag_costo_demat_corretto",
    (col("costo_demat_int") == col("costo_demat")).cast("boolean")
)

### Flag Costo Plico Corretto -- se è true allora è corretto tra i due file
df_joined_nazionale = df_joined_nazionale.withColumn(
    "costo_plico_int",
    round(col("Costo lavorazione plico") * 100, 0).cast("int")
)

df_joined_nazionale = df_joined_nazionale.withColumn(
    "flag_costo_plico_corretto",
    (col("costo_plico_int") == col("costo_plico").cast("int")).cast("boolean")
)

### Flag Costo Foglio Corretto -- se è true allora è corretto tra i due file
df_joined_nazionale = df_joined_nazionale.withColumn(
    "costo_foglio_int",
    round(col("costo_singola_pagina") * 100, 0).cast("int")
)

df_joined_nazionale = df_joined_nazionale.withColumn(
    "flag_costo_foglio_corretto",
    (col("costo_foglio_int") == col("costo_foglio").cast("int")).cast("boolean")
)

### FLAG FINALE --> mi dice s tutti i costi sono corretti per quel record
df_joined_nazionale = df_joined_nazionale.withColumn(
    "flag_complessivo_costi",
    (col("flag_costo_peso_corretto") & col("flag_costo_demat_corretto") & col("flag_costo_plico_corretto") & col("flag_costo_foglio_corretto")).cast("boolean")
)


df_joined_nazionale.createOrReplaceTempView("DF_JOINED_NAZIONALE") #fare un export solo per nazionale



# JOIN su internazionali
df_joined_internazionale = matriceCostiInterazionale.alias("m").join(
    testMassiviInternazionali.select(
        col("Concatena_Zona_Prodotto_Lotto_Peso").alias("join_key"),"COSTO NOTIFICA PESO", "Costo Demat", "Costo lavorazione plico", "costo_singola_pagina").alias("t"),
    col("m.Concatena_Zona_Prodotto_Lotto_Peso") == col("t.join_key"),
    how="left"
).drop("join_key")

###################################################################################### FLAG

# Confronto costo per internazionali (stessa logica dei nazionali)
### Flag Costo Peso Corretto -- se è true allora è corretto tra i due file
df_joined_internazionale = df_joined_internazionale.withColumn(
    "costo_peso_int",
    round(col("COSTO NOTIFICA PESO") * 100).cast("int")
)

df_joined_internazionale = df_joined_internazionale.withColumn(
    "flag_costo_peso_corretto",
    (col("costo_peso_int") == col("costo").cast("int")).cast("boolean")
)


### Flag Costo Demat Corretto -- se è true allora è corretto tra i due file
df_joined_internazionale = df_joined_internazionale.withColumn(
    "costo_demat_int",
    round(col("Costo Demat") * 100, 0).cast("int")
)

df_joined_internazionale = df_joined_internazionale.withColumn(
    "flag_costo_demat_corretto",
    (col("costo_demat_int") == col("costo_demat")).cast("boolean")
)

### Flag Costo Plico Corretto -- se è true allora è corretto tra i due file
df_joined_internazionale = df_joined_internazionale.withColumn(
    "costo_plico_int",
    round(col("Costo lavorazione plico") * 100, 0).cast("int")
)

df_joined_internazionale = df_joined_internazionale.withColumn(
    "flag_costo_plico_corretto",
    (col("costo_plico_int") == col("costo_plico").cast("int")).cast("boolean")
)

### Flag Costo Foglio Corretto -- se è true allora è corretto tra i due file
df_joined_internazionale = df_joined_internazionale.withColumn(
    "costo_foglio_int",
    round(col("costo_singola_pagina") * 100, 0).cast("int")
)

df_joined_internazionale = df_joined_internazionale.withColumn(
    "flag_costo_foglio_corretto",
    (col("costo_foglio_int") == col("costo_foglio").cast("int")).cast("boolean")
)

### FLAG FINALE --> mi dice s tutti i costi sono corretti per quel record
df_joined_internazionale = df_joined_internazionale.withColumn(
    "flag_complessivo_costi",
    (col("flag_costo_peso_corretto") & col("flag_costo_demat_corretto") & col("flag_costo_plico_corretto") & col("flag_costo_foglio_corretto")).cast("boolean")
)


df_joined_internazionale.createOrReplaceTempView("DF_JOINED_INTERNAZIONALE")


############ JOIN TRA NAZIONALI E INTERNAZIONALI 

#print(df_joined_nazionale.columns)
#print(df_joined_internazionale.columns)

df_nazionale_renamed = df_joined_nazionale.withColumnRenamed("Concatena_CAP_Prodotto_Lotto_Peso", "concatena_key")

df_nazionale_renamed.printSchema()

df_internazionale_renamed = df_joined_internazionale.withColumnRenamed("Concatena_Zona_Prodotto_Lotto_Peso", "concatena_key")

df_internazionale_renamed.printSchema()

df_final = df_nazionale_renamed.unionByName(df_internazionale_renamed, allowMissingColumns=True)

df_final.createOrReplaceTempView("DF_JOINED_UNICO")

############################################# Scrittura in tabella

output_path: str = os.path.join(output_config_dict.get('path'), unique_uuid, "check_matrice") + '.csv'
dataframe_writer: DataFrameWriter = df_final.repartition(1).write.mode(saveMode='overwrite')
dataframe_writer.options(header='True', delimiter=';').csv(output_path)

spark_session.stop()