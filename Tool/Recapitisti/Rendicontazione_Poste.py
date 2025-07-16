import configparser
import uuid
import os
from typing import Dict
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrameWriter
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, IntegerType, DateType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta, date
from pyspark.sql.functions import col, ceil, when, lit


spark = SparkSession.builder.getOrCreate()

######################################################## Configurazione iniziale: parto dalla silver postalizzazione denormalized 

print("Inizio Configurazione...")






df1 = spark.sql(""" 
                    SELECT  iun,
                            requestid,
                            requesttimestamp,
                            prodotto,
                            geokey,
                            recapitista,
                            recapitista_unificato,
                            lotto,
                            codice_oggetto,
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
                    FROM send.gold_postalizzazione_analytics
                    WHERE fine_recapito_data_rendicontazione IS NOT NULL 
                    AND recapitista_unificato = 'Poste'
                    AND fine_recapito_stato NOT IN ('RECRS006', 'RECRS013','RECRN006', 'RECRN013', 'RECAG004', 'RECAG013', 'RECRI005', 'RECRSI005')
                    AND  ( (demat_23l_ar_data_rendicontazione IS NOT NULL AND demat_plico_data_rendicontazione IS NOT NULL )
                            OR (demat_23l_ar_data_rendicontazione IS NULL AND demat_plico_data_rendicontazione IS NOT NULL)
                            OR  (demat_23l_ar_data_rendicontazione IS NOT NULL AND demat_plico_data_rendicontazione IS NULL) )
                    AND (
                        (accettazione_recapitista_con018_data IS NOT NULL AND CEIL(MONTH(accettazione_recapitista_con018_data) / 3) = 1)
                        OR (accettazione_recapitista_con018_data IS NULL AND affido_recapitista_con016_data IS NOT NULL AND CEIL(MONTH(affido_recapitista_con016_data + INTERVAL 1 DAY) / 3) = 1)
                    )
                    AND (
                        YEAR(CASE 
                            WHEN accettazione_recapitista_con018_data IS NULL THEN affido_recapitista_con016_data + INTERVAL 1 DAY
                            ELSE accettazione_recapitista_con018_data
                        END) = 2024
                    );
                    """)

print("Creo la temporary view ...")

######################################################## Configurazione


df1 = df1.withColumn(
    'dematerializzazione_data_rendicontazione',
    F.when(
        F.col('demat_23l_ar_data_rendicontazione').isNull(),  
        F.col('demat_plico_data_rendicontazione')  
    ).otherwise(
        F.when(
            F.col('demat_plico_data_rendicontazione').isNull(), 
            F.col('demat_23l_ar_data_rendicontazione')  
        ).otherwise(
            F.greatest('demat_23l_ar_data_rendicontazione', 'demat_plico_data_rendicontazione')  
        )
    )
)


######################################################## Inizio prima query

print("Inizio Prima Query Esiti...")

df1 = df1.withColumn(
    "tot_esiti",
    F.when(
        F.col("tentativo_recapito_stato").isin('RECRN010', 'RECRS010', 'RECAG010'),

        (F.when(F.col("tentativo_recapito_stato").isNotNull(), 1).otherwise(0)) +
        (F.when(F.col("messaingiacenza_recapito_stato").isNotNull(), 1).otherwise(0)) +
        (F.when(F.col("fine_recapito_stato").isNotNull(), 1).otherwise(0)) +
        (F.when(F.col("accettazione_23l_recag012_data").isNotNull(), 1).otherwise(0))
    ).otherwise(
        (F.when(F.col("fine_recapito_stato").isNotNull(), 1).otherwise(0))
    )
).withColumn(
    "esiti_plico",
    F.when(
        F.col("fine_recapito_stato").isin('RECAG003C', 'RECAG003F', 'RECAG007C', 'RECAG008C', 'RECRN002C', 'RECRN002F', 'RECRN004C', 'RECRN005C', 'RECRS002C', 'RECRS002F', 'RECRS004C', 'RECRS005C'),
        1
    ).otherwise(0)
).withColumn(
    "esiti_no_plico",
    F.col("tot_esiti") - F.col("esiti_plico")
)

######################################################## Ritardo Tentativo Recapito

df1 = df1.withColumn(
    "ritardo_tentativo_recapito",
    F.when(F.col("tentativo_recapito_stato").isin('RECRN010', 'RECRS010', 'RECAG010'),
        F.when(
            (F.col("recapitista").isin('Poste', 'FSU', 'FSU - AR', 'FSU - RS', 'FSU - 890')) &
            (F.col("affido_recapitista_con016_data") >= F.lit("2023-07-15 00:00:00").cast("timestamp")) &
            (F.col("affido_recapitista_con016_data") < F.lit("2024-05-01 00:00:00").cast("timestamp")),
            F.when(
                F.to_date(F.col("tentativo_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(tentativo_recapito_data), 16)"),
                ((F.unix_timestamp("tentativo_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(tentativo_recapito_data), 16)"))) / 3600 ).cast("int")
            ).otherwise(None)
        ).when(
            (F.col("lotto") == '27') &
            (
                ((F.col("accettazione_recapitista_con018_data") >= F.lit("2024-06-01 00:00:00").cast("timestamp")) &
                (F.col("accettazione_recapitista_con018_data") < F.lit("2024-08-01 00:00:00").cast("timestamp"))) |
                ((F.col("accettazione_recapitista_con018_data") >= F.lit("2024-09-01 00:00:00").cast("timestamp")) &
                (F.col("accettazione_recapitista_con018_data") < F.lit("2024-10-01 00:00:00").cast("timestamp")))
            ),
            F.when(
                F.col("tentativo_recapito_data_rendicontazione").cast("date") >= F.expr("date_add(tentativo_recapito_data, 31)"),
                ((F.unix_timestamp("tentativo_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(tentativo_recapito_data), 31)"))) / 3600).cast("int")
            ).otherwise(None)
        ).when(
            F.col("prodotto") == "890",
            F.when(
                F.col("lotto").isin('1','3','4','6','8','12','13','14','15','17','18','19','20') &
                (F.col("tentativo_recapito_data_rendicontazione").cast("date") >= F.expr("date_add(tentativo_recapito_data, 6)")),
                ((F.unix_timestamp("tentativo_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(tentativo_recapito_data), 6)"))) / 3600).cast("int")
            ).when(
                F.col("lotto").isin('2','5','7','9','10','11','16','99') &
                (F.col("tentativo_recapito_data_rendicontazione").cast("date") >= F.expr("date_add(tentativo_recapito_data, 8)")),
                ((F.unix_timestamp("tentativo_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(tentativo_recapito_data), 8)"))) / 3600).cast("int")
            ).otherwise(None)
        ).when(
            F.col("prodotto") == "RS",
            F.when(
                F.col("lotto").isin('21','22','23','24') &
                (F.col("tentativo_recapito_data_rendicontazione").cast("date") >= F.expr("date_add(tentativo_recapito_data, 11)")),
            ((F.unix_timestamp("tentativo_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(tentativo_recapito_data), 11)"))) / 3600).cast("int")
            ).when(
                F.col("lotto").isin('25','97') &
                (F.col("tentativo_recapito_data_rendicontazione").cast("date") >= F.expr("date_add(tentativo_recapito_data, 16)")),
                ((F.unix_timestamp("tentativo_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(tentativo_recapito_data), 16)"))) / 3600).cast("int")
            ).otherwise(None)
        ).when(
            F.col("prodotto") == "AR",
            F.when(
                F.col("lotto").isin('26','28','29','30BIS','98') &   #integrazione
                (F.col("tentativo_recapito_data_rendicontazione").cast("date") >= F.expr("date_add(tentativo_recapito_data, 8)")),
                ((F.unix_timestamp("tentativo_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(tentativo_recapito_data), 8)"))) / 3600).cast("int")
            ).when(
                F.col("lotto").isin('27','30') &
                (F.col("tentativo_recapito_data_rendicontazione").cast("date") >= F.expr("date_add(tentativo_recapito_data, 6)")),
                ((F.unix_timestamp("tentativo_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(tentativo_recapito_data), 6)"))) / 3600).cast("int")
            ).otherwise(None)
        ).when(
            (F.col("prodotto") == "RIR") & (F.col("lotto") == '26'),
            F.when(
                F.col("tentativo_recapito_data_rendicontazione").cast("date") >= F.expr("date_add(tentativo_recapito_data, 16)"),
                ((F.unix_timestamp("tentativo_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(tentativo_recapito_data), 16)"))) / 3600).cast("int")
            ).otherwise(None)
        ).when(
            (F.col("prodotto") == "RIS") & (F.col("lotto") == '21'),
            F.when(
                F.col("tentativo_recapito_data_rendicontazione").cast("date") >= F.expr("date_add(tentativo_recapito_data, 16)"),
                ((F.unix_timestamp("tentativo_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(tentativo_recapito_data), 16)"))) / 3600).cast("int")
            ).otherwise(None)
        ).otherwise(0)
    ).otherwise(None)
)

######################################################## Ritardo Messa in Giacenza

df1 = df1.withColumn(
    "ritardo_messa_in_giacenza",
    F.when(
        (F.col("recapitista").isin("Poste", "FSU", "FSU - AR", "FSU - RS", "FSU - 890")) &
        (F.col("affido_recapitista_con016_data") >= F.lit("2023-07-15 00:00:00").cast("timestamp")) &
        (F.col("affido_recapitista_con016_data") < F.lit("2024-05-01 00:00:00").cast("timestamp")),
        F.when(
            F.to_date(F.col("messaingiacenza_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(messaingiacenza_recapito_data), 16)"),
            ((F.unix_timestamp("messaingiacenza_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(messaingiacenza_recapito_data), 16)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("lotto") == '27') & (
            (F.col("accettazione_recapitista_con018_data") >= F.lit("2024-06-01 00:00:00").cast("timestamp")) &
            (F.col("accettazione_recapitista_con018_data") < F.lit("2024-08-01 00:00:00").cast("timestamp"))
        ) | (
            (F.col("accettazione_recapitista_con018_data") >= F.lit("2024-09-01 00:00:00").cast("timestamp")) &
            (F.col("accettazione_recapitista_con018_data") < F.lit("2024-10-01 00:00:00").cast("timestamp"))
        ),
        F.when(
            F.to_date(F.col("messaingiacenza_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(messaingiacenza_recapito_data), 31)"),
            ((F.unix_timestamp("messaingiacenza_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(messaingiacenza_recapito_data), 31)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == '890') & (F.col("lotto").isin('1','3','4','6','8','12','13','14','15','17','18','19','20')),
        F.when(
            F.to_date(F.col("messaingiacenza_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(messaingiacenza_recapito_data), 6)"),
            ((F.unix_timestamp("messaingiacenza_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(messaingiacenza_recapito_data), 6)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == '890') & (F.col("lotto").isin('2','5','7','9','10','11','16','99')),
        F.when(
            F.to_date(F.col("messaingiacenza_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(messaingiacenza_recapito_data), 8)"),
            ((F.unix_timestamp("messaingiacenza_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(messaingiacenza_recapito_data), 8)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "RS") & (F.col("lotto").isin('21', '22', '23', '24')),
        F.when(
            F.to_date(F.col("messaingiacenza_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(messaingiacenza_recapito_data), 11)"),
            ((F.unix_timestamp("messaingiacenza_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(messaingiacenza_recapito_data), 11)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "RS") & (F.col("lotto").isin('25', '97')),
        F.when(
            F.to_date(F.col("messaingiacenza_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(messaingiacenza_recapito_data), 16)"),
            ((F.unix_timestamp("messaingiacenza_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(messaingiacenza_recapito_data), 16)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "AR") & (F.col("lotto").isin('26', '28', '29', '30BIS','98')), #integrazione
        F.when(
            F.to_date(F.col("messaingiacenza_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(messaingiacenza_recapito_data), 8)"),
            ((F.unix_timestamp("messaingiacenza_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(messaingiacenza_recapito_data), 8)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "AR") & (F.col("lotto").isin('27', '30')),
        F.when(
            F.to_date(F.col("messaingiacenza_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(messaingiacenza_recapito_data), 6)"),
            ((F.unix_timestamp("messaingiacenza_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(messaingiacenza_recapito_data), 6)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "RIR") & (F.col("lotto") == '26'),
        F.when(
            F.to_date(F.col("messaingiacenza_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(messaingiacenza_recapito_data), 16)"),
            ((F.unix_timestamp("messaingiacenza_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(messaingiacenza_recapito_data), 16)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "RIS") & (F.col("lotto") == '21'),
        F.when(
            F.to_date(F.col("messaingiacenza_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(messaingiacenza_recapito_data), 16)"),
            ((F.unix_timestamp("messaingiacenza_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(messaingiacenza_recapito_data), 16)"))) / 3600).cast("int")
        )
    ).otherwise(0)
)

######################################################## Ritardo Accettazione 23L

df1 = df1.withColumn(
    "ritardo_accettazione_23L",
    F.when(
        (F.col("recapitista").isin("Poste", "FSU", "FSU - AR", "FSU - RS", "FSU - 890")) &
        (F.col("affido_recapitista_con016_data") >= F.lit("2023-07-15 00:00:00").cast("timestamp")) &
        (F.col("affido_recapitista_con016_data") < F.lit("2024-05-01 00:00:00").cast("timestamp")),
        F.when(
            F.to_date(F.col("rend_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23l_recag012_data), 16)"),
            ((F.unix_timestamp("rend_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23l_recag012_data), 16)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("lotto").isin('27', '22')) & (
            (F.col("accettazione_recapitista_con018_data") >= F.lit("2024-06-01 00:00:00").cast("timestamp")) &
            (F.col("accettazione_recapitista_con018_data") < F.lit("2024-08-01 00:00:00").cast("timestamp"))
        ) | (
            (F.col("accettazione_recapitista_con018_data") >= F.lit("2024-09-01 00:00:00").cast("timestamp")) &
            (F.col("accettazione_recapitista_con018_data") < F.lit("2024-10-01 00:00:00").cast("timestamp"))
        ),
        F.when(
            F.to_date(F.col("rend_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23l_recag012_data), 31)"),
            ((F.unix_timestamp("rend_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23l_recag012_data), 31)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == '890') & (F.col("lotto").isin('1', '3', '4', '6', '8', '12', '13', '14', '15', '17', '18', '19', '20')),
        F.when(
            F.to_date(F.col("rend_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23l_recag012_data), 6)"),
            ((F.unix_timestamp("rend_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23l_recag012_data), 6)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == '890') & (F.col("lotto").isin('2','5','7','9','10','11','16','99')),
        F.when(
            F.to_date(F.col("rend_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23l_recag012_data), 8)"),
            ((F.unix_timestamp("rend_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23l_recag012_data), 8)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "RS") & (F.col("lotto").isin('21','22','23','24')),
        F.when(
            F.to_date(F.col("rend_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23l_recag012_data), 11)"),
            ((F.unix_timestamp("rend_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23l_recag012_data), 11)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "RS") & (F.col("lotto").isin('25', '97')),
        F.when(
            F.to_date(F.col("rend_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23l_recag012_data), 16)"),
            ((F.unix_timestamp("rend_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23l_recag012_data), 16)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "AR") & (F.col("lotto").isin('26', '28', '29', '30BIS', '98')),  #integrazione
        F.when(
            F.to_date(F.col("rend_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23l_recag012_data), 8)"),
            ((F.unix_timestamp("rend_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23l_recag012_data), 8)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "AR") & (F.col("lotto").isin('27', '30')),
        F.when(
            F.to_date(F.col("rend_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23l_recag012_data), 6)"),
            ((F.unix_timestamp("rend_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23l_recag012_data), 6)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "RIR") & (F.col("lotto") == '26'),
        F.when(
            F.to_date(F.col("rend_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23l_recag012_data), 16)"),
            ((F.unix_timestamp("rend_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23l_recag012_data), 16)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "RIS") & (F.col("lotto") == '21'),
        F.when(
            F.to_date(F.col("rend_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23l_recag012_data), 16)"),
            ((F.unix_timestamp("rend_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23l_recag012_data), 16)"))) / 3600).cast("int")
        )
    ).otherwise(0)
)

######################################################## Ritardo Fine Recapito
 
df1 = df1.withColumn(
    "ritardo_fine_recapito",
    F.when(
        ~F.col("fine_recapito_stato").isin('RECAG003C', 'RECAG003F', 'RECAG007C', 'RECAG008C',
                                           'RECRN002C', 'RECRN002F', 'RECRN004C', 'RECRN005C',
                                           'RECRS002C', 'RECRS002F', 'RECRS004C', 'RECRS005C'),
        
        #IF: quando rientra negli stati che non prevedono demat -- uso fine_recapito_data_rendicontazione
        F.when( 
            F.col("fine_recapito_stato").isin('RECRS001C', 'RECRS003C', 'RECRSI001'), 
            F.when(
                (F.col("recapitista").isin("Poste", "FSU", "FSU - AR", "FSU - RS", "FSU - 890")) &
                (F.col("affido_recapitista_con016_data") >= F.lit("2023-07-15 00:00:00").cast("timestamp")) &
                (F.col("affido_recapitista_con016_data") < F.lit("2024-05-01 00:00:00").cast("timestamp")),
                F.when(
                    F.to_date(F.col("fine_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 16)"),
                    ((F.unix_timestamp("fine_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 16)"))) / 3600).cast("int")
                )
            ).when(
                # Rilassamento per lotto 27, calcolo sui giorni lavorativi
                (F.col("lotto").isin('27', '22')) & (
                    ((F.col("accettazione_recapitista_con018_data") >= F.lit("2024-06-01 00:00:00").cast("timestamp")) &
                        (F.col("accettazione_recapitista_con018_data") < F.lit("2024-11-01 00:00:00").cast("timestamp")))
                ),
                F.when(
                    F.to_date(F.col("fine_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 31)"),
                    ((F.unix_timestamp("fine_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 31)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == '890') & (F.col("lotto").isin('1', '3', '4', '6', '8', '12', '13', '14', '15', '17', '18', '19', '20')),
                F.when(
                    F.to_date(F.col("fine_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 6)"),
                    ((F.unix_timestamp("fine_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 6)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == '890') & (F.col("lotto").isin('2', '5', '7', '9', '10', '11', '16','99')),
                F.when(
                    F.to_date(F.col("fine_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 8)"),
                    ((F.unix_timestamp("fine_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 8)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "RS") & (F.col("lotto").isin('21', '22', '23', '24')),
                F.when(
                    F.to_date(F.col("fine_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 11)"),
                    ((F.unix_timestamp("fine_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 11)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "RS") & (F.col("lotto").isin('25', '97')),
                F.when(
                    F.to_date(F.col("fine_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 16)"),
                    ((F.unix_timestamp("fine_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 16)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "AR") & (F.col("lotto").isin('26', '28', '29', '30BIS', '98')), #integrazione
                F.when(
                    F.to_date(F.col("fine_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 8)"),
                    ((F.unix_timestamp("fine_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 8)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "AR") & (F.col("lotto").isin('27', '30')),
                F.when(
                    F.to_date(F.col("fine_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 6)"),
                    ((F.unix_timestamp("fine_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 6)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "RIR") & (F.col("lotto") == '26'),
                F.when(
                    F.to_date(F.col("fine_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 16)"),
                    ((F.unix_timestamp("fine_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 16)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "RIS") & (F.col("lotto") == '21'),
                F.when(
                    F.to_date(F.col("fine_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 16)"),
                    ((F.unix_timestamp("fine_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 16)"))) / 3600).cast("int")
                )
            )
        ).otherwise( #ELSE: quando rientra negli stati che prevedono la demat - uso dematerializzazione_data_rendicontazione
            F.when(
                (F.col("recapitista").isin("Poste", "FSU", "FSU - AR", "FSU - RS", "FSU - 890")) &
                (F.col("affido_recapitista_con016_data") >= F.lit("2023-07-15 00:00:00").cast("timestamp")) &
                (F.col("affido_recapitista_con016_data") < F.lit("2024-05-01 00:00:00").cast("timestamp")),
                F.when(
                    F.to_date(F.col("dematerializzazione_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 16)"),
                    ((F.unix_timestamp("dematerializzazione_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 16)"))) / 3600).cast("int")
                )
            ).when(
                # Rilassamento per lotto 27, calcolo sui giorni lavorativi
                (F.col("lotto").isin('27', '22')) & (
                    ((F.col("accettazione_recapitista_con018_data") >= F.lit("2024-06-01 00:00:00").cast("timestamp")) &
                        (F.col("accettazione_recapitista_con018_data") < F.lit("2024-11-01 00:00:00").cast("timestamp")))
                ),
                F.when(
                    F.to_date(F.col("dematerializzazione_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 31)"),
                    ((F.unix_timestamp("dematerializzazione_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 31)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == '890') & (F.col("lotto").isin('1', '3', '4', '6', '8', '12', '13', '14', '15', '17', '18', '19', '20')),
                F.when(
                    F.to_date(F.col("dematerializzazione_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 6)"),
                    ((F.unix_timestamp("dematerializzazione_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 6)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == '890') & (F.col("lotto").isin('2', '5', '7', '9', '10', '11', '16', '99')),
                F.when(
                    F.to_date(F.col("dematerializzazione_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 8)"),
                    ((F.unix_timestamp("dematerializzazione_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 8)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "RS") & (F.col("lotto").isin('21', '22', '23', '24')),
                F.when(
                    F.to_date(F.col("dematerializzazione_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 11)"),
                    ((F.unix_timestamp("dematerializzazione_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 11)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "RS") & (F.col("lotto").isin('25', '97')),
                F.when(
                    F.to_date(F.col("dematerializzazione_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 16)"),
                    ((F.unix_timestamp("dematerializzazione_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 16)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "AR") & (F.col("lotto").isin('26', '28', '29', '30BIS', '98')), #integrazione
                F.when(
                    F.to_date(F.col("dematerializzazione_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 8)"),
                    ((F.unix_timestamp("dematerializzazione_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 8)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "AR") & (F.col("lotto").isin('27', '30')),
                F.when(
                    F.to_date(F.col("dematerializzazione_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 6)"),
                    ((F.unix_timestamp("dematerializzazione_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 6)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "RIR") & (F.col("lotto") == '26'),
                F.when(
                    F.to_date(F.col("dematerializzazione_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 16)"),
                    ((F.unix_timestamp("dematerializzazione_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 16)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "RIS") & (F.col("lotto") == '21'),
                F.when(
                    F.to_date(F.col("dematerializzazione_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 16)"),
                    ((F.unix_timestamp("dematerializzazione_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 16)"))) / 3600).cast("int")
                )
            )
        )
    ).otherwise(None)
)

######################################################## Ritardo Plico

df1 = df1.withColumn(
    "ritardo_plico",
    F.when(
        F.col("fine_recapito_stato").isin('RECAG003C', 'RECAG003F', 'RECAG007C', 'RECAG008C',
                                           'RECRN002C', 'RECRN002F', 'RECRN004C', 'RECRN005C',
                                           'RECRS002C', 'RECRS002F', 'RECRS004C', 'RECRS005C'),
        F.when(
            # Rilassamento per lotto 27, calcolo con giorni lavorativi
            (F.col("lotto").isin('27', '22')) & (
                ((F.col("accettazione_recapitista_con018_data") >= F.lit("2024-06-01 00:00:00").cast("timestamp")) &
                    (F.col("accettazione_recapitista_con018_data") < F.lit("2024-11-01 00:00:00").cast("timestamp")))
            ),
            F.when(
                F.to_date(F.col("dematerializzazione_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 31)"),
                ((F.unix_timestamp("dematerializzazione_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 31)"))) / 3600).cast("int")
            )
        ).when(
            F.to_date(F.col("dematerializzazione_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 22)"),
            ((F.unix_timestamp("dematerializzazione_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 22)"))) / 3600).cast("int")
        ).otherwise(None)
    ).otherwise(None)
)

######################################################## Violazioni SLA

df1 = df1.withColumn(
    "esiti_rendicontati_violazione_sla_no_plico",
    F.when(
        (F.col("fine_recapito_stato").isNull()) |
        (~F.col("fine_recapito_stato").isin('RECAG003C', 'RECAG003F', 'RECAG007C', 'RECAG008C',
                                             'RECRN002C', 'RECRN002F', 'RECRN004C', 'RECRN005C',
                                             'RECRS002C', 'RECRS002F', 'RECRS004C', 'RECRS005C')),
        F.when(
            F.col("tentativo_recapito_stato") == F.col("certificazione_recapito_stato"),
            F.when(F.col("ritardo_fine_recapito") > 0, 1).otherwise(None)
        ).otherwise(
            F.when(
                F.col("tentativo_recapito_stato").isin('RECRN010', 'RECRS010', 'RECAG010'),
                (F.when(F.col("ritardo_tentativo_recapito") > 0, 1).otherwise(0) +
                 F.when(F.col("ritardo_fine_recapito") > 0, 1).otherwise(0) +
                 F.when(F.col("ritardo_messa_in_giacenza") > 0, 1).otherwise(0) +
                 F.when(F.col("ritardo_accettazione_23L") > 0, 1).otherwise(0))
            ).otherwise(
                (F.when(F.col("ritardo_fine_recapito") > 0, 1).otherwise(0) +
                 F.when(F.col("ritardo_messa_in_giacenza") > 0, 1).otherwise(0) +
                 F.when(F.col("ritardo_accettazione_23L") > 0, 1).otherwise(0))
            )
        )
    ).otherwise(
        F.when(
            F.col("tentativo_recapito_stato").isin('RECRN010', 'RECRS010', 'RECAG010'),
            (F.when(F.col("ritardo_tentativo_recapito") > 0, 1).otherwise(0) +
             F.when(F.col("ritardo_messa_in_giacenza") > 0, 1).otherwise(0) +
             F.when(F.col("ritardo_accettazione_23L") > 0, 1).otherwise(0))
        ).otherwise(
            (F.when(F.col("ritardo_messa_in_giacenza") > 0, 1).otherwise(0) +
             F.when(F.col("ritardo_accettazione_23L") > 0, 1).otherwise(0))
        )
    )
)

df1 = df1.withColumn(
    "esiti_rendicontati_violazione_sla_plico",
    F.when(
        F.col("fine_recapito_stato").isin('RECAG003C', 'RECAG003F', 'RECAG007C', 'RECAG008C',
                                           'RECRN002C', 'RECRN002F', 'RECRN004C', 'RECRN005C',
                                           'RECRS002C', 'RECRS002F', 'RECRS004C', 'RECRS005C'),
        F.when(F.col("ritardo_plico") > 0, 1).otherwise(None)
    ).otherwise(None)
)


df1.createOrReplaceTempView("dettaglio")

######################################################## Estrazione di dettaglio - scrittura in tabella

spark.sql("""SELECT * FROM dettaglio""").writeTo("send_dev.penali_rendicontazione_dettaglio")\
                                        .using("iceberg")\
                                        .tableProperty("format-version","2")\
                                        .tableProperty("engine.hive.enabled","true")\
                                        .createOrReplace()

######################################################## Inizio seconda query

SailpostData = df1.filter(F.col("recapitista_unificato") == 'RTI Sailpost-Snem').groupBy(
    "recapitista_unificato",
    F.year("accettazione_recapitista_con018_data").alias("anno"),
    (F.ceil(F.month("accettazione_recapitista_con018_data") / 3)).alias("trimestre")
    ).agg(
    F.sum(F.coalesce(F.col("esiti_plico"), F.lit(0))).alias("esiti_tot_plico"),
    F.sum(F.coalesce(F.col("esiti_no_plico"), F.lit(0))).alias("esiti_tot_no_plico"),
    (F.sum(F.coalesce(F.col("esiti_plico"), F.lit(0))) +
     F.sum(F.coalesce(F.col("esiti_no_plico"), F.lit(0)))).alias("esiti_tot"),

    F.sum(F.coalesce(F.col("esiti_rendicontati_violazione_sla_no_plico"), F.lit(0))).alias(
        "somma_esiti_violazione_no_plico"),
    F.sum(F.coalesce(F.col("esiti_rendicontati_violazione_sla_plico"), F.lit(0))).alias("somma_esiti_violazione_plico"),


    F.floor(
        F.sum(F.coalesce(F.col("ritardo_tentativo_recapito"), F.lit(0)) +
              F.coalesce(F.col("ritardo_messa_in_giacenza"), F.lit(0)) +
              F.coalesce(F.col("ritardo_fine_recapito"), F.lit(0)) +
              F.coalesce(F.col("ritardo_accettazione_23L"), F.lit(0))) #/24
    ).alias("somma_ritardi"),

    F.floor(
        F.sum(F.coalesce(F.col("ritardo_plico"), F.lit(0))) / 24.0
    ).alias("ritardo_plico_in_giorni"),

    F.count(F.col("ritardo_tentativo_recapito")).alias("count_tentativo_recapito"),
    F.count(F.col("ritardo_messa_in_giacenza")).alias("count_messa_in_giacenza"),
    F.count(F.col("ritardo_fine_recapito")).alias("count_fine_recapito"),
    F.count(F.col("ritardo_accettazione_23L")).alias("count_accettazione_23L")
)

OtherRecapitistaData = df1.filter(F.col("recapitista_unificato") != 'RTI Sailpost-Snem').groupBy(
    "recapitista_unificato",
    "lotto",
    "prodotto",
    F.year("accettazione_recapitista_con018_data").alias("anno"),
    F.ceil(F.month("accettazione_recapitista_con018_data") / 3).alias("trimestre")
).agg(
    F.sum(F.coalesce("esiti_plico", F.lit(0))).alias("esiti_tot_plico"),
    F.sum(F.coalesce("esiti_no_plico", F.lit(0))).alias("esiti_tot_no_plico"),
    (F.sum(F.coalesce("esiti_plico", F.lit(0))) + F.sum(F.coalesce("esiti_no_plico", F.lit(0)))).alias("esiti_tot"),
    F.sum(F.coalesce("esiti_rendicontati_violazione_sla_no_plico", F.lit(0))).alias("somma_esiti_violazione_no_plico"),
    F.sum(F.coalesce("esiti_rendicontati_violazione_sla_plico", F.lit(0))).alias("somma_esiti_violazione_plico"),
    F.floor(
        (F.sum(F.coalesce("ritardo_tentativo_recapito", F.lit(0))) +
         F.sum(F.coalesce("ritardo_messa_in_giacenza", F.lit(0))) +
         F.sum(F.coalesce("ritardo_fine_recapito", F.lit(0))) +
         F.sum(F.coalesce("ritardo_accettazione_23L", F.lit(0)))) #/ 24
    ).alias("somma_ritardi"),
    F.floor(F.sum(F.coalesce("ritardo_plico", F.lit(0))) / 24.0).alias("ritardo_plico_in_giorni"),
    F.count("ritardo_tentativo_recapito").alias("count_tentativo_recapito"),
    F.count("ritardo_messa_in_giacenza").alias("count_messa_in_giacenza"),
    F.count("ritardo_fine_recapito").alias("count_fine_recapito"),
    F.count("ritardo_accettazione_23L").alias("count_accettazione_23L")
)


######################################################## Calcolo delle Penali

PenaleRendicontazioneSailpost = (
    SailpostData.select(
        "recapitista_unificato",
        F.lit(None).alias("prodotto"),
        F.lit(None).alias("lotto"),
        "anno",
        "trimestre",
        "esiti_tot_plico",
        "esiti_tot_no_plico",
        "esiti_tot",
        "somma_esiti_violazione_no_plico",
        "somma_esiti_violazione_plico",
        F.round(
            F.coalesce(
                (F.col("somma_esiti_violazione_no_plico") + F.col("somma_esiti_violazione_plico")) /
                F.when(
                    (F.col("esiti_tot_plico") + F.col("esiti_tot_no_plico")) == 0,
                    None
                ).otherwise(F.col("esiti_tot_plico") + F.col("esiti_tot_no_plico")),
                F.lit(0) 
            ), 4 
        ).alias("Ritardo_nella_Rendicontazione"),
        F.when(
            (F.col("somma_esiti_violazione_no_plico") /
             F.when(F.col("esiti_tot_no_plico") == 0, None).otherwise(F.col("esiti_tot_no_plico"))) - 0.021 > 0,
            F.greatest(
                F.round(
                    ((F.col("somma_esiti_violazione_no_plico") /
                      F.when(F.col("esiti_tot_no_plico") == 0, None).otherwise(F.col("esiti_tot_no_plico"))) - 0.021) * 1000,
                    0
                ) * F.lit(500),
                F.lit(0)
            )
        ).otherwise(0).alias("Penale_Rendicontazione_No_Plico"),
        F.when(
            (F.col("somma_esiti_violazione_plico") /
             F.when(F.col("esiti_tot_plico") == 0, None).otherwise(F.col("esiti_tot_plico"))) - 0.021 > 0,
            F.greatest(
                F.round(
                    ((F.col("somma_esiti_violazione_plico") /
                      F.when(F.col("esiti_tot_plico") == 0, None).otherwise(F.col("esiti_tot_plico"))) - 0.021) * 1000,
                    0
                ) * F.lit(300),
                F.lit(0)
            )
        ).otherwise(0).alias("Penale_Rendicontazione_Plico"),
        F.when(
            ((F.col("count_tentativo_recapito") + F.col("count_messa_in_giacenza") +
              F.col( "count_fine_recapito") + F.col("count_accettazione_23L")) > 0 )&
            (F.coalesce(F.col("somma_ritardi"), F.lit(0)) > 0),
            F.round(
                (F.coalesce(F.col("somma_ritardi"), F.lit(0)) /
                 F.coalesce(
                     (F.col("count_tentativo_recapito") +
                      F.col("count_messa_in_giacenza") +
                      F.col("count_fine_recapito") +
                      F.col("count_accettazione_23L")), F.lit(0))
                 ), 2
            )
        ).otherwise(0).alias("Ritardo_Medio")))

PenaleRendicontazioneOther = (
    OtherRecapitistaData.select(
        "recapitista_unificato",
        "prodotto",
        "lotto",
        "anno",
        "trimestre",
        "esiti_tot_plico",
        "esiti_tot_no_plico",
        "esiti_tot",
        "somma_esiti_violazione_no_plico",
        "somma_esiti_violazione_plico",
        F.round(
            F.coalesce(
                (F.col("somma_esiti_violazione_no_plico") + F.col("somma_esiti_violazione_plico")) /
                F.when(
                    (F.col("esiti_tot_plico") + F.col("esiti_tot_no_plico")) == 0,
                    None
                ).otherwise(F.col("esiti_tot_plico") + F.col("esiti_tot_no_plico")),
                F.lit(0)
            ), 4
        ).alias("Ritardo_nella_Rendicontazione"),

        F.when(
            ((F.when(F.col("somma_esiti_violazione_no_plico").isNull(), F.lit(0))
            .otherwise(F.col("somma_esiti_violazione_no_plico")) /
            F.when(F.col("esiti_tot_no_plico") == 0, F.lit(None))
            .otherwise(F.col("esiti_tot_no_plico"))) - 0.021) > 0,
            F.when(
                (F.col("prodotto") == '890') & (F.col("lotto").between(1, 20)),
                F.greatest(
                    F.round(
                        ((F.when(F.col("somma_esiti_violazione_no_plico").isNull(), F.lit(0))
                        .otherwise(F.col("somma_esiti_violazione_no_plico")) /
                        F.when(F.col("esiti_tot_no_plico") == 0, F.lit(None))
                        .otherwise(F.col("esiti_tot_no_plico"))) - 0.021) * 1000, 0) * 500, F.lit(0)
                )
            ).when(
                (F.col("prodotto") == 'RS') & (F.col("lotto").between(21, 25)),
                F.greatest(
                    F.round(
                        ((F.when(F.col("somma_esiti_violazione_no_plico").isNull(), F.lit(0))
                        .otherwise(F.col("somma_esiti_violazione_no_plico")) /
                        F.when(F.col("esiti_tot_no_plico") == 0, F.lit(None))
                        .otherwise(F.col("esiti_tot_no_plico"))) - 0.021) * 1000, 0) * 300, F.lit(0)
                )
            ).when(
                (F.col("prodotto") == 'AR') & (F.col("lotto").between(26, 30)),
                F.greatest(
                    F.round(
                        ((F.when(F.col("somma_esiti_violazione_no_plico").isNull(), F.lit(0))
                        .otherwise(F.col("somma_esiti_violazione_no_plico")) /
                        F.when(F.col("esiti_tot_no_plico") == 0, F.lit(None))
                        .otherwise(F.col("esiti_tot_no_plico"))) - 0.021) * 1000, 0) * 500, F.lit(0)
                )
            ).when(
                (F.col("prodotto") == 'RS') & (F.col("lotto") == 97),
                F.greatest(F.when(F.col("somma_ritardi").isNull(), F.lit(0))
                            .otherwise(F.col("somma_ritardi") / 24 ) * 0.0678 * 0.001, F.lit(0))
            ).when(
                (F.col("prodotto") == 'AR') & (F.col("lotto") == 98),
                F.greatest(F.when(F.col("somma_ritardi").isNull(), F.lit(0))
                            .otherwise(F.col("somma_ritardi") / 24 ) * 0.0225 * 0.001, F.lit(0))
            ).when(
                (F.col("prodotto") == '890') & (F.col("lotto") == 99),
                F.greatest(F.when(F.col("somma_ritardi").isNull(), F.lit(0))
                            .otherwise(F.col("somma_ritardi") / 24 ) * 0.19 * 0.001, F.lit(0))
            ).otherwise(0)
        ).otherwise(0).alias("Penale_Rendicontazione_No_Plico"),

        F.when(
            ((F.coalesce(col("somma_esiti_violazione_plico"), lit(0)) /
            col("esiti_tot_plico").cast("double")).alias("ratio") - 0.021) > 0,
            F.when(
                (col("prodotto") == "890") & (col("lotto").between(1, 20)),
                F.greatest(
                    F.round(
                        ((F.coalesce(col("somma_esiti_violazione_plico"), lit(0)) /
                        col("esiti_tot_plico").cast("double")) - 0.021) * 1000, 0
                    ) * 300,
                    lit(0)
                )
            ).when(
                (col("prodotto") == "RS") & (col("lotto").between(21, 25)),
                F.greatest(
                    F.round(
                        ((F.coalesce(col("somma_esiti_violazione_plico"), lit(0)) /
                        col("esiti_tot_plico").cast("double")) - 0.021) * 1000, 0
                    ) * 300,
                    lit(0)
                )
            ).when(
                (col("prodotto") == "AR") & (col("lotto").between(26, 30)),
                F.greatest(
                    F.round(
                        ((F.coalesce(col("somma_esiti_violazione_plico"), lit(0)) /
                        col("esiti_tot_plico").cast("double")) - 0.021) * 1000, 0
                    ) * 300,
                    lit(0)
                )
            ).when(
                (col("prodotto") == "RS") & (col("lotto") == 97),
                F.greatest(F.coalesce(col("ritardo_plico_in_giorni"), lit(0)) * 0.0678 * 0.001, lit(0))
            ).when(
                (col("prodotto") == "AR") & (col("lotto") == 98),
                F.greatest(F.coalesce(col("ritardo_plico_in_giorni"), lit(0)) * 0.0225 * 0.001, lit(0))
            ).when(
                (col("prodotto") == "RS") & (col("lotto") == 99),
                F.greatest(F.coalesce(col("ritardo_plico_in_giorni"), lit(0)) * 0.19 * 0.001, lit(0))
            ).otherwise(lit(0))
        ).otherwise(lit(0)).alias("Penale_Rendicontazione_Plico"),

        F.when(
            (col("prodotto") == "890") & (col("lotto").between(1, 20)),
            F.round(
                F.coalesce(col("somma_ritardi"), lit(0)) /
                (F.coalesce(col("count_tentativo_recapito"), lit(0)) +
                F.coalesce(col("count_messa_in_giacenza"), lit(0)) +
                F.coalesce(col("count_fine_recapito"), lit(0)) +
                F.coalesce(col("count_accettazione_23L"), lit(0))).cast("double"),
                2
            )
        ).when(
            (col("prodotto") == "AR") & (col("lotto").between(26, 30)),
            F.round(
                F.coalesce(col("somma_ritardi"), lit(0)) /
                (F.coalesce(col("count_tentativo_recapito"), lit(0)) +
                F.coalesce(col("count_messa_in_giacenza"), lit(0)) +
                F.coalesce(col("count_fine_recapito"), lit(0)) +
                F.coalesce(col("count_accettazione_23L"), lit(0))).cast("double"),
                2
            )
        ).otherwise(lit(0)).alias("Ritardo_Medio")
    )
)

PenaleRendicontazione = PenaleRendicontazioneSailpost.union(PenaleRendicontazioneOther)

######################################################## Query Finale per l'estrazione aggregata
final_result = (
    PenaleRendicontazione.select(
        F.col("recapitista_unificato").alias("Recapitista"),
        F.col("prodotto").alias("Prodotto"),
        F.col("lotto").alias("Lotto"),
        F.col("anno").alias("Anno"),
        F.col("trimestre").alias("Trimestre"),
        (F.col("somma_esiti_violazione_no_plico") + F.col("somma_esiti_violazione_plico")).alias("Esiti_con_Violazione_SLA"),
        "esiti_tot",
        "somma_esiti_violazione_no_plico",
        "esiti_tot_no_plico",
        F.round(
            F.coalesce(
                F.col("somma_esiti_violazione_no_plico") /
                F.when(F.col("esiti_tot_no_plico") == 0, None).otherwise(F.col("esiti_tot_no_plico")),
                F.lit(0)
            ), 4
        ).alias("Percentuale_SLA_No_Plico"),
        "Penale_Rendicontazione_No_Plico",
        "somma_esiti_violazione_plico",
        "esiti_tot_plico",
        F.round(
            F.coalesce(
                F.col("somma_esiti_violazione_plico") /
                F.when(F.col("esiti_tot_plico") == 0, None).otherwise(F.col("esiti_tot_plico")),
                F.lit(0)
            ), 4
        ).alias("Percentuale_SLA_Plico"),
        "Penale_Rendicontazione_Plico",
        F.round(F.coalesce(F.col("Ritardo_Medio"), F.lit(0)), 2).alias("Ritardo_Medio"),
        F.round(F.coalesce(F.col("Ritardo_Medio") * 100, F.lit(0)), 2).alias("Penale_Ritardo_Medio")
    )
)

final_result.createOrReplaceTempView("aggregato")

######################################################## Estrazione aggregato, scrittura in tabella
spark.sql("""SELECT * FROM aggregato""").writeTo("send_dev.penali_rendicontazione_aggregato")\
                                        .using("iceberg")\
                                        .tableProperty("format-version","2")\
                                        .tableProperty("engine.hive.enabled","true")\
                                        .createOrReplace()

spark.stop()