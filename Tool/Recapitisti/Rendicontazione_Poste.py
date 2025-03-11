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

df = spark.sql("""
        SELECT
            replace(postalizzazione_requestid, 'pn-cons-000~', '') AS requestid,
            xpagopaextchcxid,
            senderpaid,
            iun,
            sentat AS data_deposito,
            taxonomycode,
            CAST(
                CASE
                    WHEN CHAR_LENGTH(requesttimestamp) = 17 THEN 
                        REPLACE(requesttimestamp,'Z',':00Z')
                    WHEN CHAR_LENGTH(requesttimestamp) = 20 THEN
                        requesttimestamp
                    ELSE NULL
                END
            AS timestamp) AS requesttimestamp,
            pm_product_type AS prodotto,
            geokey,
            recapitista,
            lotto,
            CAST(COALESCE(costo,0) AS int) AS costo_scaglione,
            CAST(COALESCE(costo_demat,0) AS int) AS costo_demat,
            CAST(COALESCE(costo_plico,0) AS int) AS costo_plico,
            CAST(COALESCE(costo_foglio,0) AS int) AS costo_foglio,
            CAST(COALESCE(costo_base_20gr,0) AS int) AS costo_base_20gr,
            CAST(COALESCE(analog_cost,0) AS int) AS prezzo_ente,
            CAST(COALESCE(envelope_weight,0) AS int) AS grammatura,
            CAST(COALESCE(number_of_pages,0) AS int) AS numero_pagine,
            TRANSFORM(
                ARRAY_SORT(
                    TRANSFORM(
                        eventslist,
                        e -> named_struct(
                            'key',
                            CASE
                                WHEN LEFT(e.inserttimestamp, 4) = '1970' and e.paperprogrstatus.clientrequesttimestamp IS NOT NULL 
                                    THEN e.paperprogrstatus.clientrequesttimestamp
                                    ELSE e.inserttimestamp
                            END,
                            'statusCode', e.paperprogrstatus.statuscode,
                            'registeredLetterCode', e.paperprogrstatus.registeredlettercode,
                            'deliveryFailureCause', e.paperprogrstatus.deliveryfailurecause,
                            'statusDateTime', CAST(LEFT(e.paperprogrstatus.statusdatetime, 16) AS timestamp),
                            'rendicontazioneDateTime', CAST(LEFT(e.paperprogrstatus.clientrequesttimestamp, 16) AS timestamp),
                            'insertDateTime', CAST(LEFT(e.inserttimestamp, 16) AS timestamp),
                            'attachments', TRANSFORM(e.paperprogrstatus.attachments, a -> named_struct(
                                                                                            'documenttype', a.documenttype,
                                                                                            'id', a.id,
                                                                                            'uri', a.uri
                                                                                            ))
                        )
                    ),
                    (a, b) -> CASE
                        WHEN a.key < b.key THEN -1
                        WHEN a.key > b.key THEN 1
                        ELSE 0
                    END
                ),
                e -> named_struct(
                    'statusCode', e.statusCode,
                    'registeredLetterCode', e.registeredLetterCode,
                    'deliveryFailureCause', e.deliveryFailureCause,
                    'statusDateTime', e.statusDateTime,
                    'rendicontazioneDateTime', e.rendicontazioneDateTime,
                    'insertDateTime', e.insertDateTime,
                    'attachments', e.attachments
                )
            ) AS ordered_events
        FROM send.silver_postalizzazione_denormalized
        WHERE
            pm_product_type IN ('890', 'AR', 'RS', 'RIS', 'RIR')
    """)

print("Creo la temporary view PREORDERED_EVENTS..")

df.createOrReplaceTempView("PREORDERED_EVENTS")

print("Inizio la query di df_demat...")

df_demat = spark.sql("""
        SELECT
            *,
            RTRIM(
                ARRAY_JOIN(
                    ARRAY_DISTINCT(
                        TRANSFORM(
                            FILTER(
                                ordered_events,
                                e -> e.statusCode not in ('REC090', 'RECAG012')
                            ),
                            e -> e.registeredLetterCode 
                        )
                    ),
                    ' '
                )
            ) AS codice_oggetto,
            ELEMENT_AT(
                    FILTER(
                        ordered_events,
                        e -> e.statusCode == 'P000'
                    ),
                1
            ) AS affido_consolidatore,
            ELEMENT_AT(
                    FILTER(
                        ordered_events,
                        e -> e.statusCode in ('CON993', 'CON995', 'CON996', 'CON997', 'CON998')
                    ),
                1
            ) AS scarto_consolidatore,
            ELEMENT_AT(
                TRANSFORM(
                    FILTER(
                        ordered_events,
                        e -> e.statusCode == 'CON080'
                    ),
                    e -> e.statusDateTime
                ),
                -1
            ) AS stampa_imbustamento_statusDateTime,
            ELEMENT_AT(
                TRANSFORM(
                    FILTER(
                        ordered_events,
                        e -> e.statusCode == 'con016'
                    ),
                    e -> e.statusDateTime
                ),
                -1
            ) AS affido_recapitista_statusDateTime,
            ELEMENT_AT(
                TRANSFORM(
                    FILTER(
                        ordered_events,
                        e -> e.statusCode == 'CON018'
                    ),
                    e -> e.statusDateTime
                ),
                -1
            ) AS accettazione_recapitista_statusDateTime,
            ELEMENT_AT(
                TRANSFORM(
		            FILTER(
                        ordered_events, 
                        e -> e.statusCode=='CON020'
                    ),
		            e ->  e.statusDateTime
		        ),
                -1
            ) AS affido_conservato_con020_statusDateTime,
            ELEMENT_AT(
                TRANSFORM(
		            FILTER(
                        ordered_events, 
                        e -> e.statusCode=='CON09A'
                    ),
		            e ->  e.statusDateTime
		        ),
                -1
            ) AS materialita_pronta_con09A_statusDateTime,
            ELEMENT_AT(
                    FILTER(
                        ordered_events,
                        e -> e.statusCode in (
                            'RECRS001C','RECRS002A','RECRS002D','RECRS010','RECRS006','RECRS013',
                            'RECRN001A','RECRN002A','RECRN002D','RECRN010','RECRN006','RECRN013',
                            'RECAG001A','RECAG002A','RECAG003A','RECAG003D','RECAG010','RECAG004','RECAG013',
                            'RECRSI001','RECRSI005',
                            'RECRI001','RECRI005',
                            'PN999','PN998'
                        )
                    ),
                -1
            ) AS tentativo_recapito,
            ELEMENT_AT(
                    FILTER(
                        ordered_events,
                        e -> e.statusCode in (
                            'RECRS011',
                            'RECRN011',
                            'RECAG011A',
                            'RECRSI002',
                            'RECRI002'
                        )
                    ),
                -1
            ) AS messaingiacenza,
            ELEMENT_AT(
                    FILTER(
                        ordered_events,
                        e -> e.statusCode in (
                            'RECRS001C','RECRS002A','RECRS002D','RECRS003C','RECRS004A','RECRS005A','RECRS006','RECRS013',
                            'RECRN001A','RECRN002A','RECRN002D','RECRN003A','RECRN004A','RECRN005A','RECRN006','RECRN013',
                            'RECAG001A','RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A','RECAG007A','RECAG008A','RECAG004','RECAG013',
                            'RECRSI003C','RECRSI004A','RECRSI005',
                            'RECRI003A','RECRI004A','RECRI005',
                            'PN999','PN998'
                        )
                    ),
                -1
            ) AS certificazione_recapito,
            ELEMENT_AT(
                    FILTER(
                        ordered_events,
                        e -> e.statusCode in (
                            'RECRS001C','RECRS002C','RECRS002F','RECRS003C','RECRS004C','RECRS005C','RECRS006','RECRS013',
                            'RECRN001C','RECRN002C','RECRN002F','RECRN003C','RECRN004C','RECRN005C','RECRN006','RECRN013',
                            'RECAG001C','RECAG002C','RECAG003C','RECAG003F','RECAG005C','RECAG006C','RECAG007C','RECAG008C','RECAG004','RECAG013',
                            'RECRSI003C','RECRSI004C','RECRSI005',
                            'RECRI003C','RECRI004C','RECRI005',
                            'PN999','PN998'
                        )
                    ),
                1
            ) AS fine_recapito,
            ELEMENT_AT(
                    FILTER(
                        ordered_events,
                        e -> e.statusCode == 'RECAG012'
                    ),
                -1
            ) AS accettazione_23l,
            ELEMENT_AT(
                    FILTER(
                        ordered_events,
                        e -> (
                            size(e.attachments) > 0 AND 
                            ARRAY_CONTAINS(e.attachments.documenttype, '23L')
                        )
                    ),
                -1
            ) AS rend_23l,
            ELEMENT_AT(
                    FILTER(
                        ordered_events,
                        e -> e.statusCode in (
                            'RECRS015',
                            'RECRN015',
                            'RECAG015'
                        )
                    ),
                -1
            ) AS causa_forza_maggiore,
            ELEMENT_AT(ordered_events,-1) AS ultimo_evento,
            ELEMENT_AT(
                    FILTER(
                        ordered_events,
                        e -> (
                            size(e.attachments) > 0 AND 
                            (
                            ARRAY_CONTAINS(e.attachments.documenttype, '23L') OR
                            ARRAY_CONTAINS(e.attachments.documenttype, 'AR')
                            ) AND
                            e.statusCode RLIKE '^REC.*[BE]$'
                        )
                    ),
                -1
            ) AS demat_23l_ar,
            ELEMENT_AT(
                    FILTER(
                        ordered_events,
                        e -> (
                            size(e.attachments) > 0 AND 
                            ARRAY_CONTAINS(e.attachments.documenttype, 'Plico') AND
                            e.statusCode RLIKE '^REC.*[BE]$'
                        )
                    ),
                -1
            ) AS demat_plico,
            ELEMENT_AT(
                    FILTER(
                        ordered_events,
                        e -> (
                            size(e.attachments) > 0 AND 
                            ARRAY_CONTAINS(e.attachments.documenttype, 'ARCAD') AND
                            e.statusCode RLIKE '^REC.*[BE]$'
                        )
                    ),
                -1
            ) AS demat_arcad,
            ELEMENT_AT(
                TRANSFORM(
                    FILTER(
                        ordered_events,
                        e -> (
                            e.statusCode IN (
                                'RECRS002B', 'RECRS002E', 'RECRS004B', 'RECRS005B',
                                'RECRN001B', 'RECRN002B', 'RECRN002E', 'RECRN003B', 
                                'RECRN004B', 'RECRN005B', 'RECAG001B', 'RECAG002B', 
                                'RECAG003B', 'RECAG011B', 'RECAG003E', 'RECAG005B',
                                'RECAG006B', 'RECAG007B', 'RECAG008B', 'RECRI003B',
                                'RECRI004B', 'RECRSI004B'
                            ) 
                            AND 
                            -- Verifica che ci sia almeno un allegato e che almeno uno non abbia documentType 'ARCAD'
                            size(e.attachments) > 0 
                            AND 
                            NOT ARRAY_CONTAINS(e.attachments.documenttype, 'ARCAD')
                        )
                    ),
                    e -> named_struct(
                        'statusCode', e.statusCode,
                        'statusDateTime', left(e.statusDateTime, 16),
                        'rendicontazioneDateTime', left(e.rendicontazioneDateTime, 16),
                        'documentType', 
                            ELEMENT_AT(
                                FILTER(e.attachments, attachment -> attachment.documenttype != 'ARCAD'),
                                -1
                            ).documentType
                    )
                ),
                -1  
            ) AS dematerializzazione
        FROM
            PREORDERED_EVENTS
    """)

print("Creo la temporary view di KPI_STRUCTURED_DEMAT...")

df_demat.createOrReplaceTempView("KPI_STRUCTURED_DEMAT")

print("Inizio la query di kpiSLA...")

kpiSla = spark.sql("""
        SELECT
            senderpaid,
            iun,
            from_utc_timestamp("data_deposito", "CET") AS data_deposito,
            requestid,
            from_utc_timestamp("requesttimestamp", "CET") AS requesttimestamp,
            prodotto,
            geokey,
            recapitista,
            CASE
                WHEN (recapitista LIKE '%Poste%') OR (recapitista LIKE 'FSU%') THEN 'Poste'
                WHEN recapitista LIKE 'RTI Fulmine%' THEN 'Fulmine'
                ELSE recapitista
            END AS recapitista_unificato,
            lotto,
            codice_oggetto,
            from_utc_timestamp(affido_consolidatore.statusDateTime, "CET") AS affido_consolidatore_data,
            from_utc_timestamp(stampa_imbustamento_statusDateTime, "CET") AS stampa_imbustamento_con080_data,
            from_utc_timestamp(affido_recapitista_statusDateTime, "CET") AS affido_recapitista_con016_data,
            from_utc_timestamp(accettazione_recapitista_statusDateTime, "CET") AS accettazione_recapitista_con018_data,
            scarto_consolidatore.statusCode AS scarto_consolidatore_stato,
            from_utc_timestamp(scarto_consolidatore.statusDateTime, "CET") AS scarto_consolidatore_data,
            tentativo_recapito.statusCode AS tentativo_recapito_stato,
            from_utc_timestamp(tentativo_recapito.statusDateTime, "CET") AS tentativo_recapito_data,
            CASE WHEN LEFT(tentativo_recapito.insertDateTime,4) = '1970' 
                THEN from_utc_timestamp(tentativo_recapito.rendicontazioneDateTime, "CET")
                ELSE from_utc_timestamp(tentativo_recapito.insertDateTime, "CET")
            END AS tentativo_recapito_data_rendicontazione,
            messaingiacenza.statusCode AS messaingiacenza_recapito_stato,
            from_utc_timestamp(messaingiacenza.statusDateTime, "CET") AS messaingiacenza_recapito_data,
            CASE WHEN LEFT(messaingiacenza.insertDateTime,4) = '1970' 
                THEN from_utc_timestamp(messaingiacenza.rendicontazioneDateTime, "CET")
                ELSE from_utc_timestamp(messaingiacenza.insertDateTime, "CET")
            END AS messaingiacenza_recapito_data_rendicontazione,
            certificazione_recapito.statusCode AS certificazione_recapito_stato,
            certificazione_recapito.deliveryFailureCause AS certificazione_recapito_dettagli,
            from_utc_timestamp(certificazione_recapito.statusDateTime, "CET") AS certificazione_recapito_data,
            CASE WHEN LEFT(certificazione_recapito.insertDateTime,4) = '1970' 
                THEN from_utc_timestamp(certificazione_recapito.rendicontazioneDateTime, "CET")
                ELSE from_utc_timestamp(certificazione_recapito.insertDateTime, "CET")
            END AS certificazione_recapito_data_rendicontazione,     
            fine_recapito.statusCode AS fine_recapito_stato,
            from_utc_timestamp(fine_recapito.statusDateTime, "CET") AS fine_recapito_data,
            CASE WHEN LEFT(fine_recapito.insertDateTime,4) = '1970' 
                THEN from_utc_timestamp(fine_recapito.rendicontazioneDateTime, "CET")
                ELSE from_utc_timestamp(fine_recapito.insertDateTime, "CET")
            END AS fine_recapito_data_rendicontazione,
            accettazione_23l.statusDateTime AS accettazione_23l_recag012_data,
            CASE WHEN LEFT(accettazione_23l.insertDateTime,4) = '1970' 
                THEN from_utc_timestamp(accettazione_23l.rendicontazioneDateTime, "CET")
                ELSE from_utc_timestamp(accettazione_23l.insertDateTime, "CET")
            END AS accettazione_23l_recag012_data_rendicontazione,
            rend_23l.statusCode AS rend_23l_stato,
            from_utc_timestamp(rend_23l.statusDateTime, "CET") AS rend_23l_data,
            CASE WHEN LEFT(rend_23l.insertDateTime,4) = '1970' 
                THEN from_utc_timestamp(rend_23l.rendicontazioneDateTime, "CET")
                ELSE from_utc_timestamp(rend_23l.insertDateTime, "CET")
            END AS demat_23l_data_rendicontazione,
            causa_forza_maggiore.deliveryFailureCause AS causa_forza_maggiore_dettagli,
            from_utc_timestamp(causa_forza_maggiore.statusDateTime, "CET") AS causa_forza_maggiore_data,
            CASE WHEN LEFT(causa_forza_maggiore.insertDateTime,4) = '1970' 
                THEN from_utc_timestamp(causa_forza_maggiore.rendicontazioneDateTime, "CET")
                ELSE from_utc_timestamp(causa_forza_maggiore.insertDateTime, "CET")
            END AS causa_forza_maggiore_data_rendicontazione,
            dematerializzazione.statusCode AS dematerializzazione_stato,
            from_utc_timestamp(dematerializzazione.statusDateTime, "CET") AS dematerializzazione_data,
            from_utc_timestamp(dematerializzazione.rendicontazioneDateTime, "CET") AS dematerializzazione_data_rendicontazione,
            dematerializzazione.documentType AS dematerializzazione_doc            
        FROM
            KPI_STRUCTURED_DEMAT
        
""")

print("Creo la temporary view KPI_SLA...")

kpiSla.createOrReplaceTempView("KPI_SLA")

kpiSla_filtered = spark.sql(""" 
                            SELECT *
                            FROM KPI_SLA
                            WHERE fine_recapito_data_rendicontazione IS NOT NULL 
                            AND recapitista IN ('Poste', 'FSU - AR', 'FSU - 890', 'FSU - RS', 'FSU')
                            AND fine_recapito_stato NOT IN ('RECRS006', 'RECRS013', 'RECRN006', 'RECRN013','RECAG004', 'RECAG013', 'RECRI005','RECRSI005') 
                            
                            AND (
                                (accettazione_recapitista_con018_data IS NOT NULL AND CEIL(MONTH(accettazione_recapitista_con018_data) / 3) = 4)
                                OR (accettazione_recapitista_con018_data IS NULL AND affido_recapitista_con016_data IS NOT NULL AND CEIL(MONTH(affido_recapitista_con016_data + INTERVAL 1 DAY) / 3) = 4)
                            )
                            AND (
                                YEAR(CASE 
                                    WHEN accettazione_recapitista_con018_data IS NULL THEN affido_recapitista_con016_data + INTERVAL 1 DAY
                                    ELSE accettazione_recapitista_con018_data
                                END) = 2023
                            );
                            """)

print("Creo la temporary view KPI_SLA_FILTERED...")

kpiSla_filtered.createOrReplaceTempView("KPI_SLA_FILTERED")

######################################################## Configurazione

print("Except...")

df1 = spark.sql("""
        SELECT * 
        FROM KPI_SLA_FILTERED
        EXCEPT  
        SELECT * 
        FROM KPI_SLA_FILTERED
        WHERE fine_recapito_stato NOT IN ('RECRS001C','RECRS003C','RECRSI003C') 
        AND dematerializzazione_stato IS NULL
    """)

df1.createOrReplaceTempView("KPI_SLA1")

record_count = spark.sql("SELECT COUNT(*) AS total_records FROM KPI_SLA1")
record_count.show()


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
            (F.col("lotto") == 27) &
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
            F.col("prodotto") == 890,
            F.when(
                F.col("lotto").isin(1, 3, 4, 6, 8, 12, 13, 14, 15, 17, 18, 19, 20) &
                (F.col("tentativo_recapito_data_rendicontazione").cast("date") >= F.expr("date_add(tentativo_recapito_data, 6)")),
                ((F.unix_timestamp("tentativo_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(tentativo_recapito_data), 6)"))) / 3600).cast("int")
            ).when(
                F.col("lotto").isin(2, 5, 7, 9, 10, 11, 16, 99) &
                (F.col("tentativo_recapito_data_rendicontazione").cast("date") >= F.expr("date_add(tentativo_recapito_data, 8)")),
                ((F.unix_timestamp("tentativo_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(tentativo_recapito_data), 8)"))) / 3600).cast("int")
            ).otherwise(None)
        ).when(
            F.col("prodotto") == "RS",
            F.when(
                F.col("lotto").isin(21, 22, 23, 24) &
                (F.col("tentativo_recapito_data_rendicontazione").cast("date") >= F.expr("date_add(tentativo_recapito_data, 11)")),
            ((F.unix_timestamp("tentativo_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(tentativo_recapito_data), 11)"))) / 3600).cast("int")
            ).when(
                F.col("lotto").isin(25, 97) &
                (F.col("tentativo_recapito_data_rendicontazione").cast("date") >= F.expr("date_add(tentativo_recapito_data, 16)")),
                ((F.unix_timestamp("tentativo_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(tentativo_recapito_data), 16)"))) / 3600).cast("int")
            ).otherwise(None)
        ).when(
            F.col("prodotto") == "AR",
            F.when(
                F.col("lotto").isin(26, 28, 29, 98) &
                (F.col("tentativo_recapito_data_rendicontazione").cast("date") >= F.expr("date_add(tentativo_recapito_data, 8)")),
                ((F.unix_timestamp("tentativo_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(tentativo_recapito_data), 8)"))) / 3600).cast("int")
            ).when(
                F.col("lotto").isin(27, 30) &
                (F.col("tentativo_recapito_data_rendicontazione").cast("date") >= F.expr("date_add(tentativo_recapito_data, 6)")),
                ((F.unix_timestamp("tentativo_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(tentativo_recapito_data), 6)"))) / 3600).cast("int")
            ).otherwise(None)
        ).when(
            (F.col("prodotto") == "RIR") & (F.col("lotto") == 26),
            F.when(
                F.col("tentativo_recapito_data_rendicontazione").cast("date") >= F.expr("date_add(tentativo_recapito_data, 16)"),
                ((F.unix_timestamp("tentativo_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(tentativo_recapito_data), 16)"))) / 3600).cast("int")
            ).otherwise(None)
        ).when(
            (F.col("prodotto") == "RIS") & (F.col("lotto") == 21),
            F.when(
                F.col("tentativo_recapito_data_rendicontazione").cast("date") >= F.expr("date_add(tentativo_recapito_data, 16)"),
                ((F.unix_timestamp("tentativo_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(tentativo_recapito_data), 6)"))) / 3600).cast("int")
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
        (F.col("lotto") == 27) & (
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
        (F.col("prodotto") == 890) & (F.col("lotto").isin(1, 3, 4, 6, 8, 12, 13, 14, 15, 17, 18, 19, 20)),
        F.when(
            F.to_date(F.col("messaingiacenza_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(messaingiacenza_recapito_data), 6)"),
            ((F.unix_timestamp("messaingiacenza_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(messaingiacenza_recapito_data), 6)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == 890) & (F.col("lotto").isin(2, 5, 7, 9, 10, 11, 16, 99)),
        F.when(
            F.to_date(F.col("messaingiacenza_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(messaingiacenza_recapito_data), 8)"),
            ((F.unix_timestamp("messaingiacenza_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(messaingiacenza_recapito_data), 8)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "RS") & (F.col("lotto").isin(21, 22, 23, 24)),
        F.when(
            F.to_date(F.col("messaingiacenza_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(messaingiacenza_recapito_data), 11)"),
            ((F.unix_timestamp("messaingiacenza_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(messaingiacenza_recapito_data), 11)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "RS") & (F.col("lotto").isin(25, 97)),
        F.when(
            F.to_date(F.col("messaingiacenza_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(messaingiacenza_recapito_data), 16)"),
            ((F.unix_timestamp("messaingiacenza_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(messaingiacenza_recapito_data), 16)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "AR") & (F.col("lotto").isin(26, 28, 29, 98)),
        F.when(
            F.to_date(F.col("messaingiacenza_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(messaingiacenza_recapito_data), 8)"),
            ((F.unix_timestamp("messaingiacenza_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(messaingiacenza_recapito_data), 8)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "AR") & (F.col("lotto").isin(27, 30)),
        F.when(
            F.to_date(F.col("messaingiacenza_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(messaingiacenza_recapito_data), 6)"),
            ((F.unix_timestamp("messaingiacenza_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(messaingiacenza_recapito_data), 6)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "RIR") & (F.col("lotto") == 26),
        F.when(
            F.to_date(F.col("messaingiacenza_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(messaingiacenza_recapito_data), 16)"),
            ((F.unix_timestamp("messaingiacenza_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(messaingiacenza_recapito_data), 16)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "RIS") & (F.col("lotto") == 21),
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
            F.to_date(F.col("demat_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23l_recag012_data), 16)"),
            ((F.unix_timestamp("demat_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23l_recag012_data), 16)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("lotto").isin(27, 22)) & (
            (F.col("accettazione_recapitista_con018_data") >= F.lit("2024-06-01 00:00:00").cast("timestamp")) &
            (F.col("accettazione_recapitista_con018_data") < F.lit("2024-08-01 00:00:00").cast("timestamp"))
        ) | (
            (F.col("accettazione_recapitista_con018_data") >= F.lit("2024-09-01 00:00:00").cast("timestamp")) &
            (F.col("accettazione_recapitista_con018_data") < F.lit("2024-10-01 00:00:00").cast("timestamp"))
        ),
        F.when(
            F.to_date(F.col("demat_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23l_recag012_data), 31)"),
            ((F.unix_timestamp("demat_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23l_recag012_data), 31)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == 890) & (F.col("lotto").isin(1, 3, 4, 6, 8, 12, 13, 14, 15, 17, 18, 19, 20)),
        F.when(
            F.to_date(F.col("demat_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23l_recag012_data), 6)"),
            ((F.unix_timestamp("demat_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23l_recag012_data), 6)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == 890) & (F.col("lotto").isin(2, 5, 7, 9, 10, 11, 16, 99)),
        F.when(
            F.to_date(F.col("demat_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23l_recag012_data), 8)"),
            ((F.unix_timestamp("demat_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23l_recag012_data), 8)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "RS") & (F.col("lotto").isin(21, 22, 23, 24)),
        F.when(
            F.to_date(F.col("demat_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23l_recag012_data), 11)"),
            ((F.unix_timestamp("demat_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23l_recag012_data), 11)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "RS") & (F.col("lotto").isin(25, 97)),
        F.when(
            F.to_date(F.col("demat_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23l_recag012_data), 16)"),
            ((F.unix_timestamp("demat_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23l_recag012_data), 16)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "AR") & (F.col("lotto").isin(26, 28, 29, 98)),
        F.when(
            F.to_date(F.col("demat_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23l_recag012_data), 8)"),
            ((F.unix_timestamp("demat_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23l_recag012_data), 8)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "AR") & (F.col("lotto").isin(27, 30)),
        F.when(
            F.to_date(F.col("demat_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23l_recag012_data), 6)"),
            ((F.unix_timestamp("demat_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23l_recag012_data), 6)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "RIR") & (F.col("lotto") == 26),
        F.when(
            F.to_date(F.col("demat_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23l_recag012_data), 16)"),
            ((F.unix_timestamp("demat_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23l_recag012_data), 16)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "RIS") & (F.col("lotto") == 21),
        F.when(
            F.to_date(F.col("demat_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23l_recag012_data), 16)"),
            ((F.unix_timestamp("demat_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23l_recag012_data), 16)"))) / 3600).cast("int")
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
                (F.col("lotto").isin(27, 22)) & (
                    ((F.col("accettazione_recapitista_con018_data") >= F.lit("2024-06-01 00:00:00").cast("timestamp")) &
                        (F.col("accettazione_recapitista_con018_data") < F.lit("2024-11-01 00:00:00").cast("timestamp")))
                ),
                F.when(
                    F.to_date(F.col("fine_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 31)"),
                    ((F.unix_timestamp("fine_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 31)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == 890) & (F.col("lotto").isin(1, 3, 4, 6, 8, 12, 13, 14, 15, 17, 18, 19, 20)),
                F.when(
                    F.to_date(F.col("fine_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 6)"),
                    ((F.unix_timestamp("fine_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 6)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == 890) & (F.col("lotto").isin(2, 5, 7, 9, 10, 11, 16, 99)),
                F.when(
                    F.to_date(F.col("fine_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 8)"),
                    ((F.unix_timestamp("fine_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 8)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "RS") & (F.col("lotto").isin(21, 22, 23, 24)),
                F.when(
                    F.to_date(F.col("fine_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 11)"),
                    ((F.unix_timestamp("fine_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 11)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "RS") & (F.col("lotto").isin(25, 97)),
                F.when(
                    F.to_date(F.col("fine_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 16)"),
                    ((F.unix_timestamp("fine_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 16)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "AR") & (F.col("lotto").isin(26, 28, 29, 98)),
                F.when(
                    F.to_date(F.col("fine_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 8)"),
                    ((F.unix_timestamp("fine_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 8)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "AR") & (F.col("lotto").isin(27, 30)),
                F.when(
                    F.to_date(F.col("fine_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 6)"),
                    ((F.unix_timestamp("fine_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 6)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "RIR") & (F.col("lotto") == 26),
                F.when(
                    F.to_date(F.col("fine_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 16)"),
                    ((F.unix_timestamp("fine_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 16)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "RIS") & (F.col("lotto") == 21),
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
                (F.col("lotto").isin(27, 22)) & (
                    ((F.col("accettazione_recapitista_con018_data") >= F.lit("2024-06-01 00:00:00").cast("timestamp")) &
                        (F.col("accettazione_recapitista_con018_data") < F.lit("2024-11-01 00:00:00").cast("timestamp")))
                ),
                F.when(
                    F.to_date(F.col("dematerializzazione_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 31)"),
                    ((F.unix_timestamp("dematerializzazione_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 31)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == 890) & (F.col("lotto").isin(1, 3, 4, 6, 8, 12, 13, 14, 15, 17, 18, 19, 20)),
                F.when(
                    F.to_date(F.col("dematerializzazione_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 6)"),
                    ((F.unix_timestamp("dematerializzazione_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 6)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == 890) & (F.col("lotto").isin(2, 5, 7, 9, 10, 11, 16, 99)),
                F.when(
                    F.to_date(F.col("dematerializzazione_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 8)"),
                    ((F.unix_timestamp("dematerializzazione_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 8)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "RS") & (F.col("lotto").isin(21, 22, 23, 24)),
                F.when(
                    F.to_date(F.col("dematerializzazione_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 11)"),
                    ((F.unix_timestamp("dematerializzazione_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 11)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "RS") & (F.col("lotto").isin(25, 97)),
                F.when(
                    F.to_date(F.col("dematerializzazione_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 16)"),
                    ((F.unix_timestamp("dematerializzazione_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 16)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "AR") & (F.col("lotto").isin(26, 28, 29, 98)),
                F.when(
                    F.to_date(F.col("dematerializzazione_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 8)"),
                    ((F.unix_timestamp("dematerializzazione_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 8)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "AR") & (F.col("lotto").isin(27, 30)),
                F.when(
                    F.to_date(F.col("dematerializzazione_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 6)"),
                    ((F.unix_timestamp("dematerializzazione_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 6)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "RIR") & (F.col("lotto") == 26),
                F.when(
                    F.to_date(F.col("dematerializzazione_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 16)"),
                    ((F.unix_timestamp("dematerializzazione_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 16)"))) / 3600).cast("int")
                )
            ).when(
                (F.col("prodotto") == "RIS") & (F.col("lotto") == 21),
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
            (F.col("lotto").isin(27, 22)) & (
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

SailpostData = df1.filter(F.col("recapitista") == 'RTI Sailpost-Snem').groupBy(
    "recapitista",
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

OtherRecapitistaData = df1.filter(F.col("recapitista") != 'RTI Sailpost-Snem').groupBy(
    "recapitista",
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
        "recapitista",
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
        "recapitista",
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
        F.col("recapitista").alias("Recapitista"),
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