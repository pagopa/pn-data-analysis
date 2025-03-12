import uuid
import os
from pyspark.sql.functions import lit, col
from pyspark.sql import SparkSession, DataFrameWriter
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from datetime import datetime, timedelta

######################################### Configurazione iniziale

spark = SparkSession.builder.getOrCreate()

# Read dataframe report sla
df_filtrato = spark.sql( """   
    SELECT *
    FROM send.gold_postalizzazione_analytics
    WHERE fine_recapito_stato NOT IN ( 'RECRS006','RECRS013','RECRS015','RECRN006', 'RECRN013', 'RECRN015','RECAG004','RECAG013','RECAG015')
    AND CEIL(MONTH(fine_recapito_data_rendicontazione) / 3) = 4 
    AND YEAR(fine_recapito_data_rendicontazione) = 2024
    AND recapitista IN ('RTI Fulmine - Sol. Camp.', 'RTI Fulmine - Forgilu') 
    """ )

df_filtrato.createTempView("gold_postalizzazione_analytics")

record_count = spark.sql("SELECT COUNT(*) AS total_records FROM gold_postalizzazione_analytics")
record_count.show()


# Creazione del dataframe con le festività
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

######################################################## Aggiunta delle UDF per il calcolo dei giorni lavorativi lotto 22,27


def datediff_workdays(start_date, end_date):
    try:

        if start_date is None or end_date is None:
            return None

        if end_date < start_date:
            return 0

        total_days = (end_date - start_date).days

        days_list = [start_date + timedelta(days=i) for i in range(1, total_days + 1)]


        working_days = [d for d in days_list if d.weekday() < 5 and d.date() not in holiday_dates]

        total_working_days = 0

        if working_days:
            total_working_days += len(working_days)

        hours_end_date = 24 - end_date.hour
        
        return total_working_days * 24  - hours_end_date

    except Exception as e:
        return None

datediff_workdays_udf = udf(datediff_workdays, IntegerType())


unique_uuid: str = str(uuid.uuid4())

######################################################## Inizio prima query

df1 = df_filtrato.withColumn(
    "tot_esiti",
    F.when(
        F.col("tentativo_recapito_stato").isin('RECRN010', 'RECRS010', 'RECAG010'),
        (F.when(F.col("tentativo_recapito_stato").isNotNull(), 1).otherwise(0)) +
        (F.when(F.col("messaingiacenza_recapito_stato").isNotNull(), 1).otherwise(0)) +
        (F.when(F.col("fine_recapito_stato").isNotNull(), 1).otherwise(0)) +
        (F.when(F.col("accettazione_23L_RECAG012_data").isNotNull(), 1).otherwise(0))
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
                    ((F.unix_timestamp("tentativo_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(tentativo_recapito_data), 16)"))) / 3600).cast("int")
                ).otherwise(None)
            ).when(
                (F.col("lotto").isin(27, 22)) &
                (
                    ((F.col("accettazione_recapitista_con018_data") >= F.lit("2024-06-01 00:00:00").cast("timestamp")) &
                    (F.col("accettazione_recapitista_con018_data") < F.lit("2024-11-01 00:00:00").cast("timestamp")))  |
                    (F.col("accettazione_recapitista_con018_data").between(F.lit('2024-12-01'), F.lit('2025-01-01')))
                ),
                F.when(
                    (datediff_workdays_udf(F.col("tentativo_recapito_data"),F.col("tentativo_recapito_data_rendicontazione")) - (31*24)) > 0,
                    (datediff_workdays_udf(F.col("tentativo_recapito_data"),F.col("tentativo_recapito_data_rendicontazione")) - (31*24)).cast("int")
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
        (F.col("lotto").isin(27, 22)) & (
            ((F.col("accettazione_recapitista_con018_data") >= F.lit("2024-06-01 00:00:00").cast("timestamp")) &
                    (F.col("accettazione_recapitista_con018_data") < F.lit("2024-11-01 00:00:00").cast("timestamp")) |
                    (F.col("accettazione_recapitista_con018_data").between(F.lit('2024-12-01'), F.lit('2025-01-01')))
            )
        ),
        F.when(
            (datediff_workdays_udf(F.col("messaingiacenza_recapito_data"),F.col("messaingiacenza_recapito_data_rendicontazione")) - (31*24)) > 0,
            (datediff_workdays_udf(F.col("messaingiacenza_recapito_data"),F.col("messaingiacenza_recapito_data_rendicontazione")) - (31*24)).cast("int")
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
            F.to_date(F.col("demat_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23L_RECAG012_data), 16)"),
            ((F.unix_timestamp("demat_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23L_RECAG012_data), 16)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("lotto").isin(27, 22)) & (
            ((F.col("accettazione_recapitista_con018_data") >= F.lit("2024-06-01 00:00:00").cast("timestamp")) &
                    (F.col("accettazione_recapitista_con018_data") < F.lit("2024-11-01 00:00:00").cast("timestamp")) |
                    (F.col("accettazione_recapitista_con018_data").between(F.lit('2024-12-01'), F.lit('2025-01-01')))
            )
        ),
        F.when(
            (datediff_workdays_udf(F.col("accettazione_23L_RECAG012_data"),F.col("demat_23l_data_rendicontazione")) - (31*24)) > 0,
            (datediff_workdays_udf(F.col("accettazione_23L_RECAG012_data"),F.col("demat_23l_data_rendicontazione")) - (31*24)).cast("int")
        )
    ).when(
        (F.col("prodotto") == 890) & (F.col("lotto").isin(1, 3, 4, 6, 8, 12, 13, 14, 15, 17, 18, 19, 20)),
        F.when(
            F.to_date(F.col("demat_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23L_RECAG012_data), 6)"),
            ((F.unix_timestamp("demat_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23L_RECAG012_data), 6)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == 890) & (F.col("lotto").isin(2, 5, 7, 9, 10, 11, 16, 99)),
        F.when(
            F.to_date(F.col("demat_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23L_RECAG012_data), 8)"),
            ((F.unix_timestamp("demat_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23L_RECAG012_data), 8)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "RS") & (F.col("lotto").isin(21, 22, 23, 24)),
        F.when(
            F.to_date(F.col("demat_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23L_RECAG012_data), 11)"),
            ((F.unix_timestamp("demat_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23L_RECAG012_data), 11)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "RS") & (F.col("lotto").isin(25, 97)),
        F.when(
            F.to_date(F.col("demat_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23L_RECAG012_data), 16)"),
            ((F.unix_timestamp("demat_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23L_RECAG012_data), 16)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "AR") & (F.col("lotto").isin(26, 28, 29, 98)),
        F.when(
            F.to_date(F.col("demat_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23L_RECAG012_data), 8)"),
            ((F.unix_timestamp("demat_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23L_RECAG012_data), 8)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "AR") & (F.col("lotto").isin(27, 30)),
        F.when(
            F.to_date(F.col("demat_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23L_RECAG012_data), 6)"),
            ((F.unix_timestamp("demat_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23L_RECAG012_data), 6)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "RIR") & (F.col("lotto") == 26),
        F.when(
            F.to_date(F.col("demat_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23L_RECAG012_data), 16)"),
            ((F.unix_timestamp("demat_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23L_RECAG012_data), 16)"))) / 3600).cast("int")
        )
    ).when(
        (F.col("prodotto") == "RIS") & (F.col("lotto") == 21),
        F.when(
            F.to_date(F.col("demat_23l_data_rendicontazione")) >= F.expr("date_add(to_date(accettazione_23L_RECAG012_data), 16)"),
            ((F.unix_timestamp("demat_23l_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(accettazione_23L_RECAG012_data), 16)"))) / 3600).cast("int")
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
                    (F.col("accettazione_recapitista_con018_data") < F.lit("2024-11-01 00:00:00").cast("timestamp"))) |
                    (F.col("accettazione_recapitista_con018_data").between(F.lit('2024-12-01'), F.lit('2025-01-01')))
            ),
            F.when(
                (datediff_workdays_udf(F.col("certificazione_recapito_data"),F.col("fine_recapito_data_rendicontazione")) - (31*24)) > 0,
                (datediff_workdays_udf(F.col("certificazione_recapito_data"),F.col("fine_recapito_data_rendicontazione")) - (31*24)).cast("int")
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
        ).otherwise(None)
    ).otherwise(None)
)



df1 = df1.withColumn(
    "ritardo_plico",
    F.when(
        F.col("fine_recapito_stato").isin('RECAG003C', 'RECAG003F', 'RECAG007C', 'RECAG008C',
                                           'RECRN002C', 'RECRN002F', 'RECRN004C', 'RECRN005C',
                                           'RECRS002C', 'RECRS002F', 'RECRS004C', 'RECRS005C'),
        F.when(
            # Rilassamento per lotto 27, calcolo sui giorni lavorativi
            (F.col("lotto").isin(27, 22)) & (
                ((F.col("accettazione_recapitista_con018_data") >= F.lit("2024-06-01 00:00:00").cast("timestamp")) &
                    (F.col("accettazione_recapitista_con018_data") < F.lit("2024-11-01 00:00:00").cast("timestamp")))  |
                    (F.col("accettazione_recapitista_con018_data").between(F.lit('2024-12-01'), F.lit('2025-01-01')))
            ),
            F.when(
                (datediff_workdays_udf(F.col("certificazione_recapito_data"),F.col("fine_recapito_data_rendicontazione")) - (31*24)) > 0,
                (datediff_workdays_udf(F.col("certificazione_recapito_data"),F.col("fine_recapito_data_rendicontazione")) - (31*24)).cast("int")
            )
        ).when(
            F.to_date(F.col("fine_recapito_data_rendicontazione")) >= F.expr("date_add(to_date(certificazione_recapito_data), 22)"),
            ((F.unix_timestamp("fine_recapito_data_rendicontazione") - F.unix_timestamp(F.expr("date_add(to_date(certificazione_recapito_data), 22)"))) / 3600).cast("int")
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
    F.year("fine_recapito_data_rendicontazione").alias("anno"),
    (F.ceil(F.month("fine_recapito_data_rendicontazione") / 3)).alias("trimestre")
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
    F.year("fine_recapito_data_rendicontazione").alias("anno"),
    F.ceil(F.month("fine_recapito_data_rendicontazione") / 3).alias("trimestre")
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
                            .otherwise(F.col("somma_ritardi")  / 24 ) * 0.19 * 0.001, F.lit(0))
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