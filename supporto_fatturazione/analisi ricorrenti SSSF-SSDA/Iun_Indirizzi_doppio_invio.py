import os
import re
import uuid
import boto3
import configparser
import unicodedata

from typing import Dict
from botocore.exceptions import ClientError

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter


# ============================================================
# CONFIGURAZIONE
# ============================================================

config = configparser.ConfigParser()
config.read("app_config.ini")

spark_config_dict: Dict[str, str] = config["spark"]
input_config_dict: Dict[str, str] = config["input"]
output_config_dict: Dict[str, str] = config["output"]

SPARK_APP_NAME = spark_config_dict.get(
    "name",
    "spark-app"
)


# ============================================================
# CONFIGURAZIONE AWS / DYNAMODB
# ============================================================

AWS_PROFILE = "tunnel-confinfo"
DYNAMODB_REGION = "eu-south-1"
DYNAMODB_TABLE = "pn-ConfidentialObjects"


# ============================================================
# INPUT
# ============================================================

# File CSV contenente gli IUN.
# Il file deve avere una colonna chiamata "iun".
INPUT_FILE = "iun_indirizzi.csv"
IUN_COLUMN = "iun"


# ============================================================
# PHYSICAL ADDRESS
# ============================================================

# Tutti i campi del physicalAddress vengono utilizzati
# nel confronto Levenshtein, nello stesso ordine per
# ATTEMPT_0 e ATTEMPT_1.

PHYSICAL_ADDRESS_FIELDS = [
    "address",
    "at",
    "cap",
    "province",
    "municipality",
    "addressDetails",
    "state",
    "municipalityDetails"
]


# ============================================================
# SESSIONE AWS
# ============================================================

session = boto3.Session(
    profile_name=AWS_PROFILE
)


credentials = session.get_credentials()

if credentials is None:
    raise RuntimeError(
        f"Impossibile ottenere le credenziali AWS "
        f"dal profilo '{AWS_PROFILE}'."
    )


# ============================================================
# CONFIGURAZIONE SPARK
# ============================================================

os.environ["SPARK_MEM"] = spark_config_dict.get(
    "memory",
    "24g"
)

spark_conf = SparkConf()

spark_conf.set(
    "spark.driver.memory",
    spark_config_dict.get(
        "driver.memory",
        "4g"
    )
)

spark_conf.set(
    "spark.executor.memory",
    spark_config_dict.get(
        "executor.memory",
        "5g"
    )
)

spark_conf.set(
    "spark.executor.cores",
    spark_config_dict.get(
        "executor.cores",
        "3"
    )
)

spark_conf.set(
    "spark.executor.instances",
    spark_config_dict.get(
        "executor.instances",
        "4"
    )
)

spark_conf.set(
    "spark.dynamicAllocation.enabled",
    spark_config_dict.get(
        "dynamicAllocation.enabled",
        "false"
    )
)

spark_conf.set(
    "spark.hadoop.fs.s3a.impl",
    "org.apache.hadoop.fs.s3a.S3AFileSystem"
)

spark_conf.set(
    "spark.hadoop.fs.s3a.path.style.access",
    "true"
)

spark_conf.set(
    "spark.jars.packages",
    "org.apache.hadoop:hadoop-aws:3.3.4"
)

spark_conf.set(
    "spark.hadoop.fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
)

spark_conf.set(
    "spark.hadoop.fs.s3a.secret.key",
    credentials.secret_key
)


spark_conf.set(
    "spark.hadoop.fs.s3a.access.key",
    credentials.access_key
)


spark_conf.set(
    "spark.hadoop.fs.s3a.session.token",
    credentials.token
)



# ============================================================
# CREAZIONE SPARK SESSION
# ============================================================

spark_session = (
    SparkSession.builder
    .appName(SPARK_APP_NAME)
    .master(
        spark_config_dict.get(
            "master",
            "local"
        )
    )
    .config(conf=spark_conf)
    .getOrCreate()
)

spark_session.sparkContext.setLogLevel(
    spark_config_dict.get(
        "logLevel",
        "WARN"
    )
)

unique_uuid: str = str(uuid.uuid4())


# ============================================================
# DYNAMODB
# ============================================================
dynamodb = session.resource(
    "dynamodb",
    region_name=DYNAMODB_REGION
)


table = dynamodb.Table(
    DYNAMODB_TABLE
)


# ============================================================
# LEVENSHTEIN
# ============================================================

def levenshtein_distance(s1: str, s2: str) -> int:
    """
    Calcola la distanza di Levenshtein tra due stringhe.
    """

    if s1 == s2:
        return 0

    if not s1:
        return len(s2)

    if not s2:
        return len(s1)

    # La seconda stringa viene mantenuta come la più corta
    # per ridurre l'utilizzo di memoria.
    if len(s1) < len(s2):
        s1, s2 = s2, s1

    previous_row = list(range(len(s2) + 1))

    for i, char_1 in enumerate(s1, start=1):

        current_row = [i]

        for j, char_2 in enumerate(s2, start=1):

            insertions = current_row[j - 1] + 1
            deletions = previous_row[j] + 1
            substitutions = (
                previous_row[j - 1]
                + (char_1 != char_2)
            )

            current_row.append(
                min(
                    insertions,
                    deletions,
                    substitutions
                )
            )

        previous_row = current_row

    return previous_row[-1]


# ============================================================
# NORMALIZZAZIONE
# ============================================================

def normalize_string(value) -> str:
    """
    Normalizza una stringa prima del confronto:

    - minuscolo
    - rimozione accenti
    - punteggiatura sostituita con spazio
    - spazi multipli rimossi
    """

    if value is None:
        return ""

    value = str(value).lower()

    value = unicodedata.normalize(
        "NFD",
        value
    )

    value = "".join(
        character
        for character in value
        if unicodedata.category(character) != "Mn"
    )

    value = re.sub(
        r"[^a-z0-9\s]",
        " ",
        value
    )

    value = re.sub(
        r"\s+",
        " ",
        value
    )

    return value.strip()


# ============================================================
# CONVERSIONE PHYSICAL ADDRESS -> STRINGA
# ============================================================

def physical_address_to_string(physical_address) -> str:
    """
    Trasforma l'intero physicalAddress in una stringa
    deterministica utilizzando tutti i campi configurati.

    Esempio:

    {
        "address": "VIA NICOLA SACCO 2",
        "at": None,
        "cap": "48015",
        "province": "RA",
        "municipality": "CERVIA",
        "addressDetails": "",
        "state": "ITALIA",
        "municipalityDetails": ""
    }

    diventa:

    VIA NICOLA SACCO 2 |  | 48015 | RA | CERVIA |  | ITALIA |
    """

    if not physical_address:
        return ""

    values = []

    for field in PHYSICAL_ADDRESS_FIELDS:

        value = physical_address.get(field)

        if value is None:
            value = ""

        elif isinstance(value, dict):

            value = " ".join(
                str(v)
                for v in value.values()
                if v is not None
            )

        elif isinstance(value, list):

            value = " ".join(
                str(v)
                for v in value
                if v is not None
            )

        values.append(str(value))

    return " | ".join(values)


# ============================================================
# RECUPERO RECORD DYNAMODB
# ============================================================

def get_attempt(iun: str, attempt: int):
    """
    Recupera il record DynamoDB relativo all'IUN e
    all'ATTEMPT specificato.

    ATTEMPT_0:
        hashKey = TIMELINE#<IUN>
        sortKey = PREPARE_ANALOG_DOMICILE.<IUN>.
                  RECINDEX_0.ATTEMPT_0

    ATTEMPT_1:
        hashKey = TIMELINE#<IUN>
        sortKey = PREPARE_ANALOG_DOMICILE.<IUN>.
                  RECINDEX_0.ATTEMPT_1
    """

    hash_key = f"TIMELINE#{iun}"

    sort_key = (
        f"SEND_ANALOG_DOMICILE."
        f"IUN_{iun}."
        f"RECINDEX_0."
        f"ATTEMPT_{attempt}"
    )


    try:

        response = table.get_item(
            Key={
                "hashKey": hash_key,
                "sortKey": sort_key
            }
        )

        return response.get("Item")

    except ClientError as error:

        print(
            f"ERRORE DynamoDB - "
            f"IUN={iun}, ATTEMPT_{attempt}: {error}"
        )

        return None


# ============================================================
# CONFRONTO PHYSICAL ADDRESS
# ============================================================

def compare_physical_addresses(
    physical_address_0,
    physical_address_1
):
    """
    Confronta tutti i valori dei due physicalAddress
    senza effettuare alcuna normalizzazione.

    Restituisce:
        - stringa originale ATTEMPT_0
        - stringa originale ATTEMPT_1
        - distanza Levenshtein
        - similarità percentuale
    """

    address_0 = physical_address_to_string(
        physical_address_0
    )

    address_1 = physical_address_to_string(
        physical_address_1
    )

    # Entrambi assenti/vuoti
    if not address_0 and not address_1:
        return (
            address_0,
            address_1,
            0,
            100.0
        )

    # Uno solo assente/vuoto
    if not address_0 or not address_1:

        distance = max(
            len(address_0),
            len(address_1)
        )

        return (
            address_0,
            address_1,
            distance,
            0.0
        )

    # Distanza di Levenshtein sulle stringhe originali
    distance = levenshtein_distance(
        address_0,
        address_1
    )

    max_length = max(
        len(address_0),
        len(address_1)
    )

    similarity = round(
        (1 - distance / max_length) * 100,
        2
    )

    return (
        address_0,
        address_1,
        distance,
        similarity
    )


# ============================================================
# MAIN
# ============================================================

try:

    # --------------------------------------------------------
    # LETTURA CSV INPUT
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("LETTURA FILE INPUT")
    print("=" * 70)

    print(
        f"File input: {INPUT_FILE}"
    )

    df_address: DataFrame = (
        spark_session.read
        .csv(
            INPUT_FILE,
            header=True,
            inferSchema=True,
            sep=";"
        )
    )

    print(
        f"Colonne disponibili: {df_address.columns}"
    )

    # --------------------------------------------------------
    # CONTROLLO COLONNA IUN
    # --------------------------------------------------------

    if IUN_COLUMN not in df_address.columns:

        raise ValueError(
            f"La colonna '{IUN_COLUMN}' non è presente "
            f"nel file {INPUT_FILE}. "
            f"Colonne disponibili: {df_address.columns}"
        )

    # --------------------------------------------------------
    # ESTRAZIONE IUN
    # --------------------------------------------------------

    iun_rows = (
        df_address
        .select(IUN_COLUMN)
        .where(
            f"{IUN_COLUMN} IS NOT NULL"
        )
        .collect()
    )

    iun_list = []

    for row in iun_rows:

        iun = row[IUN_COLUMN]

        if iun is not None:

            iun = str(iun).strip()

            if iun:
                iun_list.append(iun)

    # Rimuove duplicati mantenendo l'ordine originale
    iun_list = list(
        dict.fromkeys(iun_list)
    )

    print(
        f"Numero IUN da elaborare: {len(iun_list)}"
    )

    # --------------------------------------------------------
    # ELABORAZIONE DYNAMODB
    # --------------------------------------------------------

    results = []

    print()
    print("=" * 70)
    print("INIZIO ELABORAZIONE DYNAMODB")
    print("=" * 70)

    for index, iun in enumerate(
        iun_list,
        start=1
    ):
        """
        print()
        print(
            f"[{index}/{len(iun_list)}] IUN: {iun}"
        )
        """
        # ----------------------------------------------------
        # ATTEMPT 0
        # ----------------------------------------------------

        item_0 = get_attempt(
            iun,
            0
        )

        # ----------------------------------------------------
        # ATTEMPT 1
        # ----------------------------------------------------

        item_1 = get_attempt(
            iun,
            1
        )

        # ----------------------------------------------------
        # PHYSICAL ADDRESS
        # ----------------------------------------------------

        physical_address_0 = (
            item_0.get("physicalAddress")
            if item_0
            else None
        )

        physical_address_1 = (
            item_1.get("physicalAddress")
            if item_1
            else None
        )

        # ----------------------------------------------------
        # CONFRONTO
        # ----------------------------------------------------

        (
            address_0,
            address_1,
            distance,
            similarity
        ) = compare_physical_addresses(
            physical_address_0,
            physical_address_1
        )

        # ----------------------------------------------------
        # RISULTATO
        # ----------------------------------------------------

        result_row = {
            "iun": iun,

            "attempt_0_found": (
                "SI" if item_0 else "NO"
            ),

            "attempt_1_found": (
                "SI" if item_1 else "NO"
            ),

            "attempt_0_address": (
                address_0
            ),

            "attempt_1_address": (
                address_1
            ),

            "levenshtein_distance": (
                distance
            ),

            "similarity_percentage": (
                similarity
            )
        }

        results.append(
            result_row
        )

        """
        print(
            f"    ATTEMPT_0: "
            f"{result_row['attempt_0_found']}"
        )

        print(
            f"    ATTEMPT_1: "
            f"{result_row['attempt_1_found']}"
        )

        print(
            f"    Levenshtein: {distance}"
        )

        print(
            f"    Similarità: {similarity}%"
        )
        """
    # --------------------------------------------------------
    # DATAFRAME RISULTATO
    # --------------------------------------------------------

    if results:

        result = spark_session.createDataFrame(
            results
        )



        # ----------------------------------------------------
        # SALVATAGGIO
        # ----------------------------------------------------

        output_path: str = os.path.join(
            output_config_dict.get("path"),
            unique_uuid,
            "Iun_Indirizzi_confronto"
        ) + ".csv"

        dataframe_writer: DataFrameWriter = (
            result
            .repartition(1)
            .write
            .mode(saveMode="overwrite")
        )

        dataframe_writer.options(
            header="True",
            delimiter=";"
        ).csv(output_path)

        print()
        print("=" * 70)
        print("ELABORAZIONE COMPLETATA")
        print("=" * 70)

        print(
            f"IUN elaborati: {len(iun_list)}"
        )

        print(
            f"Output: {output_path}"
        )

    else:

        print(
            "Nessun IUN trovato nel file di input."
        )

finally:

    # --------------------------------------------------------
    # CHIUSURA SPARK
    # --------------------------------------------------------

    spark_session.stop()
