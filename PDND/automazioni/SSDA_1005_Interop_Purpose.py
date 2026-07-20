
import logging
import os
from datetime import datetime, timezone

import gspread
import numpy as np
import pandas as pd
from google.oauth2 import service_account
from pdnd_google_utils import Sheet
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"


SHEET_ID = "1bJSQDwetlhmm_t_ti8G7T2j0W3hDNTno1IbwMvYvGt4"
TAB_OUTPUT = "Estrazione stato di carico dei servizi"
TAB_LOG = "Log di controllo"

INITIAL_WS_ROWS = 2
GSHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEETS_CELL_LIMIT = 10_000_000


class CapacityLimitError(RuntimeError):
    pass


def load_google_credentials(secret_path: str) -> dict:
    return {
        name: open(os.path.join(secret_path, name)).read().strip()
        for name in os.listdir(secret_path)
        if os.path.isfile(os.path.join(secret_path, name))
    }


def get_sheet_client(creds: dict) -> Sheet:
    return Sheet(
        sheet_id=SHEET_ID,
        service_credentials=creds,
        id_mode="key"
    )


def sanitize_for_sheets(df: pd.DataFrame, missing="-"):
    if df is None:
        return pd.DataFrame()

    df = df.copy()
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.fillna(missing, inplace=True)

    for c in df.columns:
        df[c] = df[c].astype(str)
        df.loc[df[c].isin(["nan", "None", "NaT"]), c] = missing

    return df


def resize_ws(creds, rows, cols):

    credentials = service_account.Credentials.from_service_account_info(
        creds,
        scopes=GSHEETS_SCOPES
    )

    gc = gspread.authorize(credentials)
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(TAB_OUTPUT)

    if rows * cols >= SHEETS_CELL_LIMIT:
        raise CapacityLimitError("Google Sheet supera il limite di celle")

    ws.resize(rows=max(rows, 1), cols=max(cols, 1))


def export_to_sheets(df, creds):

    df = sanitize_for_sheets(df)

    resize_ws(
        creds,
        len(df) + 1,
        max(len(df.columns), 1)
    )

    sheet = get_sheet_client(creds)
    sheet.upload(TAB_OUTPUT, df)

    return df


def spark_to_sheets(df_spark, creds):
    return export_to_sheets(df_spark.toPandas(), creds)


def run_query(spark):

    query = """
WITH purpose_versions AS (

    SELECT
        p.id AS purpose_id,
        v.id AS version_id,
        v.dailycalls AS daily_calls,
        v.state AS version_state,
        p.eserviceid AS eservice_id,
        p.title AS purpose_title,
        p.description AS purpose_description,
        p.consumerid AS consumer_id

    FROM interop.silver_purpose p
    LATERAL VIEW EXPLODE(p.versions) pv AS v

    WHERE UPPER(v.state) IN (
        'ACTIVE',
        'WAITINGFORAPPROVAL'
    )
),

eservice_descriptors AS (

    SELECT
        e.id AS eservice_id,
        e.name AS eservice_name,
        e.producerid AS producer_id,
        d.id AS descriptor_id,
        d.version AS descriptor_version,
        d.dailycallsperconsumer AS daily_calls_per_consumer,
        d.dailycallstotal AS daily_calls_total

    FROM interop.silver_eservice e
    LATERAL VIEW EXPLODE(e.descriptors) ed AS d
)

SELECT DISTINCT

    a.producerid AS producer_id,
    a.id AS agreement_id,
    pv.purpose_id,
    pv.version_state as state,
    pv.version_id,
    pv.daily_calls,
    pv.eservice_id,
    ed.eservice_name,
    ed.descriptor_id,
    ed.descriptor_version,
    ed.daily_calls_per_consumer,
    ed.daily_calls_total,
    a.consumerid AS consumer_id,
    st.name AS consumer_name

FROM purpose_versions pv

JOIN interop.silver_agreement a
    ON a.eserviceid = pv.eservice_id
   AND a.consumerid = pv.consumer_id

JOIN interop.silver_tenant st
    ON st.id = a.consumerid

JOIN eservice_descriptors ed
    ON ed.producer_id = a.producerid
   AND ed.eservice_id = a.eserviceid
   AND ed.descriptor_id = a.descriptorid

WHERE a.producerid IN (
    'bce8d16d-d26f-4c35-a835-35cca48ff8a5'
)

AND UPPER(a.state) IN (
    'ACTIVE',
    'SUSPENDED'
)

ORDER BY
    pv.eservice_id,
    ed.descriptor_id
"""
    return spark.sql(query)


def build_log_df(job_start, df):
    return pd.DataFrame({
        "Data esecuzione script start (UTC)": [job_start],
        "Numero righe estratte": [len(df)],
        "Numero colonne estratte": [len(df.columns)]
    })


def main():

    spark = SparkSession.builder \
        .appName("Interop_PurposeAgreement") \
        .getOrCreate()

    job_start = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    try:

        creds = load_google_credentials(GOOGLE_SECRET_PATH)

        out_df = spark_to_sheets(
            run_query(spark),
            creds
        )

        log_df = build_log_df(job_start, out_df)

        sheet = get_sheet_client(creds)
        sheet.upload(TAB_LOG, log_df)

        logging.info("Job completato.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
