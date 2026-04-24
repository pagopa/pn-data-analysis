import logging
import os
from datetime import datetime, timezone

import gspread
import numpy as np
import pandas as pd
from google.oauth2 import service_account
from pdnd_google_utils import Sheet
from pyspark.sql import SparkSession

# ---------------- CONFIGURAZIONE BASE ----------------

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"
SHEET_ID = "190bDkGcCgYLWWGra2lKUkF9vGYzdUo8zWuflnRueRSI"

TAB_LOG = "Log di controllo"
TAB_OUTPUT = "monthly_sender_taxid_mismatch"

INITIAL_WS_ROWS = 2
GSHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEETS_CELL_LIMIT = 10_000_000


# ---------------- ECCEZIONI ----------------


class CapacityLimitError(RuntimeError):
    """Errore fatale: l'export non è rappresentabile in Google Sheets entro i limiti strutturali."""


# ---------------- FUNZIONI CREDENZIALI ----------------


def load_google_credentials(secret_path: str) -> dict:
    """Legge i file di credenziali e li restituisce come dizionario."""
    creds = {
        name: open(os.path.join(secret_path, name)).read().strip()
        for name in os.listdir(secret_path)
        if os.path.isfile(os.path.join(secret_path, name))
    }
    logging.info(f"Credenziali caricate: {list(creds.keys())}")
    return creds


# ---------------- FUNZIONI ESPORTAZIONE ----------------


def get_sheet_client(creds: dict) -> Sheet:
    """Restituisce il client Sheet già configurato."""
    return Sheet(sheet_id=SHEET_ID, service_credentials=creds, id_mode="key")


def sanitize_for_sheets(df: pd.DataFrame, missing: str = "-") -> pd.DataFrame:
    """Sanitizza il DataFrame per la scrittura su Google Sheets."""
    if df is None:
        return pd.DataFrame()

    df = df.copy()
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df = df.fillna(missing)

    for col in df.columns:
        df[col] = df[col].astype(str)
        df.loc[df[col].isin(["nan", "None", "NaT"]), col] = missing

    return df


def _resize_worksheet_grid_best_effort(
    creds: dict,
    sheet_id: str,
    worksheet_name: str,
    target_rows: int | None,
    target_cols: int | None,
):
    """
    Best-effort resize della griglia del worksheet.
    - Se target_rows/target_cols è None, mantiene il valore corrente.
    - Fail-fast solo se la griglia risultante supera il limite celle.
    - Errori API/permessi: warning e si prosegue.
    """
    if target_rows is not None and target_rows < 1:
        target_rows = 1
    if target_cols is not None and target_cols < 1:
        target_cols = 1

    try:
        credentials = service_account.Credentials.from_service_account_info(
            creds, scopes=GSHEETS_SCOPES
        )
        gc = gspread.authorize(credentials)
        sh = gc.open_by_key(sheet_id)
        ws = sh.worksheet(worksheet_name)

        current_rows = int(ws.row_count)
        current_cols = int(ws.col_count)

        final_rows = int(target_rows) if target_rows is not None else current_rows
        final_cols = int(target_cols) if target_cols is not None else current_cols

        would_allocate = final_rows * final_cols
        if would_allocate >= SHEETS_CELL_LIMIT:
            raise CapacityLimitError(
                f"Resize worksheet='{worksheet_name}' su spreadsheet={sheet_id} "
                f"porterebbe la griglia a {would_allocate} celle "
                f"(rows={final_rows} * cols={final_cols}) >= {SHEETS_CELL_LIMIT}."
            )

        if final_rows == current_rows and final_cols == current_cols:
            logging.info(
                f"Resize non necessario per '{worksheet_name}': "
                f"rows={current_rows}, cols={current_cols}"
            )
            return

        logging.info(
            f"Resize griglia worksheet '{worksheet_name}': "
            f"rows {current_rows}->{final_rows}, cols {current_cols}->{final_cols}"
        )
        ws.resize(rows=final_rows, cols=final_cols)

    except CapacityLimitError:
        raise
    except Exception as e:
        logging.warning(
            f"Resize fallito per worksheet='{worksheet_name}' "
            f"(sheet_id={sheet_id}). Continuo senza resize. Dettaglio: {e}"
        )


def spark_to_sheets(
    df_spark, creds: dict, sheet_name: str, missing: str = "-"
) -> pd.DataFrame:
    """
    Pipeline completa Spark -> Google Sheets:
      1. Converte il DataFrame Spark in Pandas
      2. Delega a export_to_sheets la sanitizzazione, il resize e l'upload

    Restituisce il DataFrame Pandas sanitizzato e scritto su Google Sheets,
    utile per i log successivi.
    """
    if df_spark is None:
        logging.warning(f"DataFrame Spark nullo, skip scrittura su tab '{sheet_name}'.")
        return pd.DataFrame()

    logging.info(f"Conversione DataFrame Spark -> Pandas per tab '{sheet_name}'...")
    df = df_spark.toPandas()
    logging.info(f"Conversione completata. Righe convertite: {len(df)}")

    return export_to_sheets(df, creds, sheet_name, missing=missing)


def export_to_sheets(
    df: pd.DataFrame, creds: dict, sheet_name: str, missing: str = "-"
) -> pd.DataFrame:
    """
    Sanitizza, ridimensiona la griglia (best-effort) e scrive un DataFrame Pandas
    sul tab indicato.

    Restituisce il DataFrame Pandas sanitizzato e scritto, utile per eventuali
    log o passaggi successivi.
    """
    logging.info(f"Scrittura su Google Sheet: tab '{sheet_name}'...")

    df = sanitize_for_sheets(df, missing=missing)

    target_rows = max(len(df) + 1, INITIAL_WS_ROWS)  # +1 header
    target_cols = max(len(df.columns), 1)

    _resize_worksheet_grid_best_effort(
        creds=creds,
        sheet_id=SHEET_ID,
        worksheet_name=sheet_name,
        target_rows=target_rows,
        target_cols=target_cols,
    )

    sheet = get_sheet_client(creds)
    sheet.upload(sheet_name, df)

    logging.info(
        f"Scrittura completata su tab '{sheet_name}'. "
        f"Righe scritte: {len(df)} | Colonne: {len(df.columns)}"
    )

    return df


# ---------------- QUERY PRINCIPALE ----------------


def run_query(spark: SparkSession):
    """
    Estrae tutti i singoli taxId delle notifiche del mese precedente per cui
    non esiste una corrispondenza con il taxCode dell'ente attivo in SelfCare.
    """
    logging.info("Avvio query principale")

    query = """
    WITH institution_base AS (
        SELECT DISTINCT
            internalistitutionid,
            CASE
                WHEN subunittype IS NOT NULL THEN CONCAT(rootparent_description, ' - ', description)
                ELSE description
            END AS denomination,
            taxcode,
            updatedat
        FROM selfcare.gold_contracts
        WHERE product = 'prod-pn'
          AND state = 'ACTIVE'
          AND internalistitutionid IS NOT NULL
          AND taxcode IS NOT NULL
    ),
    institution AS (
        SELECT
            internalistitutionid,
            denomination,
            taxcode
        FROM (
            SELECT
                internalistitutionid,
                denomination,
                taxcode,
                ROW_NUMBER() OVER (
                    PARTITION BY internalistitutionid
                    ORDER BY updatedat DESC
                ) AS rn
            FROM institution_base
        ) t
        WHERE rn = 1
    ),
    base AS (
        SELECT
            n.iun,
            n.senderdenomination,
            n.sendertaxid,
            i.taxcode AS expected_taxcode,
            i.denomination AS expected_denomination,
            n.senderpaid,
            n.sentat,
            n.year,
            n.month
        FROM send.silver_notification n
        INNER JOIN institution i
            ON n.senderpaid = i.internalistitutionid
        WHERE n.year = CAST(YEAR(ADD_MONTHS(CURRENT_DATE(), -1)) AS STRING)
          AND n.month = LPAD(CAST(MONTH(ADD_MONTHS(CURRENT_DATE(), -1)) AS STRING), 2, '0')
          AND CAST(SUBSTR(n.sentat, 1, 10) AS DATE) >= ADD_MONTHS(TRUNC(CURRENT_DATE(), 'MONTH'), -1)
          AND CAST(SUBSTR(n.sentat, 1, 10) AS DATE) < TRUNC(CURRENT_DATE(), 'MONTH')
          AND n.sendertaxid <> i.taxcode
    )
    SELECT
        senderdenomination,
        sendertaxid,
        expected_taxcode,
        expected_denomination,
        senderpaid,
        year,
        month,
        COUNT(iun) AS iun_count
    FROM base
    GROUP BY
        senderdenomination,
        sendertaxid,
        expected_taxcode,
        expected_denomination,
        senderpaid,
        year,
        month
    ORDER BY
        sendertaxid,
        senderpaid
    """

    df = spark.sql(query)
    logging.info("Query principale eseguita correttamente.")
    return df


# ---------------- LOG DI CONTROLLO ----------------


def get_max_sentat(spark: SparkSession) -> str:
    """Recupera il MAX(sentat) da send.silver_notification."""
    logging.info("Recupero MAX(sentat) da send.silver_notification...")

    df = spark.sql(
        """
        SELECT MAX(sentat) AS max_sentat
        FROM send.silver_notification
        """
    )

    value = df.collect()[0]["max_sentat"]
    max_sentat = str(value) if value is not None else "-"

    logging.info(f"MAX(sentat) recuperato: {max_sentat}")
    return max_sentat


def build_log_df(
    max_sentat: str,
    job_start_ts_utc: str,
    estrazione_df: pd.DataFrame,
) -> pd.DataFrame:
    """Costruisce il tab di log con informazioni e metriche sintetiche."""

    total_rows = len(estrazione_df)
    total_columns = len(estrazione_df.columns)

    distinct_senderpaid = (
        estrazione_df["senderpaid"].nunique()
        if not estrazione_df.empty and "senderpaid" in estrazione_df.columns
        else 0
    )

    distinct_sendertaxid = (
        estrazione_df["sendertaxid"].nunique()
        if not estrazione_df.empty and "sendertaxid" in estrazione_df.columns
        else 0
    )

    return pd.DataFrame(
        {
            "Ultimo aggiornamento dati (MAX sentat)": [max_sentat],
            "Data esecuzione script start (UTC)": [job_start_ts_utc],
            "Numero righe estratte": [total_rows],
            "Numero colonne estratte": [total_columns],
            "Numero senderpaid distinti": [int(distinct_senderpaid)],
            "Numero sendertaxid distinti": [int(distinct_sendertaxid)],
        }
    )


# ---------------- MAIN ----------------


def main():
    logging.info("Inizializzazione SparkSession...")
    spark = SparkSession.builder.appName(
        "SSDA-814_Monthly_Report_Sender_TaxId_Mismatch"
    ).getOrCreate()

    job_start_ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f"Timestamp avvio job (UTC): {job_start_ts_utc}")

    try:
        creds = load_google_credentials(GOOGLE_SECRET_PATH)

        # 1) Query principale + conversione + sanitizzazione + export
        estrazione_df = spark_to_sheets(run_query(spark), creds, TAB_OUTPUT)

        logging.info(
            f"DataFrame estratto e scritto su Sheets."
            f"Righe: {len(estrazione_df)} | Colonne: {len(estrazione_df.columns)}"
        )

        # 2) Log di controllo
        max_sentat = get_max_sentat(spark)

        log_df = build_log_df(
            max_sentat=max_sentat,
            job_start_ts_utc=job_start_ts_utc,
            estrazione_df=estrazione_df,
        )

        # 3) Export log
        export_to_sheets(log_df, creds, TAB_LOG)

        logging.info("Processo completato con successo.")

    except Exception as e:
        logging.exception(f"Errore durante l'esecuzione del job: {e}")
        raise

    finally:
        spark.stop()
        logging.info("Spark session terminata.")


if __name__ == "__main__":
    main()
