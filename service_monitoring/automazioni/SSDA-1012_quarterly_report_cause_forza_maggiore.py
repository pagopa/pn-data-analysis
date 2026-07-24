import json
import logging
import os
import urllib.request
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import gspread
import numpy as np
import pandas as pd
from google.oauth2 import service_account
from pdnd_google_utils import Sheet
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

# ---------------- CONFIGURAZIONE BASE ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

APP_NAME = "SSDA-1012 Report trimestrale cause di forza maggiore"
GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"
LOCAL_TIMEZONE = ZoneInfo("Europe/Rome")

# ---------------- GOOGLE SHEET ----------------
# Spreadsheet di output. A ogni trimestre viene creato o sovrascritto un tab
# con nome nel formato "YYYY QN", ad esempio "2026 Q4".
SHEET_ID = "1WhlR2VWUfyCEkHOfftEEmXah2h6XYwG8Xn7Nr67_AF4"
SHEET_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit"
TAB_LOG = "Log controllo"

# Spreadsheet utilizzato come input per le autorizzazioni.
SHEET_ID_AUTORIZZAZIONI = "1PMPc1RyaVuZ0hk-J-m-stWxjQlSET805GJhpR4ujEM0"
TAB_AUTORIZZAZIONI = "Foglio1"

GSHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
INITIAL_WS_ROWS = 2
SHEETS_CELL_LIMIT = 10_000_000

# ---------------- SLACK TECNICO ----------------
SLACK_CONFIG_CANDIDATE_PATHS = [
    "/Slack/notifications_webhook.txt",
    "/app/mount/Slack/notifications_webhook.txt",
    "/app/mount/notifications_webhook.txt",
]

SLACK_WEBHOOK_NAME_TECNICO = "webhook_1"
SLACK_LABEL = APP_NAME

# Chiave logica usata per eliminare eventuali duplicati interni.
EVENT_KEY_COLUMNS = [
    "requestid",
    "causa_forza_maggiore_causale",
    "causa_forza_maggiore_data_rendicontazione",
]

DETAIL_COLUMNS = [
    "requestid",
    "codice_oggetto",
    "cap",
    "provincia",
    "regione",
    "prodotto",
    "recapitista",
    "causa_forza_maggiore_causale",
    "causa_forza_maggiore_descrizione",
    "causa_forza_maggiore_data",
    "causa_forza_maggiore_data_rendicontazione",
    "flag_autorizzazione",
    "data_autorizzazione",
    "motivo autorizzazione",
]


# ---------------- ECCEZIONI ----------------
class CapacityLimitError(RuntimeError):
    """Errore fatale: l'export supera il limite strutturale di Google Sheets."""


# ---------------- FUNZIONI TEMPO ----------------
def now_utc_dt() -> datetime:
    return datetime.now(timezone.utc)


def format_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def format_date(value: date) -> str:
    return value.strftime("%Y-%m-%d")


def elapsed_minutes_str(start_dt: datetime, end_dt: datetime | None = None) -> str:
    if end_dt is None:
        end_dt = now_utc_dt()
    elapsed_seconds = (end_dt - start_dt).total_seconds()
    return f"{elapsed_seconds / 60:.2f}"


def get_previous_quarter(reference_date: date) -> tuple[date, date, str]:
    """
    Restituisce inizio, fine e nome del trimestre precedente rispetto alla data
    di esecuzione.

    Esempio: run il 02/01/2027 -> 01/10/2026, 31/12/2026, "2026 Q4".
    """
    current_quarter = ((reference_date.month - 1) // 3) + 1

    if current_quarter == 1:
        reference_year = reference_date.year - 1
        previous_quarter = 4
    else:
        reference_year = reference_date.year
        previous_quarter = current_quarter - 1

    start_month = ((previous_quarter - 1) * 3) + 1
    quarter_start = date(reference_year, start_month, 1)

    if previous_quarter == 4:
        next_quarter_start = date(reference_year + 1, 1, 1)
    else:
        next_quarter_start = date(reference_year, start_month + 3, 1)

    quarter_end = next_quarter_start - timedelta(days=1)
    tab_name = f"{reference_year} Q{previous_quarter}"

    return quarter_start, quarter_end, tab_name


# ---------------- CREDENZIALI GOOGLE ----------------
def load_google_credentials(secret_path: str) -> dict:
    if not os.path.isdir(secret_path):
        raise FileNotFoundError(
            f"Directory delle credenziali Google non trovata: {secret_path}"
        )

    creds = {
        filename: open(os.path.join(secret_path, filename), "r", encoding="utf-8")
        .read()
        .strip()
        for filename in os.listdir(secret_path)
        if os.path.isfile(os.path.join(secret_path, filename))
    }

    if not creds:
        raise RuntimeError(
            f"Nessuna credenziale Google trovata nella directory: {secret_path}"
        )

    logging.info("Credenziali Google caricate correttamente.")
    return creds


# ---------------- GOOGLE SHEET ----------------
def sanitize_for_sheets(df: pd.DataFrame, missing: str = "-") -> pd.DataFrame:
    clean_df = df.copy()
    clean_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    clean_df = clean_df.fillna(missing)
    clean_df = clean_df.astype(object).where(pd.notna(clean_df), missing)
    return clean_df.astype(str)


def get_sheet_client(creds: dict) -> Sheet:
    return Sheet(
        sheet_id=SHEET_ID,
        service_credentials=creds,
        id_mode="key",
    )


def get_authorizations_sheet_client(creds: dict) -> Sheet:
    return Sheet(
        sheet_id=SHEET_ID_AUTORIZZAZIONI,
        service_credentials=creds,
        id_mode="key",
    )


def get_gspread_client(creds: dict):
    credentials = service_account.Credentials.from_service_account_info(
        creds,
        scopes=GSHEETS_SCOPES,
    )
    return gspread.authorize(credentials)


def ensure_worksheet_exists(
    creds: dict,
    worksheet_name: str,
    rows: int = INITIAL_WS_ROWS,
    cols: int = 1,
) -> None:
    """Crea il tab se non esiste; se esiste non lo modifica."""
    gc = get_gspread_client(creds)
    spreadsheet = gc.open_by_key(SHEET_ID)

    try:
        spreadsheet.worksheet(worksheet_name)
        logging.info("Worksheet '%s' già presente.", worksheet_name)
    except gspread.WorksheetNotFound:
        logging.info(
            "Worksheet '%s' non presente. Creazione in corso...",
            worksheet_name,
        )
        spreadsheet.add_worksheet(
            title=worksheet_name,
            rows=max(rows, 1),
            cols=max(cols, 1),
        )
        logging.info("Worksheet '%s' creato correttamente.", worksheet_name)


def resize_worksheet_grid(
    creds: dict,
    worksheet_name: str,
    target_rows: int,
    target_cols: int,
) -> None:
    """Adegua la griglia del tab alle dimensioni dell'output."""
    target_rows = max(int(target_rows), 1)
    target_cols = max(int(target_cols), 1)

    would_allocate = target_rows * target_cols
    if would_allocate >= SHEETS_CELL_LIMIT:
        raise CapacityLimitError(
            f"Il tab '{worksheet_name}' richiederebbe {would_allocate} celle "
            f"({target_rows} righe x {target_cols} colonne), oltre il limite."
        )

    gc = get_gspread_client(creds)
    spreadsheet = gc.open_by_key(SHEET_ID)
    worksheet = spreadsheet.worksheet(worksheet_name)

    if (
        int(worksheet.row_count) != target_rows
        or int(worksheet.col_count) != target_cols
    ):
        worksheet.resize(rows=target_rows, cols=target_cols)
        logging.info(
            "Griglia del tab '%s' ridimensionata: righe=%s, colonne=%s.",
            worksheet_name,
            target_rows,
            target_cols,
        )


def format_header_row(creds: dict, worksheet_name: str) -> None:
    """Formatta la riga di intestazione secondo lo standard già utilizzato."""
    gc = get_gspread_client(creds)
    spreadsheet = gc.open_by_key(SHEET_ID)
    worksheet = spreadsheet.worksheet(worksheet_name)

    worksheet.format(
        "1:1",
        {
            "backgroundColor": {
                "red": 0.8509803922,
                "green": 0.8235294118,
                "blue": 0.9137254902,
            },
            "textFormat": {
                "fontFamily": "Arial",
                "fontSize": 10,
                "bold": True,
                "italic": True,
                "foregroundColor": {
                    "red": 0,
                    "green": 0,
                    "blue": 0,
                },
            },
        },
    )

    logging.info("Header formattato per il tab '%s'.", worksheet_name)


def export_to_sheets(
    df: pd.DataFrame,
    creds: dict,
    sheet_name: str,
) -> None:
    """
    Scrive il dettaglio nel tab trimestrale.

    Se il tab esiste già viene sovrascritto; se il trimestre cambia, il diverso
    nome del tab determina la creazione di un nuovo worksheet.
    """
    clean_df = sanitize_for_sheets(df)
    target_rows = max(len(clean_df) + 1, INITIAL_WS_ROWS)
    target_cols = max(len(clean_df.columns), 1)

    ensure_worksheet_exists(
        creds=creds,
        worksheet_name=sheet_name,
        rows=target_rows,
        cols=target_cols,
    )
    resize_worksheet_grid(
        creds=creds,
        worksheet_name=sheet_name,
        target_rows=target_rows,
        target_cols=target_cols,
    )

    logging.info("Sovrascrittura del tab Google Sheet: %s", sheet_name)
    get_sheet_client(creds).upload(sheet_name, clean_df)
    format_header_row(creds, sheet_name)
    logging.info(
        "Tab '%s' aggiornato correttamente. Righe: %s - Colonne: %s",
        sheet_name,
        len(clean_df),
        len(clean_df.columns),
    )


def read_authorizations_sheet(creds: dict) -> pd.DataFrame:
    """Legge il tab contenente le cause di forza maggiore autorizzate."""
    logging.info(
        "Lettura del tab autorizzazioni Google Sheet: %s",
        TAB_AUTORIZZAZIONI,
    )
    downloaded = get_authorizations_sheet_client(creds).download(TAB_AUTORIZZAZIONI)

    if downloaded is None:
        authorizations_df = pd.DataFrame()
    elif isinstance(downloaded, pd.DataFrame):
        authorizations_df = downloaded.copy()
    else:
        authorizations_df = pd.DataFrame(downloaded)

    required_columns = [
        "requestid",
        "data_autorizzazione",
        "motivo autorizzazione causa forza maggiore",
    ]
    missing_columns = [
        column for column in required_columns if column not in authorizations_df.columns
    ]
    if missing_columns:
        raise ValueError(
            "Nel Google Sheet delle autorizzazioni mancano le colonne: "
            + ", ".join(missing_columns)
        )

    authorizations_df = authorizations_df[required_columns].copy()
    authorizations_df.rename(
        columns={
            "motivo autorizzazione causa forza maggiore": ("motivo autorizzazione")
        },
        inplace=True,
    )

    authorizations_df["requestid"] = (
        authorizations_df["requestid"].fillna("").astype(str).str.strip()
    )
    authorizations_df = authorizations_df.loc[
        authorizations_df["requestid"] != ""
    ].copy()

    # Evita che eventuali requestid duplicati nel foglio moltiplichino le righe.
    authorizations_df = authorizations_df.drop_duplicates(
        subset=["requestid"],
        keep="last",
    )

    logging.info(
        "Autorizzazioni caricate: %s requestid distinti.",
        len(authorizations_df),
    )
    return authorizations_df


def add_authorization_fields(
    df: pd.DataFrame,
    authorizations_df: pd.DataFrame,
) -> pd.DataFrame:
    """Aggiunge flag, data e motivo di autorizzazione tramite requestid."""
    enriched_df = df.copy()
    enriched_df["_requestid_join"] = (
        enriched_df["requestid"].fillna("").astype(str).str.strip()
    )

    authorizations_for_join = authorizations_df.copy()
    authorizations_for_join["_requestid_join"] = (
        authorizations_for_join["requestid"].fillna("").astype(str).str.strip()
    )
    authorizations_for_join["flag_autorizzazione"] = 1
    authorizations_for_join = authorizations_for_join[
        [
            "_requestid_join",
            "flag_autorizzazione",
            "data_autorizzazione",
            "motivo autorizzazione",
        ]
    ]

    enriched_df = enriched_df.merge(
        authorizations_for_join,
        on="_requestid_join",
        how="left",
    )
    enriched_df["flag_autorizzazione"] = (
        enriched_df["flag_autorizzazione"].fillna(0).astype(int)
    )
    enriched_df.drop(columns=["_requestid_join"], inplace=True)

    return enriched_df


def prepare_detail_for_sheet(df: pd.DataFrame) -> pd.DataFrame:
    detail_df = df.copy()

    for column in DETAIL_COLUMNS:
        if column not in detail_df.columns:
            detail_df[column] = None

    detail_df = detail_df[DETAIL_COLUMNS]

    # Evita interpretazioni numeriche o scientifiche del codice oggetto e del CAP.
    for column in ["codice_oggetto", "cap"]:
        detail_df[column] = detail_df[column].apply(
            lambda value: (
                ""
                if pd.isna(value) or str(value).strip() == ""
                else (str(value) if str(value).startswith("'") else f"'{value}")
            )
        )

    # Per gli oggetti non autorizzati data e motivo restano vuoti.
    for column in ["data_autorizzazione", "motivo autorizzazione"]:
        detail_df[column] = detail_df[column].where(
            pd.notna(detail_df[column]),
            "",
        )

    for column in [
        "causa_forza_maggiore_data",
        "causa_forza_maggiore_data_rendicontazione",
    ]:
        parsed = pd.to_datetime(detail_df[column], errors="coerce")
        detail_df[column] = parsed.dt.strftime("%Y-%m-%d %H:%M:%S")

    return detail_df


# ---------------- LOG DI CONTROLLO ----------------
def build_log_df(
    report_tab_name: str,
    data_esecuzione_job: str,
    stato_esecuzione: str,
    data_inizio_trimestre: str,
    data_fine_trimestre: str,
    max_requesttimestamp_gold: str,
    n_oggetti_estratti: int | str,
    n_oggetti_non_autorizzati: int | str,
    durata_job: str,
    messaggio_errore: str = "-",
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Tab report": [report_tab_name],
            "data_esecuzione_job": [data_esecuzione_job],
            "stato_esecuzione": [stato_esecuzione],
            "data_inizio_trimestre": [data_inizio_trimestre],
            "data_fine_trimestre": [data_fine_trimestre],
            "max_requesttimestamp_gold": [max_requesttimestamp_gold],
            "n_oggetti_estratti": [n_oggetti_estratti],
            "n_oggetti_non_autorizzati": [n_oggetti_non_autorizzati],
            "durata_job": [durata_job],
            "messaggio_errore": [messaggio_errore],
        }
    )


def upsert_log_to_sheets(log_df: pd.DataFrame, creds: dict) -> None:
    """
    Mantiene una riga per ciascun tab trimestrale:
    - a parità di trimestre, la riga viene sovrascritta;
    - per un nuovo trimestre, viene aggiunta una nuova riga;
    - se il tab di log è nuovo o vuoto, crea header e prima riga.
    """
    clean_log_df = sanitize_for_sheets(log_df)

    ensure_worksheet_exists(
        creds=creds,
        worksheet_name=TAB_LOG,
        rows=INITIAL_WS_ROWS,
        cols=max(len(clean_log_df.columns), 1),
    )

    gc = get_gspread_client(creds)
    spreadsheet = gc.open_by_key(SHEET_ID)
    worksheet = spreadsheet.worksheet(TAB_LOG)

    expected_header = list(clean_log_df.columns)
    new_row = clean_log_df.iloc[0].tolist()
    existing_values = worksheet.get_all_values()

    # Un tab appena creato viene inizializzato con header e prima riga.
    header_missing = (
        not existing_values
        or not existing_values[0]
        or not any(str(value).strip() for value in existing_values[0])
    )

    if header_missing:
        logging.info(
            "Tab log '%s' nuovo o vuoto: scrittura header e prima riga.",
            TAB_LOG,
        )

        resize_worksheet_grid(
            creds=creds,
            worksheet_name=TAB_LOG,
            target_rows=INITIAL_WS_ROWS,
            target_cols=len(expected_header),
        )

        end_col = gspread.utils.rowcol_to_a1(
            1,
            len(expected_header),
        ).replace("1", "")

        worksheet.update(
            f"A1:{end_col}2",
            [
                expected_header,
                new_row,
            ],
        )

        format_header_row(creds, TAB_LOG)

        logging.info(
            "Tab log '%s' inizializzato correttamente.",
            TAB_LOG,
        )
        return

    current_header = existing_values[0]

    if current_header != expected_header:
        raise RuntimeError(
            f"Header del tab '{TAB_LOG}' non compatibile con lo schema atteso. "
            f"Attuale={current_header} | Atteso={expected_header}"
        )

    report_tab_value = str(clean_log_df.iloc[0]["Tab report"])
    target_row_number = None

    for row_number, row in enumerate(
        existing_values[1:],
        start=2,
    ):
        if row and row[0] == report_tab_value:
            target_row_number = row_number
            break

    required_rows = max(
        len(existing_values) + (0 if target_row_number is not None else 1),
        INITIAL_WS_ROWS,
    )

    resize_worksheet_grid(
        creds=creds,
        worksheet_name=TAB_LOG,
        target_rows=required_rows,
        target_cols=len(expected_header),
    )

    if target_row_number is not None:
        logging.info(
            "Riga di log già presente per il tab '%s': "
            "sovrascrittura della riga %s.",
            report_tab_value,
            target_row_number,
        )

        end_col = gspread.utils.rowcol_to_a1(
            1,
            len(expected_header),
        ).replace("1", "")

        worksheet.update(
            f"A{target_row_number}:{end_col}{target_row_number}",
            [new_row],
        )
    else:
        logging.info(
            "Nuovo trimestre '%s': aggiunta di una riga al log.",
            report_tab_value,
        )
        worksheet.append_rows([new_row])

    format_header_row(creds, TAB_LOG)

    logging.info(
        "Log trimestrale aggiornato per il tab '%s'.",
        report_tab_value,
    )


# ---------------- SLACK TECNICO ----------------
def load_slack_webhooks(config_paths: list[str]) -> dict[str, str]:
    for config_path in config_paths:
        try:
            if not os.path.isfile(config_path):
                continue

            webhooks: dict[str, str] = {}
            with open(config_path, "r", encoding="utf-8") as config_file:
                for raw_line in config_file:
                    line = raw_line.strip()
                    if not line or line.startswith("#"):
                        continue

                    if "=" not in line:
                        logging.warning(
                            "Riga non valida nel file webhook '%s': %s",
                            config_path,
                            line,
                        )
                        continue

                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()

                    if key and value:
                        webhooks[key] = value

            if webhooks:
                logging.info(
                    "Configurazione webhook caricata correttamente da %s.",
                    config_path,
                )
                return webhooks

        except Exception as exc:
            logging.warning(
                "Errore nella lettura del file webhook '%s': %s",
                config_path,
                exc,
            )

    logging.warning("Nessun file webhook valido trovato.")
    return {}


def get_slack_webhook_by_name(
    webhooks: dict[str, str],
    webhook_name: str,
) -> str | None:
    webhook_url = webhooks.get(webhook_name)
    if not webhook_url:
        logging.warning("Webhook '%s' non trovato.", webhook_name)
    return webhook_url


def send_slack(webhook_url: str | None, message: str) -> None:
    if not webhook_url:
        raise RuntimeError("Webhook Slack tecnico non valorizzato.")

    request = urllib.request.Request(
        webhook_url,
        data=json.dumps({"text": message}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=20) as response:
        response.read()


def build_slack_prefix() -> str:
    prefix = "*CDE Job Alert*"
    if SLACK_LABEL:
        prefix += f" ({SLACK_LABEL})"
    return prefix


# ---------------- QUERY ----------------
def build_query_sql(data_inizio: date, data_fine: date) -> str:
    data_inizio_sql = format_date(data_inizio)
    data_fine_sql = format_date(data_fine)

    return f"""
WITH base AS(
    SELECT
        requestid,
        CAST(codice_oggetto AS STRING) AS codice_oggetto,
        CAST(geokey AS STRING) AS cap,
        province_name AS provincia,
        region_name AS regione,
        prodotto,
        -- Il recapitista viene normalizzato dal prefisso del codice oggetto
        CASE
            WHEN codice_oggetto LIKE 'R14%' THEN 'Fulmine'
            WHEN codice_oggetto LIKE '777%'
                OR codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
            WHEN codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
            WHEN codice_oggetto LIKE '69%'
                OR codice_oggetto LIKE '381%'
                OR codice_oggetto LIKE 'RB1%' THEN 'Poste'
            ELSE COALESCE(
                recapitista_unificato,
                recapitista,
                'NON_VALORIZZATO'
            )
        END AS recapitista_unif,
        TRIM(causa_forza_maggiore_dettagli) AS causa_forza_maggiore_causale,
        CASE TRIM(causa_forza_maggiore_dettagli)
            WHEN 'C01' THEN 'Incendio'
            WHEN 'C02' THEN 'Strada chiusa per lavori in corso o frana'
            WHEN 'C03' THEN 'Strada chiusa dalle autorità per eventi eccezionali'
            WHEN 'C04' THEN 'Maltempo: Alluvione, Neve, Allagamento'
            WHEN 'C05' THEN 'Terremoto'
            WHEN 'C06' THEN 'Eruzione vulcanica'
            ELSE 'Causale non rendicontata'
        END AS causa_forza_maggiore_descrizione,
        causa_forza_maggiore_data,
        causa_forza_maggiore_data_rendicontazione
    FROM send.gold_postalizzazione_analytics
    WHERE causa_forza_maggiore_data_rendicontazione IS NOT NULL
    AND TO_DATE(causa_forza_maggiore_data_rendicontazione)
        BETWEEN TO_DATE('{data_inizio_sql}') AND TO_DATE('{data_fine_sql}')
)
SELECT
    requestid,
    codice_oggetto,
    cap,
    provincia,
    regione,
    prodotto,
    recapitista_unif AS recapitista,
    causa_forza_maggiore_causale,
    causa_forza_maggiore_descrizione,
    causa_forza_maggiore_data,
    causa_forza_maggiore_data_rendicontazione
FROM base
ORDER BY
    causa_forza_maggiore_data_rendicontazione DESC,
    requestid
"""


def build_max_requesttimestamp_query() -> str:
    return """
SELECT
    MAX(requesttimestamp) AS max_requesttimestamp_gold
FROM send.gold_postalizzazione_analytics
"""


def run_query(spark: SparkSession, data_inizio: date, data_fine: date):
    logging.info(
        "Avvio query trimestrale cause di forza maggiore. Intervallo: %s - %s",
        data_inizio,
        data_fine,
    )
    return spark.sql(build_query_sql(data_inizio, data_fine))


def get_max_requesttimestamp_gold(spark: SparkSession) -> str:
    result = spark.sql(build_max_requesttimestamp_query()).first()
    value = result["max_requesttimestamp_gold"] if result else None
    return str(value) if value is not None else "-"


# ---------------- DEDUPLICA ----------------
def canonical_timestamp_series(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    formatted = parsed.dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    return formatted.fillna("")


def add_event_key(df: pd.DataFrame) -> pd.DataFrame:
    keyed_df = df.copy()

    for column in EVENT_KEY_COLUMNS:
        if column not in keyed_df.columns:
            keyed_df[column] = ""

    requestid = keyed_df["requestid"].fillna("").astype(str).str.strip()
    causale = (
        keyed_df["causa_forza_maggiore_causale"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )
    data_rendicontazione = canonical_timestamp_series(
        keyed_df["causa_forza_maggiore_data_rendicontazione"]
    )

    keyed_df["_chiave_evento"] = requestid + "|" + causale + "|" + data_rendicontazione
    return keyed_df


def normalize_current_extraction(df: pd.DataFrame) -> pd.DataFrame:
    normalized_df = add_event_key(df)

    duplicate_count = normalized_df.duplicated(
        subset=["_chiave_evento"],
        keep="first",
    ).sum()
    if duplicate_count:
        logging.warning(
            "Rilevati %s duplicati interni sulla chiave evento; saranno rimossi.",
            duplicate_count,
        )

    return normalized_df.drop_duplicates(
        subset=["_chiave_evento"],
        keep="first",
    ).reset_index(drop=True)


# ---------------- MAIN ----------------
def main() -> None:
    logging.info("Inizializzazione SparkSession...")
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "Europe/Rome")

    job_start_utc = now_utc_dt()
    job_start_local = job_start_utc.astimezone(LOCAL_TIMEZONE)
    job_start_local_str = format_dt(job_start_local)

    data_inizio_trimestre, data_fine_trimestre, report_tab_name = get_previous_quarter(
        job_start_local.date()
    )

    logging.info(
        "Trimestre di riferimento: %s - Intervallo: %s / %s",
        report_tab_name,
        data_inizio_trimestre,
        data_fine_trimestre,
    )

    slack_prefix = build_slack_prefix()
    slack_webhooks = load_slack_webhooks(SLACK_CONFIG_CANDIDATE_PATHS)
    technical_webhook_url = get_slack_webhook_by_name(
        slack_webhooks,
        SLACK_WEBHOOK_NAME_TECNICO,
    )

    creds: dict | None = None
    output_spark = None
    max_requesttimestamp_gold = "-"
    current_object_count = 0
    non_authorized_object_count = 0

    try:
        creds = load_google_credentials(GOOGLE_SECRET_PATH)

        upsert_log_to_sheets(
            build_log_df(
                report_tab_name=report_tab_name,
                data_esecuzione_job=job_start_local_str,
                stato_esecuzione="IN CORSO",
                data_inizio_trimestre=format_date(data_inizio_trimestre),
                data_fine_trimestre=format_date(data_fine_trimestre),
                max_requesttimestamp_gold="-",
                n_oggetti_estratti=0,
                n_oggetti_non_autorizzati=0,
                durata_job="0.00",
                messaggio_errore="-",
            ),
            creds,
        )

        authorizations_df = read_authorizations_sheet(creds)
        max_requesttimestamp_gold = get_max_requesttimestamp_gold(spark)

        query_start = now_utc_dt()
        output_spark = run_query(
            spark,
            data_inizio=data_inizio_trimestre,
            data_fine=data_fine_trimestre,
        ).persist(StorageLevel.MEMORY_AND_DISK)

        raw_row_count = output_spark.count()
        logging.info(
            "Query completata. Righe grezze: %s - Tempo query: %s min",
            raw_row_count,
            elapsed_minutes_str(query_start),
        )

        current_df = output_spark.toPandas()
        current_normalized_df = normalize_current_extraction(current_df)
        current_object_count = len(current_normalized_df)

        current_enriched_df = add_authorization_fields(
            df=current_normalized_df.drop(
                columns=["_chiave_evento"],
                errors="ignore",
            ),
            authorizations_df=authorizations_df,
        )
        non_authorized_object_count = int(
            (current_enriched_df["flag_autorizzazione"] == 0).sum()
        )

        detail_for_sheet = prepare_detail_for_sheet(current_enriched_df)
        export_to_sheets(
            df=detail_for_sheet,
            creds=creds,
            sheet_name=report_tab_name,
        )

        job_end_utc = now_utc_dt()
        job_end_local = job_end_utc.astimezone(LOCAL_TIMEZONE)
        job_end_local_str = format_dt(job_end_local)
        elapsed_minutes = elapsed_minutes_str(job_start_utc, job_end_utc)

        upsert_log_to_sheets(
            build_log_df(
                report_tab_name=report_tab_name,
                data_esecuzione_job=job_end_local_str,
                stato_esecuzione="OK",
                data_inizio_trimestre=format_date(data_inizio_trimestre),
                data_fine_trimestre=format_date(data_fine_trimestre),
                max_requesttimestamp_gold=max_requesttimestamp_gold,
                n_oggetti_estratti=current_object_count,
                n_oggetti_non_autorizzati=non_authorized_object_count,
                durata_job=elapsed_minutes,
                messaggio_errore="-",
            ),
            creds,
        )

        success_message = (
            f"{slack_prefix}\n"
            f"✅✅✅ *SUCCESS* ✅✅✅\n"
            f"*Job:* {APP_NAME}\n"
            f"*Data esecuzione:* {job_end_local_str}\n"
            f"*Trimestre elaborato:* {report_tab_name}\n"
            f"*Intervallo:* {format_date(data_inizio_trimestre)} → "
            f"{format_date(data_fine_trimestre)}\n"
            f"*Oggetti estratti:* {current_object_count}\n"
            f"*Oggetti non autorizzati:* {non_authorized_object_count}\n"
            f"*Google Sheet:* {SHEET_URL}\n"
        )

        try:
            send_slack(technical_webhook_url, success_message)
        except Exception:
            logging.error(
                "Invio della notifica Slack tecnica fallito.",
                exc_info=True,
            )

        logging.info("Processo trimestrale completato correttamente.")

    except Exception as exc:
        logging.error("Errore durante l'esecuzione del job.", exc_info=True)

        job_end_utc = now_utc_dt()
        job_end_local_str = format_dt(job_end_utc.astimezone(LOCAL_TIMEZONE))
        elapsed_minutes = elapsed_minutes_str(job_start_utc, job_end_utc)

        try:
            if creds is None:
                creds = load_google_credentials(GOOGLE_SECRET_PATH)

            upsert_log_to_sheets(
                build_log_df(
                    report_tab_name=report_tab_name,
                    data_esecuzione_job=job_end_local_str,
                    stato_esecuzione="KO",
                    data_inizio_trimestre=format_date(data_inizio_trimestre),
                    data_fine_trimestre=format_date(data_fine_trimestre),
                    max_requesttimestamp_gold=max_requesttimestamp_gold,
                    n_oggetti_estratti=current_object_count,
                    n_oggetti_non_autorizzati=non_authorized_object_count,
                    durata_job=elapsed_minutes,
                    messaggio_errore=(f"{type(exc).__name__}: {str(exc)[:500]}"),
                ),
                creds,
            )
        except Exception:
            logging.warning(
                "Aggiornamento del log trimestrale fallito durante la gestione "
                "dell'errore.",
                exc_info=True,
            )

        failure_message = (
            f"{slack_prefix}\n"
            f"❌❌❌ *FAILURE* ❌❌❌\n"
            f"*Job:* {APP_NAME}\n"
            f"*Data esecuzione:* {job_end_local_str}\n"
            f"*Trimestre previsto:* {report_tab_name}\n"
            f"*Google Sheet:* {SHEET_URL}\n"
            f"*Errore:* {type(exc).__name__}: {str(exc)[:500]}\n"
        )

        try:
            send_slack(technical_webhook_url, failure_message)
        except Exception:
            logging.error(
                "Invio della notifica Slack tecnica di errore fallito.",
                exc_info=True,
            )

        raise

    finally:
        try:
            if output_spark is not None:
                output_spark.unpersist()
                logging.info("DataFrame Spark rilasciato dalla cache.")
        except Exception:
            logging.warning("unpersist del DataFrame Spark fallito.", exc_info=True)

        try:
            spark.stop()
        except Exception:
            logging.warning("spark.stop() fallito in chiusura.", exc_info=True)


if __name__ == "__main__":
    main()
