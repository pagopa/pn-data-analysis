import json
import logging
import os
import urllib.request
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from pdnd_google_utils import Sheet
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

# ---------------- CONFIGURAZIONE BASE ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

APP_NAME = "SSDA-1002 Alert cause di forza maggiore"
GOOGLE_SECRET_PATH = "/etc/dex/secrets/secret-cde-googlesheet"
LOCAL_TIMEZONE = ZoneInfo("Europe/Rome")

# ---------------- GOOGLE SHEET ----------------
SHEET_ID = "1SuSz2aXhvll7xDt4ymuCIC3-S_xDbZ5jnMB1qkLf3ac"
SHEET_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit"
TAB_LOG = "Log controllo"
TAB_DETTAGLIO = "Dettaglio ultime cause forza maggiore"

SHEET_ID_AUTORIZZAZIONI = "1PMPc1RyaVuZ0hk-J-m-stWxjQlSET805GJhpR4ujEM0"
TAB_AUTORIZZAZIONI = "Foglio1"

# In assenza di una precedente esecuzione conclusa con successo, la prima run
# analizza gli ultimi N giorni.
GIORNI_RECUPERO_PRIMA_ESECUZIONE = 7

# ---------------- SLACK ----------------
SLACK_CONFIG_CANDIDATE_PATHS = [
    "/Slack/notifications_webhook.txt",
    "/app/mount/Slack/notifications_webhook.txt",
    "/app/mount/notifications_webhook.txt",
]

SLACK_WEBHOOK_NAME_TECNICO = "webhook_1"
SLACK_WEBHOOK_NAME_ALERT = "webhook_alert_forza_causa_maggiore"

# Nome del canale funzionale riportato nel tab di log.
SLACK_CANALE_ALERT = "ss-alert_causa_forza_maggiore"

SLACK_LABEL = APP_NAME

# Limite prudenziale per suddividere eventuali riepiloghi Slack molto estesi.
SLACK_MAX_MESSAGE_CHARS = 28000

# Chiave logica usata per confrontare l'estrazione corrente con il dettaglio
# dell'ultima esecuzione riuscita.
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


def parse_datetime_or_none(value: object) -> datetime | None:
    if value is None:
        return None

    value_str = str(value).strip()
    if not value_str or value_str in {"-", "None", "NaT", "nan"}:
        return None

    parsed = pd.to_datetime(value_str, errors="coerce")
    if pd.isna(parsed):
        return None

    parsed_dt = parsed.to_pydatetime()
    if parsed_dt.tzinfo is None:
        return parsed_dt.replace(tzinfo=LOCAL_TIMEZONE)
    return parsed_dt.astimezone(LOCAL_TIMEZONE)


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


def read_sheet_tab(
    creds: dict,
    sheet_name: str,
    allow_missing: bool = False,
) -> pd.DataFrame:
    """Legge un tab tramite pdnd_google_utils.Sheet.download()."""
    try:
        logging.info("Lettura del tab Google Sheet: %s", sheet_name)
        downloaded = get_sheet_client(creds).download(sheet_name)

        if downloaded is None:
            return pd.DataFrame()
        if isinstance(downloaded, pd.DataFrame):
            return downloaded.copy()
        return pd.DataFrame(downloaded)

    except Exception:
        if allow_missing:
            logging.warning(
                "Tab '%s' non disponibile: verrà trattato come vuoto.",
                sheet_name,
                exc_info=True,
            )
            return pd.DataFrame()
        raise


def get_authorizations_sheet_client(creds: dict) -> Sheet:
    return Sheet(
        sheet_id=SHEET_ID_AUTORIZZAZIONI,
        service_credentials=creds,
        id_mode="key",
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

    # Evita che eventuali requestid duplicati nel foglio moltiplichino le righe
    # del dettaglio durante la join.
    authorizations_df = authorizations_df.drop_duplicates(
        subset=["requestid"],
        keep="last",
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
    enriched_df.drop(
        columns=["_requestid_join"],
        inplace=True,
    )

    return enriched_df


def export_to_sheets(
    df: pd.DataFrame,
    creds: dict,
    sheet_name: str,
) -> None:
    logging.info("Sovrascrittura del tab Google Sheet: %s", sheet_name)
    get_sheet_client(creds).upload(sheet_name, sanitize_for_sheets(df))
    logging.info("Tab '%s' aggiornato correttamente.", sheet_name)


def get_last_successful_execution(log_df: pd.DataFrame) -> datetime | None:
    if log_df.empty or "data_ultima_esecuzione_buon_fine" not in log_df.columns:
        return None

    # Il tab di log viene sovrascritto e contiene una sola riga; max() mantiene
    # comunque il comportamento corretto qualora fossero presenti più righe.
    parsed_values = [
        parse_datetime_or_none(value)
        for value in log_df["data_ultima_esecuzione_buon_fine"].tolist()
    ]
    valid_values = [value for value in parsed_values if value is not None]
    return max(valid_values) if valid_values else None


def build_log_df(
    data_esecuzione_job: str,
    data_ultima_esecuzione_buon_fine: str,
    stato_esecuzione: str,
    data_inizio_intervallo: str,
    data_fine_intervallo: str,
    max_requesttimestamp_gold: str,
    n_oggetti_estrazione_corrente: int | str,
    n_oggetti_estrazione_precedente: int | str,
    n_oggetti_in_comune: int | str,
    n_nuovi_oggetti: int | str,
    n_oggetti_non_autorizzati: int | str,
    canale_slack_alert: str,
    esito_invio_alert: str,
    durata_job: str,
    messaggio_errore: str = "-",
) -> pd.DataFrame:
    """Il tab viene sovrascritto e contiene lo stato più recente del job."""
    return pd.DataFrame(
        {
            "data_esecuzione_job": [data_esecuzione_job],
            "data_ultima_esecuzione_buon_fine": [data_ultima_esecuzione_buon_fine],
            "stato_esecuzione": [stato_esecuzione],
            "data_inizio_intervallo": [data_inizio_intervallo],
            "data_fine_intervallo": [data_fine_intervallo],
            "max_requesttimestamp_gold": [max_requesttimestamp_gold],
            "n_oggetti_estrazione_corrente": [n_oggetti_estrazione_corrente],
            "n_oggetti_estrazione_precedente": [n_oggetti_estrazione_precedente],
            "n_oggetti_in_comune": [n_oggetti_in_comune],
            "n_nuovi_oggetti": [n_nuovi_oggetti],
            "n_oggetti_non_autorizzati": [n_oggetti_non_autorizzati],
            "canale_slack_alert": [canale_slack_alert],
            "esito_invio_alert": [esito_invio_alert],
            "durata_job": [durata_job],
            "messaggio_errore": [messaggio_errore],
        }
    )


def write_log_tab(creds: dict, **log_values) -> None:
    export_to_sheets(
        df=build_log_df(**log_values),
        creds=creds,
        sheet_name=TAB_LOG,
    )


def prepare_detail_for_sheet(df: pd.DataFrame) -> pd.DataFrame:
    detail_df = df.copy()

    for column in DETAIL_COLUMNS:
        if column not in detail_df.columns:
            detail_df[column] = None

    detail_df = detail_df[DETAIL_COLUMNS]

    # L'apostrofo viene applicato soltanto all'output Google Sheet per evitare
    # interpretazioni numeriche o scientifiche del codice oggetto e del CAP.
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


# ---------------- SLACK ----------------
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

            logging.warning(
                "File webhook trovato ma privo di configurazioni valide: %s",
                config_path,
            )
        except Exception as exc:
            logging.warning(
                "Errore nella lettura del file webhook '%s': %s",
                config_path,
                exc,
            )

    logging.warning(
        "Nessun file webhook valido trovato: le notifiche Slack saranno disabilitate."
    )
    return {}


def get_slack_webhook_by_name(
    webhooks: dict[str, str],
    webhook_name: str,
) -> str | None:
    webhook_url = webhooks.get(webhook_name)
    if not webhook_url:
        logging.warning(
            "Webhook '%s' non trovato: la relativa notifica Slack non sarà disponibile.",
            webhook_name,
        )
    return webhook_url


def send_slack(webhook_url: str | None, message: str) -> None:
    if not webhook_url:
        raise RuntimeError("Webhook Slack non valorizzato.")

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


def build_alert_messages(
    nuovi_eventi_df: pd.DataFrame,
    data_inizio: date,
    data_fine: date,
) -> list[str]:
    """
    Costruisce un riepilogo Slack aggregato per:
    - giorno di rendicontazione;
    - recapitista;
    - CAP;
    - provincia.

    Il dettaglio dei singoli oggetti resta disponibile nel Google Sheet.
    """
    if nuovi_eventi_df.empty:
        return []

    working_df = nuovi_eventi_df.copy()

    working_df["data_rendicontazione_giorno"] = pd.to_datetime(
        working_df["causa_forza_maggiore_data_rendicontazione"],
        errors="coerce",
    ).dt.date

    # Normalizzazione dei campi utilizzati nell'aggregazione.
    for column in ["recapitista", "cap", "provincia"]:
        working_df[column] = (
            working_df[column]
            .fillna("NON DISPONIBILE")
            .astype(str)
            .str.strip()
            .str.replace(r"[\r\n]+", " ", regex=True)
        )

        working_df.loc[
            working_df[column] == "",
            column,
        ] = "NON DISPONIBILE"

    riepilogo_df = (
        working_df.groupby(
            [
                "data_rendicontazione_giorno",
                "recapitista",
                "cap",
                "provincia",
            ],
            dropna=False,
        )
        .size()
        .reset_index(name="numero_oggetti")
    )

    # Colonna tecnica usata esclusivamente per ordinare le giornate.
    riepilogo_df["_data_ordinamento"] = pd.to_datetime(
        riepilogo_df["data_rendicontazione_giorno"],
        errors="coerce",
    )

    riepilogo_df = riepilogo_df.sort_values(
        by=[
            "_data_ordinamento",
            "recapitista",
            "cap",
            "provincia",
        ],
        ascending=[True, True, True, True],
        na_position="last",
    )

    numero_totale = len(working_df)
    etichetta_oggetti = "nuovo oggetto" if numero_totale == 1 else "nuovi oggetti"

    header = (
        "⚠️ *Nuove rendicontazioni di causa di forza maggiore*\n\n"
        f"Sono stati individuati *{numero_totale} {etichetta_oggetti}* "
        f"non ancora segnalati nell'intervallo dal "
        f"*{data_inizio.strftime('%d/%m/%Y')}* "
        f"al *{data_fine.strftime('%d/%m/%Y')}*. "
        f"Per il dettaglio consultare il seguente link:\n"
        f"{SHEET_URL}\n\n"
    )

    day_blocks: list[str] = []

    for day_value, day_df in riepilogo_df.groupby(
        "data_rendicontazione_giorno",
        dropna=False,
        sort=False,
    ):
        if pd.isna(day_value):
            giorno_label = "data non disponibile"
        else:
            giorno_label = day_value.strftime("%d/%m/%Y")

        totale_giorno = int(day_df["numero_oggetti"].sum())
        etichetta_rendicontati = (
            "rendicontato" if totale_giorno == 1 else "rendicontati"
        )

        # Lunghezze massime per l'allineamento nel blocco monospaziato.
        max_recapitista_len = max(len(str(value)) for value in day_df["recapitista"])
        max_cap_len = max(len(str(value)) for value in day_df["cap"])
        max_provincia_len = max(len(str(value)) for value in day_df["provincia"])
        max_numero_len = max(len(str(int(value))) for value in day_df["numero_oggetti"])

        block_lines = [
            f"• *{totale_giorno} {etichetta_rendicontati} " f"il {giorno_label}:*",
            "```",
        ]

        for row in day_df.itertuples(index=False):
            recapitista = str(row.recapitista)
            cap = str(row.cap)
            provincia = str(row.provincia)
            numero_oggetti = int(row.numero_oggetti)

            block_lines.append(
                f"{recapitista:<{max_recapitista_len}}"
                f" | "
                f"{cap:<{max_cap_len}}"
                f" | "
                f"{provincia:<{max_provincia_len}}"
                f" | "
                f"{numero_oggetti:>{max_numero_len}}"
            )

        block_lines.append("```")

        day_blocks.append("\n".join(block_lines) + "\n")

    messages: list[str] = []
    current_message = header

    for day_block in day_blocks:
        if (
            len(current_message) + len(day_block) > SLACK_MAX_MESSAGE_CHARS
            and current_message != header
        ):
            messages.append(current_message.rstrip())

            current_message = (
                "⚠️ *Segue riepilogo delle nuove "
                "cause di forza maggiore*\n\n" + day_block
            )
        else:
            current_message += day_block

    if current_message.strip():
        messages.append(current_message.rstrip())

    return messages


def send_slack_messages(webhook_url: str | None, messages: list[str]) -> None:
    for message in messages:
        send_slack(webhook_url, message)


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
        "Avvio query cause di forza maggiore. Intervallo: %s - %s",
        data_inizio,
        data_fine,
    )
    return spark.sql(build_query_sql(data_inizio, data_fine))


def get_max_requesttimestamp_gold(spark: SparkSession) -> str:
    result = spark.sql(build_max_requesttimestamp_query()).first()
    value = result["max_requesttimestamp_gold"] if result else None
    if value is None:
        return "-"
    return str(value)


# ---------------- CONFRONTO ESTRAZIONI ----------------
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


def compare_with_previous_extraction(
    current_df: pd.DataFrame,
    previous_df: pd.DataFrame,
) -> tuple[pd.DataFrame, int]:
    current_keyed = normalize_current_extraction(current_df)

    if previous_df.empty:
        new_df = current_keyed.copy()
        common_count = 0
    else:
        previous_keyed = add_event_key(previous_df)
        previous_keys = set(previous_keyed["_chiave_evento"].tolist())
        common_mask = current_keyed["_chiave_evento"].isin(previous_keys)
        common_count = int(common_mask.sum())
        new_df = current_keyed.loc[~common_mask].copy()

    return (
        new_df.drop(columns=["_chiave_evento"], errors="ignore").reset_index(drop=True),
        common_count,
    )


# ---------------- MAIN ----------------
def main() -> None:
    logging.info("Inizializzazione SparkSession...")
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "Europe/Rome")

    job_start_utc = now_utc_dt()
    job_start_local = job_start_utc.astimezone(LOCAL_TIMEZONE)
    job_start_local_str = format_dt(job_start_local)
    data_fine_intervallo = job_start_local.date()

    slack_prefix = build_slack_prefix()

    creds: dict | None = None
    output_spark = None
    previous_detail_df = pd.DataFrame(columns=DETAIL_COLUMNS)

    previous_success_dt: datetime | None = None
    previous_success_str = "-"
    data_inizio_intervallo = data_fine_intervallo - timedelta(
        days=GIORNI_RECUPERO_PRIMA_ESECUZIONE
    )

    max_requesttimestamp_gold = "-"
    raw_row_count = 0
    current_object_count = 0
    previous_object_count = 0
    common_object_count = 0
    new_object_count = 0
    non_authorized_object_count = 0
    alert_slack_status = "NON ESEGUITO"

    slack_webhooks = load_slack_webhooks(SLACK_CONFIG_CANDIDATE_PATHS)
    technical_webhook_url = get_slack_webhook_by_name(
        slack_webhooks,
        SLACK_WEBHOOK_NAME_TECNICO,
    )
    alert_webhook_url = get_slack_webhook_by_name(
        slack_webhooks,
        SLACK_WEBHOOK_NAME_ALERT,
    )

    try:
        creds = load_google_credentials(GOOGLE_SECRET_PATH)

        # La prima esecuzione può partire con i tab ancora vuoti; dopo che esiste
        # una data di buon fine, un errore nella lettura del dettaglio è bloccante.
        current_log_df = read_sheet_tab(
            creds=creds,
            sheet_name=TAB_LOG,
            allow_missing=True,
        )
        previous_success_dt = get_last_successful_execution(current_log_df)

        if previous_success_dt is not None:
            previous_success_str = format_dt(previous_success_dt)
            data_inizio_intervallo = previous_success_dt.date()

        previous_detail_df = read_sheet_tab(
            creds=creds,
            sheet_name=TAB_DETTAGLIO,
            allow_missing=previous_success_dt is None,
        )
        previous_object_count = len(previous_detail_df)

        # Il log IN CORSO conserva la data dell'ultima esecuzione conclusa con
        # successo; in caso di interruzione, il watermark non avanza.
        write_log_tab(
            creds=creds,
            data_esecuzione_job=job_start_local_str,
            data_ultima_esecuzione_buon_fine=previous_success_str,
            stato_esecuzione="IN CORSO",
            data_inizio_intervallo=format_date(data_inizio_intervallo),
            data_fine_intervallo=format_date(data_fine_intervallo),
            max_requesttimestamp_gold="-",
            n_oggetti_estrazione_corrente=0,
            n_oggetti_estrazione_precedente=previous_object_count,
            n_oggetti_in_comune=0,
            n_nuovi_oggetti=0,
            n_oggetti_non_autorizzati=0,
            canale_slack_alert=SLACK_CANALE_ALERT,
            esito_invio_alert="NON ESEGUITO",
            durata_job="0.00",
            messaggio_errore="-",
        )

        authorizations_df = read_authorizations_sheet(creds)

        max_requesttimestamp_gold = get_max_requesttimestamp_gold(spark)
        logging.info(
            "MAX(requesttimestamp) della GOLD: %s",
            max_requesttimestamp_gold,
        )

        query_start = now_utc_dt()
        output_spark = run_query(
            spark,
            data_inizio=data_inizio_intervallo,
            data_fine=data_fine_intervallo,
        ).persist(StorageLevel.MEMORY_AND_DISK)

        raw_row_count = output_spark.count()
        logging.info(
            "Query completata. Righe grezze estratte: %s - Tempo query: %s min",
            raw_row_count,
            elapsed_minutes_str(query_start),
        )

        current_df = output_spark.toPandas()
        current_normalized_df = normalize_current_extraction(current_df)
        current_object_count = len(current_normalized_df)

        new_events_df, common_object_count = compare_with_previous_extraction(
            current_df=current_normalized_df.drop(
                columns=["_chiave_evento"],
                errors="ignore",
            ),
            previous_df=previous_detail_df,
        )
        new_object_count = len(new_events_df)

        current_enriched_df = add_authorization_fields(
            df=current_normalized_df.drop(
                columns=["_chiave_evento"],
                errors="ignore",
            ),
            authorizations_df=authorizations_df,
        )
        new_events_enriched_df = add_authorization_fields(
            df=new_events_df,
            authorizations_df=authorizations_df,
        )
        non_authorized_object_count = int(
            (current_enriched_df["flag_autorizzazione"] == 0).sum()
        )

        logging.info(
            "Confronto completato. Correnti: %s - Precedenti: %s - In comune: %s - Nuovi: %s",
            current_object_count,
            previous_object_count,
            common_object_count,
            new_object_count,
        )

        # L'alert funzionale parte soltanto se esistono nuovi eventi.
        if new_object_count > 0:
            alert_messages = build_alert_messages(
                nuovi_eventi_df=new_events_enriched_df,
                data_inizio=data_inizio_intervallo,
                data_fine=data_fine_intervallo,
            )
            send_slack_messages(alert_webhook_url, alert_messages)
            alert_slack_status = "OK"
            logging.info(
                "Alert funzionale inviato sul canale %s.",
                SLACK_CANALE_ALERT,
            )
        else:
            alert_slack_status = "NON NECESSARIO"
            logging.info("Nessun nuovo evento: alert funzionale non inviato.")

        # Il dettaglio viene sovrascritto soltanto dopo l'eventuale invio Slack
        # riuscito e contiene l'intera estrazione corrente. In questo modo,
        # eventuali rerun nella stessa giornata non ripresentano oggetti già segnalati.
        detail_for_sheet = prepare_detail_for_sheet(current_enriched_df)
        export_to_sheets(
            df=detail_for_sheet,
            creds=creds,
            sheet_name=TAB_DETTAGLIO,
        )

        job_end_utc = now_utc_dt()
        job_end_local = job_end_utc.astimezone(LOCAL_TIMEZONE)
        job_end_local_str = format_dt(job_end_local)
        elapsed_minutes = elapsed_minutes_str(job_start_utc, job_end_utc)

        # Registrazione del buon fine del processo dati.
        write_log_tab(
            creds=creds,
            data_esecuzione_job=job_end_local_str,
            data_ultima_esecuzione_buon_fine=job_end_local_str,
            stato_esecuzione="OK",
            data_inizio_intervallo=format_date(data_inizio_intervallo),
            data_fine_intervallo=format_date(data_fine_intervallo),
            max_requesttimestamp_gold=max_requesttimestamp_gold,
            n_oggetti_estrazione_corrente=current_object_count,
            n_oggetti_estrazione_precedente=previous_object_count,
            n_oggetti_in_comune=common_object_count,
            n_nuovi_oggetti=new_object_count,
            n_oggetti_non_autorizzati=non_authorized_object_count,
            canale_slack_alert=SLACK_CANALE_ALERT,
            esito_invio_alert=alert_slack_status,
            durata_job=elapsed_minutes,
            messaggio_errore="-",
        )

        success_message = (
            f"{slack_prefix}\n"
            f"✅✅✅ *SUCCESS* ✅✅✅\n"
            f"*Job:* {APP_NAME}\n"
            f"*Data esecuzione:* {job_end_local_str}\n"
            f"*Google Sheet:* {SHEET_URL}\n"
        )

        try:
            send_slack(technical_webhook_url, success_message)
        except Exception:
            logging.error(
                "Invio della notifica Slack tecnica fallito.",
                exc_info=True,
            )

        logging.info("Processo completato correttamente.")

    except Exception as exc:
        logging.error("Errore durante l'esecuzione del job.", exc_info=True)

        job_end_utc = now_utc_dt()
        job_end_local_str = format_dt(job_end_utc.astimezone(LOCAL_TIMEZONE))
        elapsed_minutes = elapsed_minutes_str(job_start_utc, job_end_utc)

        try:
            if creds is None:
                creds = load_google_credentials(GOOGLE_SECRET_PATH)

            write_log_tab(
                creds=creds,
                data_esecuzione_job=job_end_local_str,
                data_ultima_esecuzione_buon_fine=previous_success_str,
                stato_esecuzione="KO",
                data_inizio_intervallo=format_date(data_inizio_intervallo),
                data_fine_intervallo=format_date(data_fine_intervallo),
                max_requesttimestamp_gold=max_requesttimestamp_gold,
                n_oggetti_estrazione_corrente=current_object_count,
                n_oggetti_estrazione_precedente=previous_object_count,
                n_oggetti_in_comune=common_object_count,
                n_nuovi_oggetti=new_object_count,
                n_oggetti_non_autorizzati=non_authorized_object_count,
                canale_slack_alert=SLACK_CANALE_ALERT,
                esito_invio_alert=alert_slack_status,
                durata_job=elapsed_minutes,
                messaggio_errore=f"{type(exc).__name__}: {str(exc)[:500]}",
            )
        except Exception:
            logging.warning(
                "Aggiornamento del tab di log fallito durante la gestione dell'errore.",
                exc_info=True,
            )

        failure_message = (
            f"{slack_prefix}\n"
            f"❌❌❌ *FAILURE* ❌❌❌\n"
            f"*Job:* {APP_NAME}\n"
            f"*Data esecuzione:* {job_end_local_str}\n"
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
