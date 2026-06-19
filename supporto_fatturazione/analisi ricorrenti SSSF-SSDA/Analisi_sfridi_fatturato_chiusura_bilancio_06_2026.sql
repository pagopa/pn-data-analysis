WITH institution AS (
    SELECT
        internalistitutionid AS senderpaid,
        denomination AS descrizione_ente
    FROM (
        SELECT
            c.internalistitutionid,
            CASE
                WHEN c.subunittype IS NOT NULL
                    THEN CONCAT(c.rootparent_description, ' - ', c.description)
                ELSE c.description
            END AS denomination,
            ROW_NUMBER() OVER (
                PARTITION BY c.internalistitutionid
                ORDER BY c.updatedat DESC
            ) AS rn
        FROM selfcare.gold_contracts c
        WHERE c.product = 'prod-pn'
          AND c.state = 'ACTIVE'
    ) x
    WHERE rn = 1
),
n_base AS (
    SELECT
        n.iun,
        n.senderpaid,
        n.type_notif,
        n.actual_status,
        n.sentat,
        n.tms_viewed,
        n.tms_effective_date
    FROM send.gold_notification_analytics n
    WHERE 1=1
      AND n.sentat >= '2023-01-01'
      AND n.sentat < '2026-01-01'
      AND n.type_notif = 'DIGITAL'
      AND n.actual_status NOT IN ('CANCELLED') --, 'UNREACHABLE')
      AND (n.tms_viewed IS NULL OR n.tms_viewed > '2026-04-30')
      AND (n.tms_effective_date IS NULL OR n.tms_effective_date > '2026-04-30')
),
g_base AS (
    SELECT
        g.iun,
        g.senderpaid,
        g.requestid,
        g.data_deposito,
        g.affido_consolidatore_data,
        g.requesttimestamp,
        g.perfezionamento_data,
        g.prezzo_ente,
        n.type_notif,
        n.actual_status
    FROM send.gold_postalizzazione_analytics g
    LEFT JOIN send.gold_notification_analytics n
        ON g.iun = n.iun
    WHERE g.pcretry_rank = 1
      AND g.attempt_rank = 1
      AND g.affido_consolidatore_data >= '2023-01-01'
      AND g.affido_consolidatore_data < '2026-01-01'
      AND (g.perfezionamento_data IS NULL OR g.perfezionamento_data > '2026-04-30')
      --AND g.statusrequest NOT IN ('PN999', 'PN998', 'error', 'internalError', 'syntaxError','transformationError','semanticError','authenticationError', 'duplicatedRequest')
),
base AS (
    SELECT
        i.descrizione_ente,
        COALESCE(n.senderpaid, g.senderpaid) AS codice_ente_senderpaid,
        COALESCE(n.iun, g.iun) AS iun,
        CASE
            WHEN g.requestid IS NOT NULL THEN g.affido_consolidatore_data
            ELSE n.sentat
        END AS data_accertamento_presa_in_carico,
        COALESCE(n.type_notif, g.type_notif) AS tipologia_notifica,
        COALESCE(n.actual_status, g.actual_status) AS stato_notifica,
        CASE
            WHEN g.requestid IS NOT NULL THEN g.prezzo_ente
            ELSE 0
        END AS prezzo_ente_analogico,
        CASE
            WHEN COALESCE(n.type_notif, g.type_notif) = 'DIGITAL' THEN 1
            ELSE 0
        END AS prezzo_ente_digitale
    FROM n_base n
    FULL OUTER JOIN g_base g
        ON n.iun = g.iun
    LEFT JOIN institution i
        ON COALESCE(n.senderpaid, g.senderpaid) = i.senderpaid
)
SELECT
    b.descrizione_ente AS `descrizione dell’Ente`,
    b.codice_ente_senderpaid AS `codice Ente`,
    b.iun AS `IUN`,
    b.data_accertamento_presa_in_carico AS `data di accertamento della presa in carico`,
    b.tipologia_notifica AS `tipologia di notifica`,
    b.stato_notifica AS `stato della notifica`,
    b.prezzo_ente_analogico AS `Prezzo Ente analogico`,
    b.prezzo_ente_digitale AS `Prezzo Ente digitale`,
    b.prezzo_ente_analogico + b.prezzo_ente_digitale AS `somma tra prezzo analogico e digitale`
FROM base b
WHERE stato_notifica NOT IN ('CANCELLED')
--AND b.iun = 'RQDM-EDUT-HTZJ-202512-L-1'
;