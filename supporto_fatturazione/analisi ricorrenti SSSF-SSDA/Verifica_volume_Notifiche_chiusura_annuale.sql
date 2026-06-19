-- Verifica volume Notifiche per chiusura annuale
-- Sostituire solo start_periodo con il primo giorno dell'anno

WITH parametri_raw AS (
    SELECT
        CAST('2025-01-01' AS DATE) AS start_periodo
),
parametri AS (
    SELECT
        CAST(start_periodo AS STRING) AS start_periodo,
        CAST(ADD_MONTHS(start_periodo, 10) AS STRING) AS start_ultimi_2_mesi,
        CAST(ADD_MONTHS(start_periodo, 12) AS STRING) AS end_periodo,
        CAST(DATE_SUB(ADD_MONTHS(start_periodo, 12), 1) AS STRING) AS end_periodo_label
    FROM parametri_raw
),
base_notifiche AS (
    SELECT
        n.iun,
        n.sentat,
        n.type_notif,
        n.tms_viewed,
        n.tms_effective_date,
        n.tms_refused,
        n.tms_cancelled
    FROM send.gold_notification_analytics n
    CROSS JOIN parametri p
    WHERE n.sentat >= p.start_periodo
      AND n.sentat <  p.end_periodo
),
base_notifiche_eventi AS (
    SELECT
        iun,
        type_notif,
        COALESCE(LEAST(tms_viewed, tms_effective_date), tms_viewed, tms_effective_date) AS data_perfezionamento,
        COALESCE(LEAST(tms_refused, tms_cancelled), tms_refused, tms_cancelled) AS data_rifiuto_cancellazione
    FROM base_notifiche
),
cluster AS (
    SELECT
        'Digitale' AS tipologia,
        iun,
        data_perfezionamento
    FROM base_notifiche_eventi
    CROSS JOIN parametri p
    WHERE type_notif = 'DIGITAL'
      AND (
            data_rifiuto_cancellazione IS NULL
         OR data_rifiuto_cancellazione <  p.start_periodo
         OR data_rifiuto_cancellazione >= p.end_periodo
      )
    --
    UNION ALL
    --
    SELECT
        'Analogica' AS tipologia,
        iun,
        data_perfezionamento
    FROM base_notifiche_eventi
    CROSS JOIN parametri p
    WHERE type_notif = 'ANALOG'
      AND (
            data_rifiuto_cancellazione IS NULL
         OR data_rifiuto_cancellazione <  p.start_periodo
         OR data_rifiuto_cancellazione >= p.end_periodo
      )
    --
    UNION ALL
    --
    SELECT
        'Multi-destinatario' AS tipologia,
        iun,
        data_perfezionamento
    FROM base_notifiche_eventi
    CROSS JOIN parametri p
    WHERE type_notif = 'MULTI'
      AND (
            data_rifiuto_cancellazione IS NULL
         OR data_rifiuto_cancellazione <  p.start_periodo
         OR data_rifiuto_cancellazione >= p.end_periodo
      )
    --
    UNION ALL
    --
    SELECT
        'Rifiutate/Cancellate' AS tipologia,
        iun,
        data_perfezionamento
    FROM base_notifiche_eventi
    CROSS JOIN parametri p
    WHERE data_rifiuto_cancellazione >= p.start_periodo
      AND data_rifiuto_cancellazione <  p.end_periodo
    --
    UNION ALL
    --
    SELECT
        'Totale' AS tipologia,
        iun,
        data_perfezionamento
    FROM base_notifiche_eventi
)
SELECT
    c.tipologia AS `Tipologia`,
    --
    COUNT(c.iun) AS `Totale notifiche depositate nell'anno`,
    --
    COUNT(CASE
        WHEN c.data_perfezionamento >= p.start_periodo
         AND c.data_perfezionamento <  p.end_periodo
        THEN c.iun
    END) AS `Perfezionate nell'anno`,
    --
    COUNT(CASE
        WHEN c.data_perfezionamento >= p.start_periodo
         AND c.data_perfezionamento <  p.start_ultimi_2_mesi
        THEN c.iun
    END) AS `di cui Perfezionate nei primi 10 mesi`,
    --
    COUNT(CASE
        WHEN c.data_perfezionamento >= p.start_ultimi_2_mesi
         AND c.data_perfezionamento <  p.end_periodo
        THEN c.iun
    END) AS `di cui Perfezionate negli ultimi 2 mesi`,
    --
    COUNT(CASE
        WHEN c.data_perfezionamento IS NULL
          OR c.data_perfezionamento <  p.start_periodo
          OR c.data_perfezionamento >= p.end_periodo
        THEN c.iun
    END) AS `Non Perfezionate nell'anno`,
    --
    CONCAT(
        '[',
        p.start_periodo,
        ' , ',
        p.end_periodo_label,
        ']'
    ) AS `Periodo di riferimento`
FROM cluster c
CROSS JOIN parametri p
GROUP BY
    c.tipologia,
    p.start_periodo,
    p.start_ultimi_2_mesi,
    p.end_periodo,
    p.end_periodo_label
ORDER BY
    CASE c.tipologia
        WHEN 'Digitale' THEN 1
        WHEN 'Analogica' THEN 2
        WHEN 'Multi-destinatario' THEN 3
        WHEN 'Rifiutate/Cancellate' THEN 4
        WHEN 'Totale' THEN 5
        ELSE 99
    END;