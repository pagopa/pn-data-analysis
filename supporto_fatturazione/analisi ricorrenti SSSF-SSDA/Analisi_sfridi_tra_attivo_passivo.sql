WITH parametri AS (
    SELECT
        CAST('2025-01-01' AS TIMESTAMP) AS start_affido_consolidatore,
        CAST('2026-05-01' AS TIMESTAMP) AS end_affido_consolidatore
),
base AS (
    SELECT
        g.iun,
        g.requestid,
        g.attempt_number,
        CAST(g.pcretry_number AS INT) AS pcretry_number,
        g.affido_consolidatore_data,
        g.geokey,
        g.recapitista,
        g.prezzo_ente,
        g.costo_recapitista,
        g.costo_consolidatore
    FROM send.gold_postalizzazione_analytics g
    INNER JOIN parametri p
        ON g.affido_consolidatore_data >= p.start_affido_consolidatore
       AND g.affido_consolidatore_data <  p.end_affido_consolidatore
),
coppie_pcretry AS (
    SELECT
        iniziale.iun,
        iniziale.requestid AS request_id_iniziale,
        iniziale.affido_consolidatore_data AS data_affido_consolidatore_iniziale,
        finale.requestid AS request_id_finale,
        finale.affido_consolidatore_data AS data_affido_consolidatore_finale,
        finale.geokey AS cap,
        iniziale.recapitista AS recapitista_iniziale,
        finale.recapitista AS recapitista_finale,
        iniziale.prezzo_ente,
        COALESCE(iniziale.costo_recapitista, 0) AS costo_recapitista_iniziale,
        COALESCE(finale.costo_recapitista, 0) AS costo_recapitista_finale,
        COALESCE(iniziale.costo_consolidatore, 0) AS costo_consolidatore_iniziale,
        COALESCE(finale.costo_consolidatore, 0) AS costo_consolidatore_finale,
        iniziale.attempt_number,
        iniziale.pcretry_number AS pcretry_iniziale,
        finale.pcretry_number AS pcretry_finale
    FROM base iniziale
    INNER JOIN base finale
        ON iniziale.iun = finale.iun
       AND iniziale.prezzo_ente = finale.prezzo_ente
       AND iniziale.attempt_number = finale.attempt_number
       AND finale.pcretry_number = iniziale.pcretry_number + 1
    WHERE (
            COALESCE(iniziale.costo_recapitista, 0) <> COALESCE(finale.costo_recapitista, 0)
            OR 
            COALESCE(iniziale.costo_consolidatore, 0) <> COALESCE(finale.costo_consolidatore, 0)
      )
)
SELECT
    iun,
    request_id_iniziale,
    data_affido_consolidatore_iniziale,
    request_id_finale,
    data_affido_consolidatore_finale,
    CONCAT("'", cap) AS cap,
    recapitista_iniziale,
    recapitista_finale,
    prezzo_ente,
    costo_recapitista_iniziale,
    costo_recapitista_finale,
    costo_consolidatore_iniziale,
    costo_consolidatore_finale,
    --
    costo_recapitista_finale - costo_recapitista_iniziale AS delta_recapitista,
    costo_consolidatore_finale - costo_consolidatore_iniziale AS delta_consolidatore,
    (costo_recapitista_finale - costo_recapitista_iniziale)
      +
      (costo_consolidatore_finale - costo_consolidatore_iniziale)
      AS delta_totale,
    --
    ABS(costo_recapitista_finale - costo_recapitista_iniziale) AS delta_recapitista_abs,
    ABS(costo_consolidatore_finale - costo_consolidatore_iniziale) AS delta_consolidatore_abs,
    ABS(costo_recapitista_finale - costo_recapitista_iniziale)
      +
      ABS(costo_consolidatore_finale - costo_consolidatore_iniziale) AS delta_totale_abs,
    --
    CASE
        WHEN costo_recapitista_finale <> costo_recapitista_iniziale
         AND costo_consolidatore_finale <> costo_consolidatore_iniziale
            THEN 'entrambi'
        WHEN costo_recapitista_finale <> costo_recapitista_iniziale
            THEN 'recapitista'
        WHEN costo_consolidatore_finale <> costo_consolidatore_iniziale
            THEN 'consolidatore'
        ELSE 'nessuna'
    END AS tipo_variazione
FROM coppie_pcretry
ORDER BY
    iun,
    attempt_number,
    pcretry_iniziale;