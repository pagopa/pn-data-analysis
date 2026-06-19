-- TUTTI I RECAPITISTI TRANNE POSTE (CAMBIA FILTRO E CODICE OGGETTO)
WITH dati_gold_corretti AS (
    SELECT 
        *,
        CASE
            WHEN codice_oggetto LIKE 'R14%' THEN 'Fulmine'
            WHEN codice_oggetto LIKE '777%' OR codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
            WHEN codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
            WHEN codice_oggetto LIKE '69%' OR codice_oggetto LIKE '381%' OR codice_oggetto LIKE 'RB1%' THEN 'Poste'
            ELSE recapitista_unificato
        END AS recapitista_unif
    FROM send.gold_postalizzazione_analytics
)SELECT
    r.iun,
    r.requestid,
    r.requesttimestamp,
    --r.codice_oggetto, --Per Fulmine
    CONCAT ("'", codice_oggetto) AS codiceOggetto, --Per altri Recapitisti
    r.lotto,
    r.prodotto,
    r.recapitista_unif,
    r.recapitista,
    r.fine_recapito_stato,
    r.fine_recapito_data,
    r.fine_recapito_data_rendicontazione,
    r.statusrequest,
    r.costo_recapitista,
        CASE 
     WHEN r.prodotto = '890' THEN ROUND((r.costo_recapitista*1.0/100.0)*5,2) 
     WHEN r.prodotto IN ('RIR','RIS') THEN ROUND((r.costo_recapitista*1.0/100.0),2) -- SOLO NUOVA GARA
     ELSE ROUND(2,2) END AS penale
FROM dati_gold_corretti r
WHERE 
    CEIL(MONTH(r.fine_recapito_data_rendicontazione) / 3) = 3
    AND YEAR(r.fine_recapito_data_rendicontazione) = 2025
    AND r.scarto_consolidatore_stato IS NULL
    AND r.statusrequest NOT IN ('PN998', 'PN999')
    AND r.fine_recapito_stato IN (
     'RECRS006', 
     'RECRS013', 
     'RECRN006', 
        'RECRN013', 
        'RECAG004', 
        'RECAG013', 
        'RECRI005',
        'RECRSI005')
AND
--recapitista_unif = 'Fulmine'
recapitista_unif = 'POST & SERVICE'
--recapitista_unif = 'RTI Sailpost-Snem' 
;




---PER POSTE
WITH dati_gold_corretti AS (
    SELECT 
        *,
        CASE
            WHEN codice_oggetto LIKE 'R14%' THEN 'Fulmine'
            WHEN codice_oggetto LIKE '777%' OR codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
            WHEN codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
            WHEN codice_oggetto LIKE '69%' OR codice_oggetto LIKE '381%' OR codice_oggetto LIKE 'RB1%' THEN 'Poste'
            ELSE recapitista_unificato
        END AS recapitista_unif
    FROM send.gold_postalizzazione_analytics
)SELECT
    r.iun,
    r.requestid,
    r.requesttimestamp,
    CONCAT ("'", r.codice_oggetto) AS codiceOggetto,
    r.lotto,
    r.prodotto,
    r.recapitista_unif,
    r.recapitista,
    r.fine_recapito_stato,
    r.fine_recapito_data,
    r.fine_recapito_data_rendicontazione,
    r.statusrequest,
    r.costo_recapitista,
        CASE 
     WHEN r.prodotto = '890' THEN ROUND((r.costo_recapitista*1.0/100.0)*5,2) 
     WHEN r.prodotto IN ('RIR','RIS') THEN ROUND((r.costo_recapitista*1.0/100.0),2)  -- SOLO NUOVA GARA
     ELSE ROUND(2,2) END AS penale
FROM dati_gold_corretti r
WHERE (-- Calcolare il trimestre da accettazione_recapitista_CON018_data se non è NULL
      (r.accettazione_recapitista_CON018_data IS NOT NULL AND CEIL(MONTH(r.accettazione_recapitista_CON018_data) / 3) = 2)
      -- Se CON018 è NULL, calcolare il trimestre su affido_recapitista_CON016_data, se non è NULL
      OR (r.accettazione_recapitista_CON018_data IS NULL AND r.affido_recapitista_CON016_data IS NOT NULL AND CEIL(MONTH(r.affido_recapitista_CON016_data + INTERVAL 1 DAY) / 3) = 2)
  )
  AND (-- Anno da accettazione_recapitista_CON018_data se non è NULL, altrimenti da affido_recapitista_CON016_data
      YEAR(CASE 
          WHEN r.accettazione_recapitista_CON018_data IS NULL THEN r.affido_recapitista_CON016_data + INTERVAL 1 DAY
          ELSE r.accettazione_recapitista_CON018_data
      END) = 2024)
  AND r.scarto_consolidatore_stato IS NULL
    AND r.statusrequest NOT IN ('PN998', 'PN999')
    AND r.fine_recapito_stato IN (
     'RECRS006', 
     'RECRS013', 
     'RECRN006', 
        'RECRN013', 
        'RECAG004', 
        'RECAG013', 
        'RECRI005',
        'RECRSI005')
AND
recapitista_unif = 'Poste'
;