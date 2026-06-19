/*
Estrazione 1 - Consolidatore

Numero e costo consolidatore degli oggetti con affido consolidatore ricadente tra il 01 Aprile 2026 e il 31 Maggio 2026. 
Se affido consolidatore non valorizzato, considerare request time stamp nell’intervallo indicato sopra. Da escludere scarti. 
Necessitiamo solo dell’aggregato, non dell’elenco dettagliato di ogni oggetto. 

Numero medio di fogli aggiuntivi stampati (dunque num totale di fogli -1) nel seguente periodo temporale: 01/01/2025 - 31/05/2026 (data di riferimento: affido al consolidatore)
*/

-- ESTRAZIONE 1.1

WITH base AS (
    SELECT
        g.requestid,
        g.costo_consolidatore,
        COALESCE(g.affido_consolidatore_data, g.requesttimestamp) AS data_riferimento_consolidatore
    FROM send.gold_postalizzazione_analytics g
    WHERE 1=1
      AND g.scarto_consolidatore_stato IS NULL
)
SELECT
    '01/04/2026 - 31/05/2026' AS periodo_riferimento,
    COUNT(*) AS numero_oggetti,
    SUM(costo_consolidatore) AS costo_consolidatore
FROM base
WHERE data_riferimento_consolidatore >= '2026-04-01'  AND data_riferimento_consolidatore <  '2026-06-01'
;
  
  
  
  -- ESTRAZIONE 1.2

WITH base AS (
    SELECT
        g.requestid,
        g.affido_consolidatore_data,
        g.numero_pagine,
        CASE
            WHEN g.numero_pagine >= 1 THEN g.numero_pagine - 1
            ELSE g.numero_pagine
        END AS fogli_aggiuntivi
    FROM send.gold_postalizzazione_analytics g
    WHERE 1=1
      AND g.affido_consolidatore_data >= '2025-01-01' AND g.affido_consolidatore_data <  '2026-06-01'
)
SELECT
    '01/01/2025 - 31/05/2026' AS periodo_riferimento,
    --COUNT(*) AS numero_oggetti,
    --SUM(fogli_aggiuntivi) AS totale_fogli_aggiuntivi,
    ROUND(AVG(fogli_aggiuntivi), 2) AS media_fogli_aggiuntivi
FROM base;



/*
Estrazione 2 – Recapitisti

Per ogni lotto, estrarre numero e costo recapitista degli oggetti avente CON018 negli intervalli indicati sotto. 
Se CON018 non valorizzato, considerare CON016 negli intervalli indicati sotto. 
Se CON016 non valorizzato, considerare request time stamp negli intervalli indicati sotto. 
Per ogni ATTEMPT, conteggiare solo l’ultimo PC RETRY. Necessitiamo solo dell’aggregato, non dell’elenco dettagliato di ogni oggetto. 

Lotti 1,2,5,7,9,10,11,15,17,19,21,23,24,26,28,29,97,98,99: dal 01 Gennaio 2025 al 31 Maggio 2026

Lotto 30BIS: due viste separate: dal 01 Gennaio 2025 al 24 Febbraio 2026; dal 25 febbraio 2026 al 31 Maggio 2026

Lotti 13,14,30: dal 01 Ottobre 2025 al 31 Maggio 2026

Lotti 22,25,27: dal 01 Aprile 2026 al 31 Maggio 2026

Lotti 6,12,18,20: dal 01 Aprile 2026 al 31 Maggio 2026

Per ogni lotto, indicare dunque i campi “lotto”, “numero oggetti”, “costo recapitista” e, se possibile, il “periodo di riferimento”
*/

-- ESTRAZIONE 2

WITH base AS (
    SELECT
        g.requestid,
        g.lotto,
        g.costo_recapitista,
        COALESCE(
            g.accettazione_recapitista_con018_data,
            g.affido_recapitista_con016_data,
            g.requesttimestamp
        ) AS data_riferimento_recapitista
    FROM send.gold_postalizzazione_analytics g
    WHERE g.pcretry_rank = 1
),
periodizzati AS (
    SELECT
        b.requestid,
        b.lotto,
        b.costo_recapitista,
        CASE
            WHEN b.lotto IN (
                '1','2','5','7','9','10','11','15','16', '17','19',
                '21','23','24','26','28','29','97','98','99'
            )
            AND b.data_riferimento_recapitista >= '2025-01-01'
            AND b.data_riferimento_recapitista <  '2026-06-01'
            THEN '01/01/2025 - 31/05/2026'
            --
            WHEN b.lotto = '30BIS'
            AND b.data_riferimento_recapitista >= '2025-01-01'
            AND b.data_riferimento_recapitista <  '2026-02-25'
            THEN '01/01/2025 - 24/02/2026'
            --
            WHEN b.lotto = '30BIS'
            AND b.data_riferimento_recapitista >= '2026-02-25'
            AND b.data_riferimento_recapitista <  '2026-06-01'
            THEN '25/02/2026 - 31/05/2026'
            --
            WHEN b.lotto IN ('13','14','30')
            AND b.data_riferimento_recapitista >= '2025-10-01'
            AND b.data_riferimento_recapitista <  '2026-06-01'
            THEN '01/10/2025 - 31/05/2026'
            --
            WHEN b.lotto IN ('22','25','27')
            AND b.data_riferimento_recapitista >= '2026-04-01'
            AND b.data_riferimento_recapitista <  '2026-06-01'
            THEN '01/04/2026 - 31/05/2026'
            --
            WHEN b.lotto IN ('6','12','18','20')
            AND b.data_riferimento_recapitista >= '2026-04-01'
            AND b.data_riferimento_recapitista <  '2026-06-01'
            THEN '01/04/2026 - 31/05/2026'
        END AS periodo_riferimento
    FROM base b
)
SELECT
    lotto,
    COUNT(*) AS numero_oggetti,
    SUM(costo_recapitista) AS costo_recapitista,
    periodo_riferimento
FROM periodizzati
WHERE periodo_riferimento IS NOT NULL
GROUP BY
    lotto,
    periodo_riferimento
ORDER BY
    CASE
        WHEN lotto = '30BIS' THEN 30
        ELSE CAST(lotto AS INT)
    END,
    lotto,
    periodo_riferimento;
