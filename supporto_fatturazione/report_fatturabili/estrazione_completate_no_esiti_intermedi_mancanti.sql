--- 1° CTE dati_gold_corretti: fix su correzione campo recapitista sulla base del codice oggetto
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
), --- 2° CTE temp_postalizzazione: aggiunge i flag di assenza_inesito, assenza_messa_in_giacenza, assenza_dematerializzazione_23l_ar_plico, assenza_demat_*, assenza_recag012
    temp_postalizzazione AS (
        SELECT
            s.*,
            IF (
                fine_recapito_stato IN ('RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C','RECAG005C','RECAG006C','RECAG007C','RECAG008C')
                AND tentativo_recapito_stato IS NULL,
                1,
    0
            ) AS assenza_inesito,
            IF (
                fine_recapito_stato IN ('RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C','RECAG005C','RECAG006C','RECAG007C','RECAG008C')
                AND messaingiacenza_recapito_stato IS NULL,
                1,
                0
            ) AS assenza_messa_in_giacenza,
            IF (
                certificazione_recapito_stato IS NULL,
                1,
                0
            ) AS assenza_pre_esito,
            IF (
                (
                    fine_recapito_stato IN ('RECAG008C') AND ( demat_23l_ar_stato IS NULL OR demat_plico_stato IS NULL)
                )
                OR ( 
                    demat_23l_ar_stato IS NULL AND demat_plico_stato IS NULL
                ),
                1,
                0
            ) AS assenza_dematerializzazione_23l_ar_plico,
            IF (
                prodotto = '890'
                AND fine_recapito_stato IN ('RECAG005C','RECAG006C','RECAG007C','RECAG008C')
                AND demat_arcad_data_rendicontazione IS NULL,
                1,
                0
            ) AS assenza_demat_ARCAD,
            IF (
                prodotto = '890'
                AND fine_recapito_stato IN ('RECAG005C','RECAG006C','RECAG007C','RECAG008C')
                AND accettazione_23l_recag012_data IS NULL,
                1,
                0
            ) AS assenza_RECAG012
        --- FIX : aggiunta temp_controlli
        FROM dati_gold_corretti s
    ),
    --- 3° CTE: mi costruisco il flag di esiti mancanti 
    --- Seleziono tutti i campi di prima e in più aggiungo un flag unico sugli esiti mancanti se trovo almeno un esito mancante tra quelli calcolati prima 
    temp_assenza_eventi AS (
    SELECT
        *,
        IF(
            assenza_inesito = 1 OR assenza_messa_in_giacenza = 1 OR assenza_pre_esito = 1 
            OR assenza_dematerializzazione_23l_ar_plico = 1 OR assenza_demat_ARCAD = 1 OR assenza_RECAG012 = 1,
            1,
            0
        ) AS flag_esiti_mancanti
        FROM
            temp_postalizzazione
    ) SELECT 
 g.iun,
    g.requestid,
    g.requesttimestamp,
    g.prodotto,
    g.geokey,
    c.area AS zona,
    c.provincia,
    c.regione,
    g.recapitista_unif,
    g.lotto,
    CONCAT("'", g.codice_oggetto) AS codiceOggetto, -- Per altri Recapitisti
    g.prezzo_ente,
    g.grammatura,
    g.numero_pagine,
    g.costo_consolidatore,
    g.costo_recapitista,
    g.affido_consolidatore_data,
    g.stampa_imbustamento_con080_data,
    g.materialita_pronta_con09a_data,
    g.affido_recapitista_con016_data,
    g.accettazione_recapitista_con018_data,
    g.scarto_consolidatore_stato,
    g.scarto_consolidatore_data,
    g.tentativo_recapito_stato,
    g.tentativo_recapito_data,
    g.tentativo_recapito_data_rendicontazione,
    g.messaingiacenza_recapito_stato,
    g.messaingiacenza_recapito_data,
    g.messaingiacenza_recapito_data_rendicontazione,
    g.certificazione_recapito_stato,
    g.certificazione_recapito_dettagli,
    g.certificazione_recapito_data,
    g.certificazione_recapito_data_rendicontazione,
    g.demat_23l_ar_stato,
    g.demat_23l_ar_data_rendicontazione,
    g.demat_plico_stato,
    g.demat_plico_data_rendicontazione,
    g.demat_arcad_stato,
    g.demat_arcad_data_rendicontazione,
    g.fine_recapito_stato,
    g.fine_recapito_data,
    g.fine_recapito_data_rendicontazione,
    g.accettazione_23l_recag012_data,
    g.accettazione_23l_recag012_data_rendicontazione,
    g.perfezionamento_data,
    g.perfezionamento_tipo,
    g.perfezionamento_notificationdate,
    g.perfezionamento_stato,
    g.perfezionamento_stato_dettagli,
    g.tms_cancelled,
    g.statusrequest,
    g.flag_esiti_mancanti
FROM temp_assenza_eventi g
LEFT JOIN send_dev.cap_area_provincia_regione c ON c.cap = g.geokey 
WHERE 
    g.statusrequest NOT IN ('PN998') -- Esclusione oggetti bloccati con PN998
    AND g.fine_recapito_stato IS NOT NULL -- Fine_recapito valorizzato
    AND g.scarto_consolidatore_stato IS NULL  -- Esclusione scarti consoolidatore
    AND g.pcretry_rank = 1 -- Filtro su oggetti con pcretry più alto
    AND g.fine_recapito_stato NOT IN (
        'RECRS006', 'RECRS013', 'RECRN006', 'RECRN013',
        'RECAG004', 'RECAG013', 'RECRSI005', 'RECRI005','PN998') -- Esclusione oggetti furtati/smarriti/deteriorati e non rendicontabili
    AND g.recapitista_unif = '${recapitista}'
    AND (-- Calcolare il trimestre da accettazione_recapitista_CON018_data se non è NULL
        (g.accettazione_recapitista_CON018_data IS NOT NULL 
         AND QUARTER(g.accettazione_recapitista_CON018_data) = ${trimestre})
        -- Se CON018 è NULL, calcolare il trimestre su affido_recapitista_CON016_data
        OR (g.accettazione_recapitista_CON018_data IS NULL 
            AND g.affido_recapitista_CON016_data IS NOT NULL 
            AND QUARTER(g.affido_recapitista_CON016_data + INTERVAL 1 DAY) = ${trimestre}))
    AND (-- Anno da accettazione_recapitista_CON018_data se non è NULL, altrimenti da affido_recapitista_CON016_data
        YEAR(CASE WHEN g.accettazione_recapitista_CON018_data IS NULL 
             THEN g.affido_recapitista_CON016_data + INTERVAL 1 DAY
             ELSE g.accettazione_recapitista_CON018_data
             END) = ${anno})
    AND flag_esiti_mancanti = 1 -- Filtro su oggetti con rendicontazione completa
;
    
        