--- Partendo dallo scarico delle critiche della scorsa settimana, aggiornare la tabella temporanea e effettuare un nuovo scarico 


--- Verifica delle numeriche delle criticità della settimana precedente - tabella temporanea dedicata
SELECT COUNT(*) FROM send_dev.weekly_postalizzazioni_critiche

--------------------------------------------- Dettaglio
SELECT 
    g.senderpaid,
    g.iun,
    g.requestID,
    g.requesttimestamp AS requestDateTime,
    g.prodotto,
    g.geokey,
    g.recapitista_unificato,
    g.lotto,
    --codice_oggetto AS codiceOggetto, --Per Fulmine
    CONCAT ("'", g.codice_oggetto) AS codiceOggetto, --Per altri Recapitisti
    g.affido_consolidatore_data,
    g.stampa_imbustamento_CON080_data,
    g.affido_recapitista_CON016_data,
    g.accettazione_recapitista_CON018_data,
    g.affido_conservato_CON020_data,
    g.materialita_pronta_CON09A_data,
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
    g.fine_recapito_stato,
    g.fine_recapito_data,
    g.fine_recapito_data_rendicontazione,
    g.accettazione_23L_RECAG012_data,
    g.accettazione_23L_RECAG012_data_rendicontazione,
    g.rend_23L_stato,
    g.rend_23L_data,
    g.rend_23L_data_rendicontazione,
    g.causa_forza_maggiore_dettagli,
    g.causa_forza_maggiore_data,
    g.causa_forza_maggiore_data_rendicontazione,
    g.perfezionamento_data,
    g.perfezionamento_tipo,
    g.perfezionamento_notificationdate,
    g.perfezionamento_stato,
    g.perfezionamento_stato_dettagli,
    g.flag_wi7_report_postalizzazioni_incomplete, 
    g.wi7_cluster, -- quando il flag wi7 = 0 'Chiuse'
    s.statusrequest
FROM send.gold_postalizzazione_analytics g LEFT JOIN send.silver_postalizzazione_denormalized s ON (g.requestid = s.requestid_computed)
WHERE g.requestid IN (
		SELECT requestid FROM send_dev.weekly_postalizzazioni_critiche
    );



--------------------------------------------- Prima pivot - completa
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
),
aggregati AS (
    SELECT 
        g.senderpaid, 
        COUNT(CASE WHEN recapitista_unif = 'Fulmine' THEN 1 END) AS conteggio_fulmine,
        COUNT(CASE WHEN recapitista_unif = 'Poste' THEN 1 END) AS conteggio_poste,
        COUNT(CASE WHEN recapitista_unif = 'POST & SERVICE' THEN 1 END) AS conteggio_po_se,
        COUNT(CASE WHEN recapitista_unif = 'RTI Sailpost-Snem' THEN 1 END) AS conteggio_sailpost,
        COUNT(*) AS totale
    FROM dati_gold_corretti g
    LEFT JOIN send.silver_postalizzazione_denormalized s 
        ON g.requestid = s.requestid_computed
    WHERE g.requestid IN (
        SELECT requestid FROM send_dev.weekly_postalizzazioni_critiche
    ) 
    GROUP BY g.senderpaid
),
totale AS (
    SELECT 
        'TOTALE' AS senderpaid,
        SUM(conteggio_fulmine) AS conteggio_fulmine,
        SUM(conteggio_poste) AS conteggio_poste,
        SUM(conteggio_po_se) AS conteggio_po_se,
        SUM(conteggio_sailpost) AS conteggio_sailpost,
        SUM(totale) AS totale
    FROM aggregati
)
SELECT *
FROM (
    SELECT * FROM aggregati
    UNION ALL
    SELECT * FROM totale
) t
ORDER BY 
    CASE WHEN t.senderpaid = 'TOTALE' THEN 1 ELSE 0 END,
    t.senderpaid ASC;


--------------------------------------------- Seconda pivot - senza chiuse
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
),
aggregati AS (
    SELECT 
        g.senderpaid, 
        COUNT(CASE WHEN recapitista_unif = 'Fulmine' THEN 1 END) AS conteggio_fulmine,
        COUNT(CASE WHEN recapitista_unif = 'Poste' THEN 1 END) AS conteggio_poste,
        COUNT(CASE WHEN recapitista_unif = 'POST & SERVICE' THEN 1 END) AS conteggio_po_se,
        COUNT(CASE WHEN recapitista_unif = 'RTI Sailpost-Snem' THEN 1 END) AS conteggio_sailpost,
        COUNT(*) AS totale
    FROM dati_gold_corretti g
    LEFT JOIN send.silver_postalizzazione_denormalized s 
        ON g.requestid = s.requestid_computed
    WHERE flag_wi7_report_postalizzazioni_incomplete = 1 AND g.requestid IN (
        SELECT requestid FROM  send_dev.weekly_postalizzazioni_critiche
    ) 
    GROUP BY g.senderpaid
),
totale AS (
    SELECT 
        'TOTALE' AS senderpaid,
        SUM(conteggio_fulmine) AS conteggio_fulmine,
        SUM(conteggio_poste) AS conteggio_poste,
        SUM(conteggio_po_se) AS conteggio_po_se,
        SUM(conteggio_sailpost) AS conteggio_sailpost,
        SUM(totale) AS totale
    FROM aggregati
)
SELECT *
FROM (
    SELECT * FROM aggregati
    UNION ALL
    SELECT * FROM totale
) t
ORDER BY 
    CASE WHEN t.senderpaid = 'TOTALE' THEN 1 ELSE 0 END,
    t.senderpaid ASC;
