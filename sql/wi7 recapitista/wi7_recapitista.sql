--------------------------- ESTRAZIONE WORK ITEM 7
----- se l'enteid è critico, se la notifica è perfezionata non deve essere segnalata come critica
----- questa regola vale per tutti gli enti tranne INPS (a prescindere che siano perfezionate o mento -- tutte come critiche)

SELECT 
    senderpaid,
    iun,
    requestID,
    requesttimestamp AS requestDateTime,
    prodotto,
    geokey,
    recapitista,
    lotto,
    --codice_oggetto AS codiceOggetto, --Per Fulmine
    CONCAT ("'", codice_oggetto) AS codiceOggetto, --Per altri Recapitisti
    affido_consolidatore_data,
    stampa_imbustamento_CON080_data,
    affido_recapitista_CON016_data,
    accettazione_recapitista_CON018_data,
    affido_conservato_CON020_data,
    materialita_pronta_CON09A_data,
    scarto_consolidatore_stato,
    scarto_consolidatore_data,
    tentativo_recapito_stato,
    tentativo_recapito_data,
    tentativo_recapito_data_rendicontazione,
    messaingiacenza_recapito_stato,
    messaingiacenza_recapito_data,
    messaingiacenza_recapito_data_rendicontazione,
    certificazione_recapito_stato,
    certificazione_recapito_dettagli,
    certificazione_recapito_data,
    certificazione_recapito_data_rendicontazione,
    fine_recapito_stato,
    fine_recapito_data,
    fine_recapito_data_rendicontazione,
    accettazione_23L_RECAG012_data,
    accettazione_23L_RECAG012_data_rendicontazione,
    rend_23L_stato,
    rend_23L_data,
    rend_23L_data_rendicontazione,
    perfezionamento_data,
    perfezionamento_tipo,
    perfezionamento_notificationdate,
    perfezionamento_stato,
    perfezionamento_stato_dettagli,
    CASE WHEN senderpaid IN (
			--- da inserire l'elenco degli enti critici aggiornati, al netto di INPS che viene gestito nel secondo CASE
            ...
	    ) AND perfezionamento_data IS NULL THEN 'Critico' 
        --- gestione di INPS
	    WHEN senderpaid = '53b40136-65f2-424b-acfb-7fae17e35c60' THEN 'Critico'
    END AS criticita,
    flag_wi7_report_postalizzazioni_incomplete, 
    wi7_cluster
FROM send.gold_postalizzazione_analytics 
WHERE flag_wi7_report_postalizzazioni_incomplete = 1  
--fin qui estrazione WI7 completa, se serve il singolo recapitista aggiungere:
AND
--recapitista_unificato = 'Fulmine'
--recapitista_unificato = 'POST & SERVICE'
--recapitista_unificato = 'Poste'
recapitista_unificato = 'RTI Sailpost-Snem' 
;

--- Verifica della data di aggiornamento della gold
SELECT MAX(requesttimestamp) FROM send.gold_postalizzazione_analytics

