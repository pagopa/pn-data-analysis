---------- La seguente query ha lo scopo di produrre uno specchietto di monitoraggio 
---------- sulla base di notifiche che non hanno valorizzati i campi affido_recapitista_con016_data, accettazione_recapitista_con018_data o entrambi 

---------- Specchietto CON016 e CON018
SELECT COUNT(CASE WHEN affido_recapitista_con016_data IS NULL AND accettazione_recapitista_con018_data IS NULL THEN 1 END ) AS con016_null_con018_null,
       COUNT(CASE WHEN affido_recapitista_con016_data IS NOT NULL AND accettazione_recapitista_con018_data IS NULL THEN 1 END ) AS con018_null,
       COUNT(CASE WHEN affido_recapitista_con016_data IS NULL AND accettazione_recapitista_con018_data IS NOT NULL THEN 1 END) AS con016_null,
       recapitista_unificato
FROM send.gold_postalizzazione_analytics 
WHERE scarto_consolidatore_stato IS NULL 
AND requesttimestamp >= '2024-12-01' 
 GROUP BY recapitista_unificato 



 
 ---alternativa con month e year 
 SELECT COUNT(CASE WHEN affido_recapitista_con016_data IS NULL AND accettazione_recapitista_con018_data IS NULL THEN 1 END ) AS con016_null_con018_null,
       COUNT(CASE WHEN affido_recapitista_con016_data IS NOT NULL AND accettazione_recapitista_con018_data IS NULL THEN 1 END ) AS con018_null,
       COUNT(CASE WHEN affido_recapitista_con016_data IS NULL AND accettazione_recapitista_con018_data IS NOT NULL THEN 1 END) AS con016_null,
       recapitista_unificato
FROM send.gold_postalizzazione_analytics 
WHERE scarto_consolidatore_stato IS NULL 
      AND requesttimestamp >= '2025-01-01' AND requesttimestamp < '2025-04-01' 
---AND YEAR(requesttimestamp) = 2025 AND MONTH(requesttimestamp) BETWEEN 1 AND 3
 GROUP BY recapitista_unificato 
 
 


 --- Aggiunta di una colonna di monitoraggio sulla classificazione della notifica, nell'estrazione di dettaglio per recapitista
 WITH temp AS (SELECT 
 	CASE 
        WHEN affido_recapitista_con016_data IS NULL AND accettazione_recapitista_con018_data IS NULL THEN 'con016_null_con018_null'
        WHEN affido_recapitista_con016_data IS NOT NULL AND accettazione_recapitista_con018_data IS NULL THEN 'con018_null'
        WHEN affido_recapitista_con016_data IS NULL AND accettazione_recapitista_con018_data IS NOT NULL THEN 'con016_null'
        ELSE NULL
    END AS classificazione_notifica,
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
WHERE scarto_consolidatore_stato IS NULL 
  AND requesttimestamp >= '2025-01-01' 
  AND requesttimestamp < '2025-04-01'
  AND recapitista_unificato = 'POST & SERVICE' --- RTI Sailpost-Snem
) SELECT * FROM temp WHERE classificazione_notifica IS NOT NULL