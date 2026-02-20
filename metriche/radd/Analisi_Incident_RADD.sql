--- ANALISI NUMERICHE
--- il comune è da considerarsi scoperto quando il flag attivazione_marche è = NULL oppure è = 1 e stiamo con sentat prima dell'8 aprile 2025

    
---- ANALISI NUMERICHE PER ATTIVAZIONE NORMALE  - DAL 25 MARZO IN POI - NO MARCHE 
WITH temp AS (
select
        t.eventid,
        t.iun,
        g.geokey, -- questo serve poi per fare la join con i cap della tabella cap comuni 
        COALESCE(c.comune, 'Comune Scoperto') AS comune,
        c.attivazione_marche,
        `timestamp`,
        t.paid,
        t.notificationsentat,
        get_json_object(t.details, '$.recIndex') recIndex,
        get_json_object(t.details, '$.aarTemplateType') aarTemplateType,
        get_json_object(t.details, '$.nextSourceAttemptsMade') nextSourceAttemptsMade,
        get_json_object(t.details, '$.numberOfPages') numberofpages,
        get_json_object(t.details, '$.aarKey') aarKey,
        t.timelineelementid,
        t.category,
        t.`year`,
        t.`month`,
        t.daily
    from send.silver_timeline t 
         LEFT JOIN send.gold_postalizzazione_analytics g ON (g.iun= t.iun)
         LEFT JOIN send_dev.cap_comuni c ON (c.cap = g.geokey)
    where t.notificationsentat >= '2025-03-25'
          AND category in ('AAR_CREATION_REQUEST') AND g.attempt_number = '1'
) 
--- Qui inserire le viste definite di seguito 

-- ESTRAZIONE DI DETTAGLIO SUI COMUNI COPERTI DAL 25 MARZO IN POI 
SELECT * FROM temp
WHERE attivazione_marche = '0' 
      AND aarTemplateType = 'AAR_NOTIFICATION'


-- ESTRAZIONE CONTEGGIO SUI COMUNI COPERTI (NO MARCHE) DAL 25 MARZO IN POI 
SELECT 
    --c.attivazione_marche,
    COUNT(DISTINCT CASE 
            WHEN comune <> 'Comune Scoperto' 
                 AND aarTemplateType = 'AAR_NOTIFICATION_RADD_ALT'
            THEN iun END) AS notifiche_Comuni_Coperti_RADD_ALT,
    COUNT(DISTINCT CASE 
            WHEN comune <> 'Comune Scoperto' 
                 AND aarTemplateType = 'AAR_NOTIFICATION'
            THEN iun END) AS notifiche_Comuni_Coperti_no_RADD_ALT 
FROM temp 
WHERE attivazione_marche = '0'

-- ESTRAZIONE DI DETTAGLIO SUI COMUNI COPERTI (NO MARCHE) CHE NON HANNO UN RADD ALT
SELECT COUNT(DISTINCT iun) FROM temp
WHERE attivazione_marche = '0' 
      AND aarTemplateType = 'AAR_NOTIFICATION'



----- APPROFONDIMENTO REGIONE MARCHE COMUNI SCOPERTI
WITH temp AS (
select
        t.eventid,
        t.iun,
        g.geokey, -- questo serve poi per fare la join con i cap della tabella cap comuni 
        COALESCE(c.comune, 'Comune Scoperto') AS comune,
        c.attivazione_marche,
        `timestamp`,
        t.paid,
        t.notificationsentat,
        get_json_object(t.details, '$.recIndex') recIndex,
        get_json_object(t.details, '$.aarTemplateType') aarTemplateType,
        get_json_object(t.details, '$.nextSourceAttemptsMade') nextSourceAttemptsMade,
        get_json_object(t.details, '$.numberOfPages') numberofpages,
        get_json_object(t.details, '$.aarKey') aarKey,
        t.timelineelementid,
        t.category,
        t.`year`,
        t.`month`,
        t.daily
    from send.silver_timeline t 
         LEFT JOIN send.gold_postalizzazione_analytics g ON (g.iun= t.iun)
         LEFT JOIN send_dev.cap_comuni c ON (c.cap = g.geokey)
    where t.notificationsentat >= '2025-04-08'
    AND category in ('AAR_CREATION_REQUEST') AND g.attempt_number = '0'
)
--- Qui inserire le viste definite di seguito 

-- ESTRAZIONE DI DETTAGLIO
SELECT * FROM temp
WHERE attivazione_marche = '1'
      AND aarTemplateType = 'AAR_NOTIFICATION'

-- ESTRAZIONE CONTEGGIO SUI COMUNI COPERTI MARCHE DA 8 Aprile IN POI 
SELECT 
    COUNT(DISTINCT CASE 
            WHEN comune <> 'Comune Scoperto' 
                 AND aarTemplateType = 'AAR_NOTIFICATION_RADD_ALT'
            THEN iun END) AS notifiche_altri_comuni_RADD_ALT,
    COUNT(DISTINCT CASE 
            WHEN comune <> 'Comune Scoperto' 
                 AND aarTemplateType = 'AAR_NOTIFICATION'
            THEN iun END) AS notifiche_altri_comuni_no_RADD_ALT 
FROM temp 
WHERE attivazione_marche = '1'


-- ESTRAZIONE DI DETTAGLIO SUI COMUNI COPERTI MARCHE CHE NON HANNO UN RADD ALT
SELECT COUNT(DISTINCT iun) FROM temp
WHERE attivazione_marche = '1'
      AND aarTemplateType = 'AAR_NOTIFICATION'

      
      
--------------- ANALISI NUMERICHE COMUNI SCOPERTI 
WITH temp AS (
    SELECT
        t.eventid,
        t.iun,
        g.geokey,
        COALESCE(c.comune, 'Comune Scoperto') AS comune,
        c.attivazione_marche,
        `timestamp`,
        t.paid,
        t.notificationsentat,
        t.details,
        get_json_object(t.details, '$.recIndex') recIndex,
        get_json_object(t.details, '$.aarTemplateType') aarTemplateType,
        get_json_object(t.details, '$.nextSourceAttemptsMade') nextSourceAttemptsMade,
        get_json_object(t.details, '$.numberOfPages') numberofpages,
        get_json_object(t.details, '$.aarKey') aarKey,
        t.timelineelementid,
        t.category,
        t.`year`,
        t.`month`,
        t.daily
    FROM send.silver_timeline t 
    LEFT JOIN send.gold_postalizzazione_analytics g ON (g.iun = t.iun)
    LEFT JOIN send_dev.cap_comuni c ON (c.cap = g.geokey)
    WHERE t.notificationsentat >= '2025-03-25' -- da inserire la data precisa con il timestamp
     AND category in ('AAR_CREATION_REQUEST') AND g.attempt_number = '0'
) 
--- Qui inserire le viste definite di seguito 



--- Conteggi - COMUNI SCOPERTI 
SELECT 
    COUNT(DISTINCT CASE 
        WHEN (attivazione_marche IS NULL 
               OR (attivazione_marche = '1' AND notificationsentat < '2025-04-08'))
        THEN iun END) AS notifiche_comune_scoperto_no_RADD_ALT,
    COUNT(DISTINCT CASE 
        WHEN (attivazione_marche IS NULL 
               OR (attivazione_marche = '1' AND notificationsentat < '2025-04-08'))
             AND aarTemplateType = 'AAR_NOTIFICATION_RADD_ALT'
        THEN iun END) AS notifiche_comune_scoperto_RADD_ALT
FROM temp



-- ESTRAZIONE DI DETTAGLIO SUI COMUNI SCOPERTI CHE HANNO UN RADD ALT
SELECT * FROM temp
WHERE (attivazione_marche IS NULL OR (attivazione_marche = '1' AND notificationsentat < '2025-04-08') )
      AND aarTemplateType = 'AAR_NOTIFICATION_RADD_ALT'

--- CONTEGGIO PER VERIFICARE LE NUMERICHE DEL DETTAGLIO DI SOPRA
SELECT COUNT(DISTINCT iun) FROM temp
WHERE (attivazione_marche IS NULL OR (attivazione_marche = '1' AND notificationsentat < '2025-04-08') )
      AND aarTemplateType = 'AAR_NOTIFICATION_RADD_ALT'      

