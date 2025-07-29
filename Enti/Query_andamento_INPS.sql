--------------------------------------------- QUERY 1 - Andamento Regionale prodotti 890 e AR
WITH dati_gold_corretti AS (
    SELECT 
        *,
        CASE
            WHEN codice_oggetto LIKE 'R14%' THEN 'Fulmine'
            WHEN codice_oggetto LIKE '777%' OR codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
            WHEN codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
            WHEN (codice_oggetto LIKE '697%' OR codice_oggetto LIKE '381%' OR codice_oggetto LIKE 'RB1%') AND lotto NOT IN ('97','98','99') THEN 'Poste'
            WHEN (codice_oggetto LIKE '697%' OR codice_oggetto LIKE '381%' OR codice_oggetto LIKE 'RB1%') AND lotto IN ('97','98','99') THEN 'FSU'
            ELSE recapitista_unificato
        END AS recapitista_unif
    FROM send.gold_postalizzazione_analytics
    --WHERE requesttimestamp >= DATE_SUB(NOW(),  INTERVAL 3 MONTH) --- da decommentare per fare l'estrazione ultimi3mesi
) SELECT
       car.regione,
       gpa.prodotto,
       CASE
       WHEN gpa.recapitista_unif = 'FSU' AND gpa.prodotto = 'AR' THEN 'FSU - AR'
       WHEN gpa.recapitista_unif = 'FSU' AND gpa.prodotto = '890' THEN 'FSU - 890'
       WHEN gpa.recapitista_unif = 'FSU' AND gpa.prodotto = 'RS' THEN 'FSU - RS'
       ELSE gpa.recapitista_unif
       END AS recapitista, 
  COUNT (*) AS totale_affidi,    
 (COUNT(CASE WHEN gpa.flag_consegnate = 1 OR gpa.flag_consegnate_in_giacenza = 1 THEN gpa.iun END)) AS consegnate, 
  ROUND((COUNT(CASE WHEN gpa.flag_consegnate = 1 OR gpa.flag_consegnate_in_giacenza = 1 THEN gpa.iun END) / COUNT(*)),3) AS percentuale_consegnate, 
 (COUNT(CASE WHEN gpa.flag_consegnate = 1 THEN gpa.iun END)) AS consegna_diretta,
  ROUND((COUNT(CASE WHEN gpa.flag_consegnate = 1 THEN gpa.iun END) / COUNT(*)),3) AS percentuale_consegna_diretta,
 (COUNT(CASE WHEN gpa.flag_consegnate_in_giacenza = 1 THEN gpa.iun END)) AS consegna_in_giacenza,
  ROUND((COUNT(CASE WHEN gpa.flag_consegnate_in_giacenza = 1 THEN gpa.iun END) / COUNT(*)),3) AS percentuale_consegna_in_giacenza, 
 (COUNT(CASE WHEN gpa.flag_mancata_consegna_in_giacenza = 1 OR gpa.flag_mancato_recapito = 1 THEN gpa.iun END)) AS mancata_consegna,
  ROUND((COUNT(CASE WHEN gpa.flag_mancata_consegna_in_giacenza = 1 OR gpa.flag_mancato_recapito = 1 THEN gpa.iun END) / COUNT(*)),3) AS percentuale_mancata_consegna,
 (COUNT(CASE WHEN gpa.flag_compiuta_giacenza = 1 THEN gpa.iun END)) AS compiuta_giacenza,
  ROUND((COUNT(CASE WHEN gpa.flag_compiuta_giacenza = 1 THEN gpa.iun END ) / COUNT(*)),3) AS percentuale_compiuta_giacenza,
 (COUNT(CASE WHEN gpa.flag_irreperibile = 1 THEN gpa.iun END)) AS irreperibilita,
  ROUND((COUNT(CASE WHEN gpa.flag_irreperibile = 1 THEN gpa.iun END) / COUNT(*)),3) AS percentuale_irreperibilita,
 (COUNT(CASE WHEN gpa.flag_giacenza_in_corso = 1 OR gpa.flag_non_inviato = 1 THEN gpa.iun END))  AS in_corso,
  ROUND((COUNT(CASE WHEN gpa.flag_giacenza_in_corso = 1 OR gpa.flag_non_inviato = 1 THEN gpa.iun END)/ COUNT(*)),3)  AS percentuale_in_corso
FROM dati_gold_corretti  gpa
JOIN send_dev.cap_area_provincia_regione car
ON gpa.geokey = car.cap
WHERE  
   gpa.flag_prese_in_carico = 1 and gpa.prodotto IN ('890', 'AR')
   AND requestid NOT IN (SELECT requestid_computed FROM send.silver_postalizzazione_denormalized WHERE statusrequest IN ('PN999', 'PN998'))  --fixed
GROUP BY car.regione, gpa.prodotto, 
  CASE
   WHEN gpa.recapitista_unif = 'FSU' AND gpa.prodotto = 'AR' THEN 'FSU - AR'
   WHEN gpa.recapitista_unif = 'FSU' AND gpa.prodotto = '890' THEN 'FSU - 890'
   WHEN gpa.recapitista_unif = 'FSU' AND gpa.prodotto = 'RS' THEN 'FSU - RS'
   ELSE gpa.recapitista_unif
  END
ORDER by gpa.prodotto, car.regione;


--------------------------------------- QUERY 2 - Andamento Regionale ente INPS e prodotto AR

WITH dati_gold_corretti AS (
    SELECT 
        *,
        CASE
            WHEN codice_oggetto LIKE 'R14%' THEN 'Fulmine'
            WHEN codice_oggetto LIKE '777%' OR codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
            WHEN codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
            WHEN (codice_oggetto LIKE '697%' OR codice_oggetto LIKE '381%' OR codice_oggetto LIKE 'RB1%') AND lotto NOT IN ('97','98','99') THEN 'Poste'
            WHEN (codice_oggetto LIKE '697%' OR codice_oggetto LIKE '381%' OR codice_oggetto LIKE 'RB1%') AND lotto IN ('97','98','99') THEN 'FSU'
            ELSE recapitista_unificato
        END AS recapitista_unif
    FROM send.gold_postalizzazione_analytics
    WHERE requesttimestamp >= DATE_SUB(NOW(),  INTERVAL 3 MONTH) --- da decommentare per fare l'estrazione ultimi3mesi
)
SELECT
       car.regione,
       CASE
       WHEN gpa.recapitista_unif = 'FSU' AND gpa.prodotto = 'AR' THEN 'FSU - AR'
       WHEN gpa.recapitista_unif = 'FSU' AND gpa.prodotto = '890' THEN 'FSU - 890'
       WHEN gpa.recapitista_unif = 'FSU' AND gpa.prodotto = 'RS' THEN 'FSU - RS'
       ELSE gpa.recapitista_unif
       END AS recapitista, 
  COUNT (*) AS totale_affidi,    
 (COUNT(CASE WHEN gpa.flag_consegnate = 1 OR gpa.flag_consegnate_in_giacenza = 1 THEN gpa.iun END)) AS consegnate, 
  ROUND((COUNT(CASE WHEN gpa.flag_consegnate = 1 OR gpa.flag_consegnate_in_giacenza = 1 THEN gpa.iun END) / COUNT(*)),3) AS percentuale_consegnate, 
 (COUNT(CASE WHEN gpa.flag_consegnate = 1 THEN gpa.iun END)) AS consegna_diretta,
  ROUND((COUNT(CASE WHEN gpa.flag_consegnate = 1 THEN gpa.iun END) / COUNT(*)),3) AS percentuale_consegna_diretta,
 (COUNT(CASE WHEN gpa.flag_consegnate_in_giacenza = 1 THEN gpa.iun END)) AS consegna_in_giacenza,
  ROUND((COUNT(CASE WHEN gpa.flag_consegnate_in_giacenza = 1 THEN gpa.iun END) / COUNT(*)),3) AS percentuale_consegna_in_giacenza, 
 (COUNT(CASE WHEN gpa.flag_mancata_consegna_in_giacenza = 1 OR gpa.flag_mancato_recapito = 1 THEN gpa.iun END)) AS mancata_consegna,
  ROUND((COUNT(CASE WHEN gpa.flag_mancata_consegna_in_giacenza = 1 OR gpa.flag_mancato_recapito = 1 THEN gpa.iun END) / COUNT(*)),3) AS percentuale_mancata_consegna,
 (COUNT(CASE WHEN gpa.flag_compiuta_giacenza = 1 THEN gpa.iun END)) AS compiuta_giacenza,
  ROUND((COUNT(CASE WHEN gpa.flag_compiuta_giacenza = 1 THEN gpa.iun END ) / COUNT(*)),3) AS percentuale_compiuta_giacenza,
 (COUNT(CASE WHEN gpa.flag_irreperibile = 1 THEN gpa.iun END)) AS irreperibilita,
  ROUND((COUNT(CASE WHEN gpa.flag_irreperibile = 1 THEN gpa.iun END) / COUNT(*)),3) AS percentuale_irreperibilita,
 (COUNT(CASE WHEN gpa.flag_giacenza_in_corso = 1 OR gpa.flag_non_inviato = 1 THEN gpa.iun END))  AS in_corso,
  ROUND((COUNT(CASE WHEN gpa.flag_giacenza_in_corso = 1 OR gpa.flag_non_inviato = 1 THEN gpa.iun END)/ COUNT(*)),3)  AS percentuale_in_corso
FROM dati_gold_corretti gpa
JOIN send_dev.cap_area_provincia_regione car
ON gpa.geokey = car.cap
WHERE  
  gpa.flag_prese_in_carico = 1 AND senderpaid ='53b40136-65f2-424b-acfb-7fae17e35c60' AND gpa.prodotto = 'AR'
  AND requestid NOT IN (SELECT requestid_computed FROM send.silver_postalizzazione_denormalized WHERE statusrequest IN ('PN999', 'PN998'))  --fixed
GROUP BY car.regione, 
  CASE
   WHEN gpa.recapitista_unif = 'FSU' AND gpa.prodotto = 'AR' THEN 'FSU - AR'
   WHEN gpa.recapitista_unif = 'FSU' AND gpa.prodotto = '890' THEN 'FSU - 890'
   WHEN gpa.recapitista_unif = 'FSU' AND gpa.prodotto = 'RS' THEN 'FSU - RS'
   ELSE gpa.recapitista_unif
  END
ORDER by car.regione;



--------------------------- QUERY 3 - Andamento Fornitori ente INPS e prodotto AR

WITH dati_gold_corretti AS (
    SELECT 
        *,
        CASE
            WHEN codice_oggetto LIKE 'R14%' THEN 'Fulmine'
            WHEN codice_oggetto LIKE '777%' OR codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
            WHEN codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
            WHEN (codice_oggetto LIKE '697%' OR codice_oggetto LIKE '381%' OR codice_oggetto LIKE 'RB1%') AND lotto NOT IN ('97','98','99') THEN 'Poste'
            WHEN (codice_oggetto LIKE '697%' OR codice_oggetto LIKE '381%' OR codice_oggetto LIKE 'RB1%') AND lotto IN ('97','98','99') THEN 'FSU'
            ELSE recapitista_unificato
        END AS recapitista_unif
    FROM send.gold_postalizzazione_analytics
    --WHERE requesttimestamp >= DATE_SUB(NOW(),  INTERVAL 3 MONTH) --- da decommentare per fare l'estrazione ultimi3mesi
)
SELECT
       CASE
       WHEN gpa.recapitista_unif = 'FSU' AND gpa.prodotto = 'AR' THEN 'FSU - AR'
       WHEN gpa.recapitista_unif = 'FSU' AND gpa.prodotto = '890' THEN 'FSU - 890'
       WHEN gpa.recapitista_unif = 'FSU' AND gpa.prodotto = 'RS' THEN 'FSU - RS'
       ELSE gpa.recapitista_unif
       END AS recapitista, 
  COUNT (*) AS totale_affidi,    
 (COUNT(CASE WHEN gpa.flag_consegnate = 1 OR gpa.flag_consegnate_in_giacenza = 1 THEN gpa.iun END)) AS consegnate, 
  ROUND((COUNT(CASE WHEN gpa.flag_consegnate = 1 OR gpa.flag_consegnate_in_giacenza = 1 THEN gpa.iun END) / COUNT(*)),3) AS percentuale_consegnate, 
 (COUNT(CASE WHEN gpa.flag_consegnate = 1 THEN gpa.iun END)) AS consegna_diretta,
  ROUND((COUNT(CASE WHEN gpa.flag_consegnate = 1 THEN gpa.iun END) / COUNT(*)),3) AS percentuale_consegna_diretta,
 (COUNT(CASE WHEN gpa.flag_consegnate_in_giacenza = 1 THEN gpa.iun END)) AS consegna_in_giacenza,
  ROUND((COUNT(CASE WHEN gpa.flag_consegnate_in_giacenza = 1 THEN gpa.iun END) / COUNT(*)),3) AS percentuale_consegna_in_giacenza, 
 (COUNT(CASE WHEN gpa.flag_mancata_consegna_in_giacenza = 1 OR gpa.flag_mancato_recapito = 1 THEN gpa.iun END)) AS mancata_consegna,
  ROUND((COUNT(CASE WHEN gpa.flag_mancata_consegna_in_giacenza = 1 OR gpa.flag_mancato_recapito = 1 THEN gpa.iun END) / COUNT(*)),3) AS percentuale_mancata_consegna,
 (COUNT(CASE WHEN gpa.flag_compiuta_giacenza = 1 THEN gpa.iun END)) AS compiuta_giacenza,
  ROUND((COUNT(CASE WHEN gpa.flag_compiuta_giacenza = 1 THEN gpa.iun END ) / COUNT(*)),3) AS percentuale_compiuta_giacenza,
 (COUNT(CASE WHEN gpa.flag_irreperibile = 1 THEN gpa.iun END)) AS irreperibilita,
  ROUND((COUNT(CASE WHEN gpa.flag_irreperibile = 1 THEN gpa.iun END) / COUNT(*)),3) AS percentuale_irreperibilita,
 (COUNT(CASE WHEN gpa.flag_giacenza_in_corso = 1 OR gpa.flag_non_inviato = 1 THEN gpa.iun END))  AS in_corso,
  ROUND((COUNT(CASE WHEN gpa.flag_giacenza_in_corso = 1 OR gpa.flag_non_inviato = 1 THEN gpa.iun END)/ COUNT(*)),3)  AS percentuale_in_corso
FROM dati_gold_corretti  gpa
WHERE  gpa.flag_prese_in_carico = 1 AND gpa.senderpaid ='53b40136-65f2-424b-acfb-7fae17e35c60' AND gpa.prodotto = 'AR'
       AND requestid NOT IN (SELECT requestid_computed FROM send.silver_postalizzazione_denormalized WHERE statusrequest IN ('PN999', 'PN998'))  --fixed
GROUP BY 
  CASE
   WHEN gpa.recapitista_unif = 'FSU' AND gpa.prodotto = 'AR' THEN 'FSU - AR'
   WHEN gpa.recapitista_unif = 'FSU' AND gpa.prodotto = '890' THEN 'FSU - 890'
   WHEN gpa.recapitista_unif = 'FSU' AND gpa.prodotto = 'RS' THEN 'FSU - RS'
   ELSE gpa.recapitista_unif
  END;