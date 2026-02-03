----------------------- Scarico completo dei key client con recapitista
WITH vista_conteggio AS (
    SELECT senderpaid,
           recapitista_unificato,
           COUNT(*) AS totale_affidi,
           COUNT(CASE WHEN fine_recapito_stato IS NOT NULL THEN 1 END) AS completate,
           COUNT(CASE WHEN fine_recapito_stato IS NULL THEN 1 END) AS totale_in_corso,
           COUNT(CASE WHEN fine_recapito_stato IS NULL AND flag_wi7_report_postalizzazioni_incomplete = 0 THEN 1 END) AS in_corso,
           COUNT(CASE WHEN flag_wi7_report_postalizzazioni_incomplete = 1 AND DATEDIFF(NOW(), requesttimestamp) <= 30 THEN 1 END) AS fuori_sla_un_mese,
           COUNT(CASE WHEN flag_wi7_report_postalizzazioni_incomplete = 1 AND DATEDIFF(NOW(), requesttimestamp) > 30 AND DATEDIFF(NOW(), requesttimestamp) <= 60 THEN 1 END) AS fuori_sla_due_mesi,
           COUNT(CASE WHEN flag_wi7_report_postalizzazioni_incomplete = 1 AND DATEDIFF(NOW(), requesttimestamp) > 60 THEN 1 END) AS fuori_sla_piu_due_mesi
    FROM send.gold_postalizzazione_analytics
    WHERE accettazione_recapitista_data IS NOT NULL 
    GROUP BY senderpaid
             ,recapitista_unificato
), selfcare_view AS (
  SELECT DISTINCT internalistitutionid as paid,
     CONCAT_WS(' - ', institution.rootParent.description, institution.description) as senderdenomination
   FROM
     selfcare.silver_contracts
   WHERE
     product = 'prod-pn' AND state = 'ACTIVE' 
) SELECT v.senderpaid,
         s.senderdenomination,
         v.recapitista_unificato,
         v.totale_affidi,
         v.completate,
         v.totale_in_corso,
         v.in_corso,
         v.fuori_sla_un_mese,
         v.fuori_sla_due_mesi,
         v.fuori_sla_piu_due_mesi,
         ((v.fuori_sla_un_mese + v.fuori_sla_due_mesi + v.fuori_sla_piu_due_mesi) / NULLIF((v.totale_in_corso), 0)) AS percentuale_fuori_sla_totale_in_corso
FROM vista_conteggio v LEFT JOIN selfcare_view s ON (v.senderpaid = s.paid)
ORDER BY percentuale_fuori_sla_totale_in_corso DESC
;


----------------------- Scarico completo dei Key client (unico filtro: +1000 tot. in corso)
WITH vista_conteggio AS (
    SELECT senderpaid,
           COUNT(*) AS totale_affidi,
           COUNT(CASE WHEN fine_recapito_stato IS NOT NULL THEN 1 END) AS completate,
           COUNT(CASE WHEN fine_recapito_stato IS NULL THEN 1 END) AS totale_in_corso,
           COUNT(CASE WHEN fine_recapito_stato IS NULL AND flag_wi7_report_postalizzazioni_incomplete = 0 THEN 1 END) AS in_corso,
           COUNT(CASE WHEN flag_wi7_report_postalizzazioni_incomplete = 1 AND DATEDIFF(NOW(), requesttimestamp) <= 30 THEN 1 END) AS fuori_sla_un_mese,
           COUNT(CASE WHEN flag_wi7_report_postalizzazioni_incomplete = 1 AND DATEDIFF(NOW(), requesttimestamp) > 30 AND DATEDIFF(NOW(), requesttimestamp) <= 60 THEN 1 END) AS fuori_sla_due_mesi,
           COUNT(CASE WHEN flag_wi7_report_postalizzazioni_incomplete = 1 AND DATEDIFF(NOW(), requesttimestamp) > 60 THEN 1 END) AS fuori_sla_piu_due_mesi
    FROM send.gold_postalizzazione_analytics
    WHERE accettazione_recapitista_data IS NOT NULL 
    GROUP BY senderpaid
), selfcare_view AS (
  SELECT DISTINCT internalistitutionid as paid,
     CONCAT_WS(' - ', institution.rootParent.description, institution.description) as senderdenomination
   FROM
     selfcare.silver_contracts
   WHERE
     product = 'prod-pn' AND state = 'ACTIVE' 
) SELECT v.senderpaid,
         s.senderdenomination,
         v.totale_affidi,
         v.completate,
         v.totale_in_corso,
         v.in_corso,
         v.fuori_sla_un_mese,
         v.fuori_sla_due_mesi,
         v.fuori_sla_piu_due_mesi,
         ((v.fuori_sla_un_mese + v.fuori_sla_due_mesi + v.fuori_sla_piu_due_mesi) / NULLIF((v.totale_in_corso), 0)) AS percentuale_fuori_sla_totale_in_corso
FROM vista_conteggio v LEFT JOIN selfcare_view s ON (v.senderpaid = s.paid)
WHERE totale_in_corso >= 1000
ORDER BY percentuale_fuori_sla_totale_in_corso DESC;


----- Versione modificata --- Key client fuori SLA/Affidi

WITH filtro_gold AS (
    SELECT *
    FROM send.gold_postalizzazione_analytics
    WHERE accettazione_recapitista_data IS NOT NULL
      AND (
          CASE 
               WHEN accettazione_recapitista_CON018_data IS NOT NULL THEN accettazione_recapitista_CON018_data
               ELSE affido_recapitista_con016_data
          END
      ) >= '2024-12-01 00:00:00.000'
), vista_conteggio AS (
    SELECT senderpaid,
           COUNT(*) AS totale_affidi,
           COUNT(CASE WHEN fine_recapito_stato IS NOT NULL THEN 1 END) AS completate,
           COUNT(CASE WHEN fine_recapito_stato IS NULL THEN 1 END) AS totale_in_corso,
           COUNT(CASE WHEN fine_recapito_stato IS NULL AND flag_wi7_report_postalizzazioni_incomplete = 0 THEN 1 END) AS in_corso,
           COUNT(CASE WHEN flag_wi7_report_postalizzazioni_incomplete = 1 AND DATEDIFF(NOW(), requesttimestamp) <= 30 THEN 1 END) AS fuori_sla_un_mese,
           COUNT(CASE WHEN flag_wi7_report_postalizzazioni_incomplete = 1 AND DATEDIFF(NOW(), requesttimestamp) > 30 AND DATEDIFF(NOW(), requesttimestamp) <= 60 THEN 1 END) AS fuori_sla_due_mesi,
           COUNT(CASE WHEN flag_wi7_report_postalizzazioni_incomplete = 1 AND DATEDIFF(NOW(), requesttimestamp) > 60 THEN 1 END) AS fuori_sla_piu_due_mesi
    FROM filtro_gold
    GROUP BY senderpaid
), selfcare_view AS (
    SELECT DISTINCT internalistitutionid AS paid,
           CONCAT_WS(' - ', institution.rootParent.description, institution.description) AS senderdenomination
    FROM selfcare.silver_contracts
    WHERE product = 'prod-pn' AND state = 'ACTIVE'
) SELECT v.senderpaid,
       s.senderdenomination,
       v.totale_affidi,
       v.completate,
       v.totale_in_corso,
       v.in_corso,
       v.fuori_sla_un_mese,
       v.fuori_sla_due_mesi,
       v.fuori_sla_piu_due_mesi,
       ((v.fuori_sla_un_mese + v.fuori_sla_due_mesi + v.fuori_sla_piu_due_mesi) / NULLIF(v.totale_in_corso, 0)) AS percentuale_fuori_sla_totale_in_corso
FROM vista_conteggio v
LEFT JOIN selfcare_view s ON v.senderpaid = s.paid
ORDER BY percentuale_fuori_sla_totale_in_corso DESC;
