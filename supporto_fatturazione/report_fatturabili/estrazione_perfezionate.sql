SELECT DISTINCT g.iun,
    g.requestid,
    g.requesttimestamp,
    g.prodotto,
    g.geokey,
    c.area AS zona,
    c.provincia,
    c.regione,
    COALESCE(i.recapitista_corretto, g.recapitista) AS recapitista,
    COALESCE(i.recapitista_unificato_corretto, g.recapitista_unificato) AS recapitista_unificato,
    COALESCE(i.lotto_corretto, CAST(g.lotto AS INT)) AS lotto,
    --g.codice_oggetto, --Per Fulmine
    CONCAT ("'", g.codice_oggetto) AS codiceOggetto, --Per altri Recapitisti
    g.prezzo_ente,
    g.grammatura,
    g.numero_pagine,
    g.costo_consolidatore,
    g.costo_recapitista,
    g.affido_consolidatore_data,
    g.stampa_imbustamento_con080_data,
    g.affido_recapitista_con016_data,
    g.accettazione_recapitista_con018_data,
    g.affido_conservato_con020_data,
    g.materialita_pronta_con09a_data,
    g.scarto_consolidatore_stato,
    g.scarto_consolidatore_data,
    g.accettazione_recapitista_data,
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
    g.demat_23l_ar_path,
    g.demat_23l_ar_data_rendicontazione,
    g.demat_plico_stato,
    g.demat_plico_path,
    g.demat_plico_data_rendicontazione,
    g.fine_recapito_stato,
    g.fine_recapito_data,
    g.fine_recapito_data_rendicontazione,
    g.perfezionamento_data,
    g.perfezionamento_tipo,
    g.perfezionamento_notificationdate,
    g.perfezionamento_stato,
    g.perfezionamento_stato_dettagli,
    g.flag_wi7_report_postalizzazioni_incomplete,
    g.wi7_cluster,
    g.tms_cancelled,
    g.statusrequest
FROM send.gold_postalizzazione_analytics g
     LEFT JOIN send_dev.cap_area_provincia_regione c ON (g.geokey = c.cap) 
     LEFT JOIN send_dev.temp_incident i ON (g.requestid = i.requestid)
WHERE g.scarto_consolidatore_stato IS NULL
  AND g.pcretry_rank = 1
  AND g.recapitista_unificato = '${recapitista}'
  AND g.statusrequest NOT IN ('PN998')
  AND (g.fine_recapito_stato NOT IN ('RECRS006', 'RECRS013','RECRN006', 'RECRN013', 'RECAG004', 'RECAG013') OR g.fine_recapito_stato IS NULL)       --- esclusione furtati/smarriti/non rend. ...
  AND (YEAR(g.perfezionamento_data) = ${anno} AND QUARTER(g.perfezionamento_data) = ${trimestre}                                                      -- perfezionate q2 2025
       OR YEAR(g.tms_cancelled) = ${anno}  AND QUARTER(g.tms_cancelled) = ${trimestre}                                                                -- oppure cancellate nel q2 2025
       OR g.certificazione_recapito_dettagli = 'M02' AND g.fine_recapito_stato IS NOT NULL AND YEAR(g.fine_recapito_data_rendicontazione) = ${anno} -- oppure completate nel q2 2025 con destinatario deceduto
       AND QUARTER(g.fine_recapito_data_rendicontazione) =  ${trimestre})
;