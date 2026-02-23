--------------------------- ESTRAZIONE WORK ITEM 7
----- se l'enteid è critico, se la notifica è perfezionata non deve essere segnalata come critica
----- questa regola vale per tutti gli enti tranne INPS (a prescindere che siano perfezionate o mento -- tutte come critiche)
WITH
    dati_gold_corretti AS (
        SELECT
            *,
            CASE
                WHEN codice_oggetto LIKE 'R14%' THEN 'Fulmine'
                WHEN codice_oggetto LIKE '777%'
                OR codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
                WHEN codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
                WHEN codice_oggetto LIKE '69%'
                OR codice_oggetto LIKE '381%'
                OR codice_oggetto LIKE 'RB1%' THEN 'Poste'
                ELSE recapitista_unificato
            END AS recapitista_unif
        FROM
            send.gold_postalizzazione_analytics
    ),
    temp AS (
        SELECT
            *,
            --Inserimento data con FIX
            COALESCE(
                LEAST (
                    accettazione_recapitista_con018_data,
                    affido_recapitista_con016_data
                ),
                accettazione_recapitista_con018_data,
                affido_recapitista_con016_data,
                requesttimestamp
            ) AS affido_accettazione_rec_data
        FROM
            dati_gold_corretti
    )
SELECT
    senderpaid,
    iun,
    d.requestID,
    requesttimestamp AS requestDateTime,
    prodotto,
    --geokey,
    CONCAT ("'", geokey) AS geokey,
    region_name AS regione,
    province_name AS provincia,
    recapitista_unif,
    lotto,
    --codice_oggetto AS codiceOggetto, --Per Fulmine
    CONCAT ("'", codice_oggetto) AS codiceOggetto,
    --Per altri Recapitisti
    affido_consolidatore_data,
    stampa_imbustamento_CON080_data,
    affido_recapitista_CON016_data,
    accettazione_recapitista_CON018_data,
    --affido_accettazione_rec_data,
    SUBSTRING( CAST(affido_accettazione_rec_data AS STRING), 1, 7 ) AS mese_anno_accettazione,
    SUBSTRING( CAST(affido_accettazione_rec_data AS STRING), 1, 4 ) AS anno_accettazione,
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
    -- Elenco di senderpaid critici passati da Giuseppe di settimana in settimana
    CASE
        WHEN senderpaid IN (
            '135100c9-d464-4abf-a9b1-a10f5d7903b7',
            '051c98e9-40ce-43a6-aa6e-2123889e1532',
            'f7c5a16a-be9e-4890-ae31-2d651a2529e7',
            '2f84ae92-0ec5-44ec-bd3a-1d9d727fd777',
            'a712e6b9-d037-497d-9fbc-6fe734d5c91f',
            '8bdd616c-6130-4f8b-b450-1035714433b5',
            '6ba7b43f-756c-4a99-af13-37c996c3a061',
            '31a04b78-72b6-4dc4-99a9-786f954235a8',
            '980fabc0-f073-42b1-b9ac-a0b86581960a',
            '1d8c397c-58ed-4bb8-a5b9-c368f48423f7',
            '76fd95f3-c1a1-410f-95c8-a6ac00989aae',
            'f04ea5f0-bae1-4277-b604-07c67b6cb83b',
            '9a7c1b23-46a3-489b-8ed4-398ffb32b45a',
            'def76838-2285-42f3-a403-4949bd027102',
            'ac4cf7ee-35d2-44ee-9f25-a99b11b22a4a',
            'b34aad56-ee4a-4e3b-b37b-17fe50697b8d',
            '8e9ee634-31a1-4406-9ad8-70e2a4c5b637'
            --'53b40136-65f2-424b-acfb-7fae17e35c60'-- gestito nel secondo case when INPS
        )
        AND perfezionamento_data IS NULL
        AND e.requestid IS NULL THEN 'Critico'
        -- fixed AND e.requestid IS NULL
        WHEN senderpaid = '53b40136-65f2-424b-acfb-7fae17e35c60'
        AND e.requestid IS NULL THEN 'Critico'
        -- fixed AND e.requestid IS NULL
    END AS criticita,
    flag_wi7_report_postalizzazioni_incomplete,
    wi7_cluster,
    CASE
        WHEN senderpaid = '53b40136-65f2-424b-acfb-7fae17e35c60' THEN 'SI'
        ELSE 'NO'
    END AS inps
FROM
    temp d
    LEFT JOIN send_dev.wi7_poste_da_escludere e ON d.requestid = e.requestid
WHERE
    flag_wi7_report_postalizzazioni_incomplete = 1
    --fin qui estrazione WI7 completa, se serve il singolo recapitista aggiungere:
    AND
    --recapitista_unif = 'Fulmine'
    --recapitista_unif = 'POST & SERVICE'
    --recapitista_unif = 'Poste'
    recapitista_unif = 'RTI Sailpost-Snem'
    --AND senderpaid = 'ac4cf7ee-35d2-44ee-9f25-a99b11b22a4a' --INPS
    AND statusrequest NOT IN ('PN999', 'PN998');