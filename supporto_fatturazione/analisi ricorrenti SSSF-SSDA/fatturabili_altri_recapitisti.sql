WITH temp_parametri_estrazione AS (
    SELECT
        2026 AS anno_riferimento,
        1 AS quarter_riferimento,
        'RTI Sailpost-Snem' AS recapitista_riferimento
),
temp_perimetro_oggetti AS (
    SELECT DISTINCT
        s.requestid,
        s.iun
    FROM send.gold_postalizzazione_analytics s
    CROSS JOIN temp_parametri_estrazione p
    WHERE
        s.scarto_consolidatore_stato IS NULL
        AND s.pcretry_rank = 1
        AND (
            CASE
                WHEN s.codice_oggetto LIKE 'R14%' THEN 'Fulmine'
                WHEN s.codice_oggetto LIKE '777%' OR s.codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
                WHEN s.codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
                WHEN s.codice_oggetto LIKE '69%' OR s.codice_oggetto LIKE '381%' OR s.codice_oggetto LIKE 'RB1%' THEN 'Poste'
                ELSE s.recapitista_unificato
            END
        ) = p.recapitista_riferimento
        AND s.statusrequest NOT IN ('PN998')
        AND (
            s.fine_recapito_stato NOT IN ('RECRS006', 'RECRS013','RECRN006', 'RECRN013', 'RECAG004', 'RECAG013')
            OR s.fine_recapito_stato IS NULL
        )
        AND (
            (
                YEAR(s.perfezionamento_data) = p.anno_riferimento
                AND QUARTER(s.perfezionamento_data) = p.quarter_riferimento
            )
            OR (
                YEAR(s.tms_cancelled) = p.anno_riferimento
                AND QUARTER(s.tms_cancelled) = p.quarter_riferimento
            )
            OR (
                s.certificazione_recapito_dettagli = 'M02'
                AND s.fine_recapito_stato IS NOT NULL
                AND YEAR(s.fine_recapito_data_rendicontazione) = p.anno_riferimento
                AND QUARTER(s.fine_recapito_data_rendicontazione) = p.quarter_riferimento
            )
        )
),
temp_demat AS (
    SELECT
        REGEXP_REPLACE(p.requestid, '^pn-cons-000~', '') AS requestid,
        a.item.documenttype AS documenttype,
        ROW_NUMBER() OVER (
            PARTITION BY REGEXP_REPLACE(p.requestid, '^pn-cons-000~', '')
            ORDER BY e.paperprogrstatus.clientrequesttimestamp DESC
        ) AS rn_demat
    FROM
        send.silver_postalizzazione p
        JOIN temp_perimetro_oggetti po
            ON REGEXP_REPLACE(p.requestid, '^pn-cons-000~', '') = po.requestid,
        p.eventslist e,
        e.item.paperprogrstatus.attachments a
    WHERE a.item.documenttype IN ('23L', 'AR')
),
temp_silver_postalizzazione AS (
    SELECT
        REGEXP_REPLACE(t.requestid, '^pn-cons-000~', '') AS requestid,
        e.paperprogrstatus.statuscode AS statuscode,
        CASE
            WHEN LENGTH(e.paperprogrstatus.statusdatetime) = 17 THEN CONCAT(
                SUBSTR(e.paperprogrstatus.statusdatetime, 1, 16),
                ':00Z'
            )
            ELSE e.paperprogrstatus.statusdatetime
        END AS statusdatetime,
        CASE
            WHEN LENGTH(e.paperprogrstatus.clientrequesttimestamp) = 17 THEN CONCAT(
                SUBSTR(e.paperprogrstatus.clientrequesttimestamp, 1, 16),
                ':00.000Z'
            )
            ELSE e.paperprogrstatus.clientrequesttimestamp
        END AS clientrequesttimestamp,
        CASE
            WHEN e.paperprogrstatus.statuscode IN (
                'RECRN003C','RECRN004C','RECRN005C','RECRN001C','RECRN002C','RECRN002F','RECAG001C',
                'RECAG002C','RECAG003C','RECAG003F','RECAG005C','RECAG006C','RECAG007C','RECAG008C'
            ) THEN 'FINE_RECAPITO'
            WHEN e.paperprogrstatus.statuscode IN (
                'RECRN003A','RECRN004A','RECRN005A','RECRN001A','RECRN002A','RECRN002D','RECAG001A',
                'RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A','RECAG007A','RECAG008A'
            ) THEN 'CERTIFICAZIONE_RECAPITO'
            ELSE NULL
        END AS tipo,
        ROW_NUMBER() OVER (
            PARTITION BY
                REGEXP_REPLACE(t.requestid, '^pn-cons-000~', ''),
                CASE
                    WHEN e.paperprogrstatus.statuscode IN (
                        'RECRN003C','RECRN004C','RECRN005C','RECRN001C','RECRN002C','RECRN002F','RECAG001C',
                        'RECAG002C','RECAG003C','RECAG003F','RECAG005C','RECAG006C','RECAG007C','RECAG008C'
                    ) THEN 'FINE_RECAPITO'
                    WHEN e.paperprogrstatus.statuscode IN (
                        'RECRN003A','RECRN004A','RECRN005A','RECRN001A','RECRN002A','RECRN002D','RECAG001A',
                        'RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A','RECAG007A','RECAG008A'
                    ) THEN 'CERTIFICAZIONE_RECAPITO'
                END
            ORDER BY e.paperprogrstatus.clientrequesttimestamp DESC
        ) AS rn
    FROM
        send.silver_postalizzazione t
        JOIN temp_perimetro_oggetti po
            ON REGEXP_REPLACE(t.requestid, '^pn-cons-000~', '') = po.requestid,
        t.eventslist e
    WHERE
        e.paperprogrstatus.statuscode IN (
            'RECRN001A','RECRN002A','RECRN002D','RECRN003A','RECRN004A','RECRN005A','RECAG001A','RECAG002A',
            'RECAG003A','RECAG003D','RECAG005A','RECAG006A','RECAG007A','RECAG008A','RECRN001C','RECRN002C',
            'RECRN002F','RECRN003C','RECRN004C','RECRN005C','RECAG001C','RECAG002C','RECAG003C','RECAG003F',
            'RECAG005C','RECAG006C','RECAG007C','RECAG008C'
        )
        AND e.paperprogrstatus.statuscode NOT IN ('PN999','PN998')
),
temp_ultimi_eventi_silver_postalizzazione AS (
    SELECT
        requestid,
        MAX(CASE WHEN tipo = 'FINE_RECAPITO' THEN statuscode END) AS fine_recapito_stato,
        MAX(CASE WHEN tipo = 'FINE_RECAPITO' THEN statusdatetime END) AS fine_recapito_data,
        MAX(CASE WHEN tipo = 'FINE_RECAPITO' THEN clientrequesttimestamp END) AS fine_recapito_rendicontazione,
        MAX(CASE WHEN tipo = 'CERTIFICAZIONE_RECAPITO' THEN statuscode END) AS certificazione_recapito_stato,
        MAX(CASE WHEN tipo = 'CERTIFICAZIONE_RECAPITO' THEN statusdatetime END) AS certificazione_recapito_data,
        MAX(CASE WHEN tipo = 'CERTIFICAZIONE_RECAPITO' THEN clientrequesttimestamp END) AS certificazione_recapito_rendicontazione
    FROM temp_silver_postalizzazione
    WHERE rn = 1
    GROUP BY requestid
),
temp_analog_attempt AS (
    SELECT DISTINCT
        tl.iun,
        CAST(regexp_extract(tl.timelineelementid, 'ATTEMPT_([0-9]+)', 1) AS INT) AS attempt_number_timeline
    FROM send.silver_timeline tl
    JOIN temp_perimetro_oggetti po
        ON tl.iun = po.iun
    WHERE tl.category = 'SEND_ANALOG_FEEDBACK'
), 
temp_schedule_refinement AS (
    SELECT DISTINCT
        tl.iun,
        1 AS flag_schedule_refinement
    FROM send.silver_timeline tl
    JOIN temp_perimetro_oggetti po
        ON tl.iun = po.iun
    WHERE tl.category = 'SCHEDULE_REFINEMENT'
),
temp_controlli AS (
    SELECT
        s.senderpaid,
        sn.senderdenomination,
        s.iun,
        s.data_deposito,
        n.actual_status,
        s.requestid,
        s.requesttimestamp,
        s.prodotto,
        CONCAT("'", s.geokey) AS geokey,
        s.zona,
        s.grammatura,
        s.costo_recapitista,
        CASE
            WHEN s.codice_oggetto LIKE 'R14%' THEN 'Fulmine'
            WHEN s.codice_oggetto LIKE '777%' OR s.codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
            WHEN s.codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
            WHEN s.codice_oggetto LIKE '69%' OR s.codice_oggetto LIKE '381%' OR s.codice_oggetto LIKE 'RB1%' THEN 'Poste'
            ELSE s.recapitista_unificato
        END AS recapitista_unif,
        s.lotto,
        s.codice_oggetto,
        CONCAT("'", s.codice_oggetto) AS codiceOggetto,
        s.scarto_consolidatore_stato,
        s.scarto_consolidatore_data,
        s.affido_recapitista_con016_data,
        s.accettazione_recapitista_con018_data,
        CASE
            WHEN s.accettazione_recapitista_CON018_data IS NOT NULL THEN s.accettazione_recapitista_CON018_data
            ELSE s.affido_recapitista_con016_data
        END AS affido_accettazione_rec_data,
        s.tentativo_recapito_stato,
        s.tentativo_recapito_data,
        s.tentativo_recapito_data_rendicontazione,
        s.messaingiacenza_recapito_stato,
        s.messaingiacenza_recapito_data,
        s.messaingiacenza_recapito_data_rendicontazione,
        s.certificazione_recapito_stato,
        s.certificazione_recapito_dettagli,
        s.certificazione_recapito_data,
        s.certificazione_recapito_data_rendicontazione,
        s.demat_23l_ar_stato,
        s.demat_23l_ar_data_rendicontazione,
        s.demat_plico_stato,
        s.demat_plico_data_rendicontazione,
        s.demat_arcad_stato,
        s.demat_arcad_data_rendicontazione,
        s.fine_recapito_stato,
        s.fine_recapito_data,
        s.fine_recapito_data_rendicontazione,
        s.accettazione_23l_recag012_data,
        s.accettazione_23l_recag012_data_rendicontazione,
        s.recindex_number,
        s.perfezionamento_data,
        s.perfezionamento_tipo,
        s.perfezionamento_notificationdate,
        s.perfezionamento_stato,
        s.perfezionamento_stato_dettagli,
        s.ultimo_evento_stato,
        s.ultimo_evento_data,
        s.ultimo_evento_data_rendicontazione,
        s.stato_richiesta,
        LEAST(
            COALESCE(n.tms_viewed, n.tms_effective_date),
            COALESCE(n.tms_effective_date, n.tms_viewed)
        ) AS tms_perfezionamento_notification,
        COALESCE(tl.flag_schedule_refinement, 0) AS flag_schedule_refinement,
        CASE
            WHEN a.attempt_number_timeline = 0 AND CAST(s.attempt_number AS INT) = 0 THEN 1
            ELSE 0
        END AS flag_feedback_attempt_0,
        s.tms_cancelled,
        n.tms_date_payment,
        s.flag_wi7_consolidatore,
        s.flag_wi7_report_postalizzazioni_incomplete,
        s.wi7_cluster,
        s.pcretry_rank,
        s.attempt_rank,
        s.statusrequest,
        t.certificazione_recapito_stato AS certificazione_recapito_stato_silver,
        t.certificazione_recapito_data AS certificazione_recapito_data_silver,
        t.certificazione_recapito_rendicontazione AS certificazione_recapito_rendicontazione_silver,
        t.fine_recapito_stato AS fine_recapito_stato_silver,
        t.fine_recapito_data AS fine_recapito_data_silver,
        t.fine_recapito_rendicontazione AS fine_recapito_rendicontazione_silver,
        d.documenttype,
        CASE WHEN w.requestid IS NOT NULL THEN 1 ELSE 0 END AS flag_oggetto_tavoli_poste,
        CASE WHEN b.requestid IS NOT NULL THEN 1 ELSE 0 END AS flag_oggetto_bloccato_incident,
        b.gestione_economica_ciclo_passivo,
        CASE
            WHEN n.type_notif = 'MULTI' THEN 1
            ELSE 0
        END AS flag_multi_destinatario,
        CASE
            WHEN s.attempt_rank = 1 AND s.pcretry_rank = 1 THEN 1
            ELSE 0
        END AS flag_ultima_postalizzazione,
        s.flag_prodotto_estero,
        CASE
            WHEN s.certificazione_recapito_dettagli IN ('M02') THEN 1
            ELSE 0
        END AS flag_destinatario_deceduto,
        CASE
            WHEN s.flag_wi7_report_postalizzazioni_incomplete = 1 THEN 1
            ELSE 0
        END flag_fuori_sla,
        CASE
            WHEN s.certificazione_recapito_stato NOT IN ('RECRS002A','RECRN002A','RECAG003A','RECRS002D','RECRN002D','RECAG003D')
                    AND s.certificazione_recapito_dettagli IN ('M01','M03','M04','M02','M05','M06','M07','M08','M09') THEN 1
            WHEN s.certificazione_recapito_stato IN ('RECRS002A', 'RECRN002A', 'RECAG003A')
                    AND (
                        s.certificazione_recapito_dettagli NOT IN ('M02', 'M05', 'M06', 'M07', 'M08', 'M09')
                        OR s.certificazione_recapito_dettagli IS NULL
                    ) THEN 1
            WHEN s.certificazione_recapito_stato IN ('RECRS002D', 'RECRN002D', 'RECAG003D')
                    AND (
                        s.certificazione_recapito_dettagli NOT IN ('M01', 'M03', 'M04')
                        OR s.certificazione_recapito_dettagli IS NULL
                    ) THEN 1
            ELSE 0
        END AS controllo_causale,
        CASE
            WHEN s.certificazione_recapito_stato IN ('RECRN003A', 'RECRN004A', 'RECRN005A')
                AND (s.tentativo_recapito_stato = 'RECRN010' OR s.tentativo_recapito_stato IS NULL) THEN 0
            WHEN s.certificazione_recapito_stato IN ('RECRS003A', 'RECRS004A', 'RECRS005A')
                AND (s.tentativo_recapito_stato = 'RECRS010' OR s.tentativo_recapito_stato IS NULL) THEN 0
            WHEN s.certificazione_recapito_stato IN ('RECAG005A','RECAG006A','RECAG007A','RECAG008A')
                AND (s.tentativo_recapito_stato = 'RECAG010' OR s.tentativo_recapito_stato IS NULL) THEN 0
            WHEN s.certificazione_recapito_stato NOT IN (
                    'RECRN003A','RECRN004A','RECRN005A','RECAG005A','RECRS003A',
                    'RECRS004A','RECRS005A','RECAG006A','RECAG007A','RECAG008A'
                )
                AND s.tentativo_recapito_stato = s.certificazione_recapito_stato THEN 0
            ELSE 1
        END AS controllo_inesito_casi_giacenza,
        CASE
            WHEN s.certificazione_recapito_stato = 'RECRN001A'
                AND (s.demat_23l_ar_stato = 'RECRN001B' OR s.demat_plico_stato = 'RECRN001B' OR s.demat_arcad_stato = 'RECRN001B')
                AND t.fine_recapito_stato = 'RECRN001C' THEN 0
            WHEN s.certificazione_recapito_stato = 'RECRN002A'
                AND (s.demat_23l_ar_stato = 'RECRN002B' OR s.demat_plico_stato = 'RECRN002B' OR s.demat_arcad_stato = 'RECRN002B')
                AND t.fine_recapito_stato = 'RECRN002C' THEN 0
            WHEN s.certificazione_recapito_stato = 'RECRN002D'
                AND (s.demat_23l_ar_stato = 'RECRN002E' OR s.demat_plico_stato = 'RECRN002E' OR s.demat_arcad_stato = 'RECRN002E')
                AND t.fine_recapito_stato = 'RECRN002F' THEN 0
            WHEN s.certificazione_recapito_stato = 'RECRN003A'
                AND (s.demat_23l_ar_stato = 'RECRN003B' OR s.demat_plico_stato = 'RECRN003B' OR s.demat_arcad_stato = 'RECRN003B')
                AND t.fine_recapito_stato = 'RECRN003C' THEN 0
            WHEN s.certificazione_recapito_stato = 'RECRN004A'
                AND (s.demat_23l_ar_stato = 'RECRN004B' OR s.demat_plico_stato = 'RECRN004B' OR s.demat_arcad_stato = 'RECRN004B')
                AND t.fine_recapito_stato = 'RECRN004C' THEN 0
            WHEN s.certificazione_recapito_stato = 'RECRN005A'
                AND (s.demat_23l_ar_stato = 'RECRN005B' OR s.demat_plico_stato = 'RECRN005B' OR s.demat_arcad_stato = 'RECRN005B')
                AND t.fine_recapito_stato = 'RECRN005C' THEN 0
            WHEN s.certificazione_recapito_stato = 'RECAG001A'
                AND (s.demat_23l_ar_stato = 'RECAG001B' OR s.demat_plico_stato = 'RECAG001B' OR s.demat_arcad_stato = 'RECAG001B')
                AND t.fine_recapito_stato = 'RECAG001C' THEN 0
            WHEN s.certificazione_recapito_stato = 'RECAG002A'
                AND (s.demat_23l_ar_stato = 'RECAG002B' OR s.demat_plico_stato = 'RECAG002B' OR s.demat_arcad_stato = 'RECAG002B')
                AND t.fine_recapito_stato = 'RECAG002C' THEN 0
            WHEN s.certificazione_recapito_stato = 'RECAG003A'
                AND (s.demat_23l_ar_stato = 'RECAG003B' OR s.demat_plico_stato = 'RECAG003B' OR s.demat_arcad_stato = 'RECAG003B')
                AND t.fine_recapito_stato = 'RECAG003C' THEN 0
            WHEN s.certificazione_recapito_stato = 'RECAG003D'
                AND (s.demat_23l_ar_stato = 'RECAG003E' OR s.demat_plico_stato = 'RECAG003E' OR s.demat_arcad_stato = 'RECAG003E')
                AND t.fine_recapito_stato = 'RECAG003F' THEN 0
            WHEN s.certificazione_recapito_stato = 'RECAG005A'
                AND (
                    s.demat_23l_ar_stato IN ('RECAG011B', 'RECAG005B')
                    OR s.demat_plico_stato IN ('RECAG011B', 'RECAG005B')
                    OR s.demat_arcad_stato IN ('RECAG011B', 'RECAG005B')
                )
                AND t.fine_recapito_stato = 'RECAG005C' THEN 0
            WHEN s.certificazione_recapito_stato = 'RECAG006A'
                AND (
                    s.demat_23l_ar_stato IN ('RECAG011B', 'RECAG006B')
                    OR s.demat_plico_stato IN ('RECAG011B', 'RECAG006B')
                    OR s.demat_arcad_stato IN ('RECAG011B', 'RECAG006B')
                )
                AND t.fine_recapito_stato = 'RECAG006C' THEN 0
            WHEN s.certificazione_recapito_stato = 'RECAG007A'
                AND (
                    s.demat_23l_ar_stato IN ('RECAG011B', 'RECAG007B')
                    OR s.demat_plico_stato IN ('RECAG011B', 'RECAG007B')
                    OR s.demat_arcad_stato IN ('RECAG011B', 'RECAG007B')
                )
                AND t.fine_recapito_stato = 'RECAG007C' THEN 0
            WHEN s.certificazione_recapito_stato = 'RECAG008A'
                AND (
                    s.demat_23l_ar_stato IN ('RECAG011B', 'RECAG008B')
                    OR s.demat_plico_stato IN ('RECAG011B', 'RECAG008B')
                    OR s.demat_arcad_stato IN ('RECAG011B', 'RECAG008B')
                )
                AND t.fine_recapito_stato = 'RECAG008C' THEN 0
            ELSE 1
        END AS controllo_tripletta,
        CASE
            WHEN CAST(t.certificazione_recapito_data AS TIMESTAMP) = CAST(t.fine_recapito_data AS TIMESTAMP) THEN 0
            ELSE 1
        END AS controllo_date_business,
        CASE
            WHEN s.certificazione_recapito_stato = 'RECRN005A'
            AND DATEDIFF(
                CAST(s.certificazione_recapito_data AS DATE),
                CAST(s.tentativo_recapito_data AS DATE)
            ) < 30 THEN 1
            ELSE 0
        END AS controllo_tempistiche_compiuta_giacenza,
        CASE
            WHEN s.prodotto = '890' AND d.documenttype = '23L' THEN 0
            WHEN s.prodotto = 'AR' AND d.documenttype = 'AR' THEN 0
            WHEN d.documenttype IS NULL THEN 0
            ELSE 1
        END AS controllo_documentType
    FROM send.gold_postalizzazione_analytics s
    JOIN temp_perimetro_oggetti po ON s.requestid = po.requestid
    LEFT JOIN temp_demat d ON d.requestid = s.requestid AND d.rn_demat = 1
    LEFT JOIN send_dev.wi7_poste_da_escludere w ON s.requestid = w.requestid
    LEFT JOIN send_dev.oggetti_bloccati b ON s.requestid = b.requestid
    LEFT JOIN send.gold_notification_analytics n ON s.iun = n.iun
    LEFT JOIN temp_schedule_refinement tl ON s.iun = tl.iun
    LEFT JOIN send.silver_notification sn ON s.iun = sn.iun
    LEFT JOIN temp_analog_attempt a ON (
        CAST(s.attempt_number AS INT) = CAST(a.attempt_number_timeline AS INT)
        AND s.iun = a.iun
    )
    LEFT JOIN temp_ultimi_eventi_silver_postalizzazione t ON s.requestid = t.requestid
),
temp_postalizzazione AS (
    SELECT
        s.*,
        IF(
            fine_recapito_stato IN ('RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C','RECAG005C','RECAG006C','RECAG007C','RECAG008C')
            AND tentativo_recapito_stato IS NULL,
            1,
            0
        ) AS assenza_inesito,
        IF(
            fine_recapito_stato IN ('RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C','RECAG005C','RECAG006C','RECAG007C','RECAG008C')
            AND messaingiacenza_recapito_stato IS NULL,
            1,
            0
        ) AS assenza_messa_in_giacenza,
        IF(certificazione_recapito_stato IS NULL, 1, 0) AS assenza_pre_esito,
        IF(
            (
                fine_recapito_stato IN ('RECAG008C')
                AND (demat_23l_ar_stato IS NULL OR demat_plico_stato IS NULL)
            )
            OR (
                demat_23l_ar_stato IS NULL
                AND demat_plico_stato IS NULL
            ),
            1,
            0
        ) AS assenza_dematerializzazione_23l_ar_plico,
        IF(
            prodotto = '890'
            AND fine_recapito_stato IN ('RECAG005C','RECAG006C','RECAG007C','RECAG008C')
            AND demat_arcad_data_rendicontazione IS NULL,
            1,
            0
        ) AS assenza_demat_ARCAD,
        IF(
            prodotto = '890'
            AND fine_recapito_stato IN ('RECAG005C','RECAG006C','RECAG007C','RECAG008C')
            AND accettazione_23l_recag012_data IS NULL,
            1,
            0
        ) AS assenza_RECAG012
    FROM temp_controlli s
),
temp_assenza_eventi AS (
    SELECT
        *,
        IF(
            assenza_inesito = 1
            OR assenza_messa_in_giacenza = 1
            OR assenza_pre_esito = 1
            OR assenza_dematerializzazione_23l_ar_plico = 1
            OR assenza_demat_ARCAD = 1
            OR assenza_RECAG012 = 1,
            1,
            0
        ) AS flag_esiti_mancanti
    FROM temp_postalizzazione
),
finale AS (
    SELECT
        t.iun,
        t.actual_status,
        t.requestid,
        t.requesttimestamp,
        t.prodotto,
        t.geokey,
        t.zona,
        t.grammatura,
        t.costo_recapitista,
        t.recapitista_unif,
        t.lotto,
        t.codice_oggetto,
        t.codiceOggetto,
        t.scarto_consolidatore_stato,
        t.scarto_consolidatore_data,
        t.affido_recapitista_con016_data,
        t.accettazione_recapitista_con018_data,
        t.tentativo_recapito_stato,
        t.tentativo_recapito_data,
        t.tentativo_recapito_data_rendicontazione,
        t.messaingiacenza_recapito_stato,
        t.messaingiacenza_recapito_data,
        t.messaingiacenza_recapito_data_rendicontazione,
        t.certificazione_recapito_stato,
        t.certificazione_recapito_dettagli,
        t.certificazione_recapito_data,
        t.certificazione_recapito_data_rendicontazione,
        t.demat_23l_ar_stato,
        t.demat_23l_ar_data_rendicontazione,
        t.demat_plico_stato,
        t.demat_plico_data_rendicontazione,
        t.demat_arcad_stato,
        t.demat_arcad_data_rendicontazione,
        t.fine_recapito_stato,
        t.fine_recapito_data,
        t.fine_recapito_data_rendicontazione,
        t.accettazione_23l_recag012_data,
        t.accettazione_23l_recag012_data_rendicontazione,
        t.perfezionamento_data,
        t.perfezionamento_tipo,
        t.tms_cancelled,
        t.flag_wi7_consolidatore,
        t.flag_wi7_report_postalizzazioni_incomplete,
        t.pcretry_rank,
        t.attempt_rank,
        t.statusrequest,
        t.ultimo_evento_stato,
        t.stato_richiesta,
        t.flag_multi_destinatario,
        t.flag_ultima_postalizzazione,
        t.flag_prodotto_estero,
        t.flag_destinatario_deceduto,
        t.flag_esiti_mancanti,
        t.flag_oggetto_tavoli_poste,
        t.gestione_economica_ciclo_passivo,
        CASE
            WHEN controllo_causale = 1
              OR controllo_date_business = 1
              OR controllo_tripletta = 1
              OR controllo_tempistiche_compiuta_giacenza = 1
              OR controllo_inesito_casi_giacenza = 1
              OR controllo_documentType = 1
            THEN 1
            ELSE 0
        END AS flag_errore_rendicontazione,
        CONCAT_WS(
            ', ',
            CASE WHEN controllo_causale = 1 AND fine_recapito_stato IS NOT NULL AND flag_oggetto_tavoli_poste = 0 THEN 'errore rend. causale' END,
            CASE WHEN controllo_date_business = 1 AND fine_recapito_stato IS NOT NULL AND flag_oggetto_tavoli_poste = 0 THEN 'errore rend. date business' END,
            CASE WHEN controllo_tripletta = 1 AND fine_recapito_stato IS NOT NULL AND flag_oggetto_tavoli_poste = 0 THEN 'errore rend. tripletta' END,
            CASE WHEN controllo_tempistiche_compiuta_giacenza = 1 AND fine_recapito_stato IS NOT NULL AND flag_oggetto_tavoli_poste = 0 THEN 'errore rend. tempistiche compiuta giacenza' END,
            CASE WHEN controllo_inesito_casi_giacenza = 1 AND fine_recapito_stato IS NOT NULL AND flag_oggetto_tavoli_poste = 0 THEN 'errore rend. inesito casi giacenza' END,
            CASE WHEN assenza_inesito = 1 AND fine_recapito_stato IS NOT NULL AND flag_oggetto_tavoli_poste = 0 THEN 'assenza inesito' END,
            CASE WHEN assenza_messa_in_giacenza = 1 AND fine_recapito_stato IS NOT NULL AND flag_oggetto_tavoli_poste = 0 THEN 'assenza messa in giacenza' END,
            CASE WHEN assenza_pre_esito = 1 AND fine_recapito_stato IS NOT NULL AND flag_oggetto_tavoli_poste = 0 THEN 'assenza pre-esito' END,
            CASE WHEN assenza_dematerializzazione_23l_ar_plico = 1 AND fine_recapito_stato IS NOT NULL AND flag_oggetto_tavoli_poste = 0 THEN 'assenza demat 23l_ar / plico' END,
            CASE WHEN assenza_demat_arcad = 1 AND fine_recapito_stato IS NOT NULL AND flag_oggetto_tavoli_poste = 0 THEN 'assenza demat ARCAD' END,
            CASE WHEN assenza_RECAG012 = 1 AND fine_recapito_stato IS NOT NULL AND flag_oggetto_tavoli_poste = 0 THEN 'assenza RECAG012' END,
            CASE WHEN controllo_documentType = 1 AND fine_recapito_stato IS NOT NULL AND flag_oggetto_tavoli_poste = 0 THEN 'errore documentType' END,
            CASE
                WHEN controllo_causale = 0
                    AND controllo_date_business = 0
                    AND controllo_tripletta = 0
                    AND controllo_tempistiche_compiuta_giacenza = 0
                    AND controllo_inesito_casi_giacenza = 0
                    AND assenza_inesito = 0
                    AND assenza_RECAG012 = 0
                    AND assenza_pre_esito = 0
                    AND assenza_dematerializzazione_23l_ar_plico = 0
                    AND assenza_messa_in_giacenza = 0
                    AND assenza_demat_arcad = 0
                    AND flag_destinatario_deceduto = 0
                    AND controllo_documentType = 0
                    AND flag_feedback_attempt_0 = 0 
                THEN 'da perfezionare'
            END,
            CASE
                WHEN controllo_causale = 0
                    AND controllo_date_business = 0
                    AND controllo_tripletta = 0
                    AND controllo_tempistiche_compiuta_giacenza = 0
                    AND controllo_inesito_casi_giacenza = 0
                    AND assenza_inesito = 0
                    AND assenza_RECAG012 = 0
                    AND assenza_pre_esito = 0
                    AND assenza_dematerializzazione_23l_ar_plico = 0
                    AND assenza_messa_in_giacenza = 0
                    AND assenza_demat_arcad = 0
                    AND flag_destinatario_deceduto = 0
                    AND controllo_documentType = 0
                    AND flag_feedback_attempt_0 = 1 
                THEN 'bloccato al primo attempt'
            END
        ) AS dettaglio_rendicontazione,
        CASE
            WHEN tms_perfezionamento_notification IS NOT NULL THEN 'Oggetto perfezionato'
            WHEN tms_cancelled IS NOT NULL THEN 'Oggetto cancellato'
            WHEN flag_schedule_refinement = 1 THEN 'Oggetto in previsione di perfezionamento'
            WHEN statusrequest IN ('PN999', 'PN998')
              OR fine_recapito_stato IN ('PN999', 'PN998')
              OR tentativo_recapito_stato IN ('PN999', 'PN998')
              OR certificazione_recapito_stato IN ('PN999', 'PN998') THEN 'Oggetto bloccato con PN998 / PN999'
            WHEN statusrequest IN ('error', 'internalError', 'syntaxError','transformationError','semanticError','authenticationError','duplicatedRequest') THEN 'Oggetto scartato con eventi di errore'
            WHEN scarto_consolidatore_stato IS NOT NULL THEN 'Oggetto scartato dal Consolidatore'
            WHEN flag_oggetto_tavoli_poste = 1 THEN 'Oggetto rientrante in tavoli Duplicati / Non Rendicontabili'
            WHEN (flag_wi7_report_postalizzazioni_incomplete = 1 OR flag_wi7_consolidatore = 1)
              AND flag_oggetto_tavoli_poste = 0 THEN 'Oggetto fuori sla'
            WHEN flag_destinatario_deceduto = 1 THEN 'Oggetto restituito al mittente (esito destinatario deceduto)'
            WHEN certificazione_recapito_stato IN ('RECRS006','RECRS013','RECRN006','RECRN013','RECAG004','RECAG013')
              OR fine_recapito_stato IN ('RECRS006','RECRS013','RECRN006','RECRN013','RECAG004','RECAG013')
              THEN 'Oggetto esitato per Furto / Non Rendicontabile'
            WHEN (
                controllo_causale = 1
                OR controllo_date_business = 1
                OR controllo_tripletta = 1
                OR controllo_tempistiche_compiuta_giacenza = 1
                OR controllo_inesito_casi_giacenza = 1
                OR controllo_documentType = 1
                OR flag_esiti_mancanti = 1
            )
            AND fine_recapito_stato IS NOT NULL THEN 'Errore rendicontazione/assenza eventi intermedi'
            WHEN (
                controllo_causale = 0
                AND controllo_date_business = 0
                AND controllo_tripletta = 0
                AND controllo_tempistiche_compiuta_giacenza = 0
                AND controllo_inesito_casi_giacenza = 0
                AND controllo_documentType = 0
                AND assenza_inesito = 0
                AND assenza_RECAG012 = 0
                AND assenza_pre_esito = 0
                AND assenza_dematerializzazione_23l_ar_plico = 0
                AND assenza_messa_in_giacenza = 0
                AND assenza_demat_arcad = 0
                AND flag_destinatario_deceduto = 0
            )
            AND fine_recapito_stato IS NOT NULL THEN 'Oggetto con rendicontazione corretta in analisi Team Prodotto'
            ELSE 'Oggetto in corso di postalizzazione'
        END AS stato_avanzamento_oggetti,
        t.controllo_causale,
        t.controllo_date_business,
        t.controllo_tripletta,
        t.controllo_tempistiche_compiuta_giacenza,
        t.controllo_inesito_casi_giacenza,
        t.assenza_inesito,
        t.assenza_messa_in_giacenza,
        t.assenza_pre_esito,
        t.assenza_dematerializzazione_23l_ar_plico,
        t.assenza_demat_ARCAD,
        t.assenza_RECAG012
    FROM temp_assenza_eventi t
)
SELECT DISTINCT
    actual_status,
    iun,
    requestid,
    requesttimestamp,
    prodotto,
    geokey,
    CONCAT("'", geokey) AS cap,
    zona,
    recapitista_unif AS recapitista,
    lotto,
    codice_oggetto,
    codiceoggetto,
    grammatura,
    costo_recapitista,
    scarto_consolidatore_stato,
    scarto_consolidatore_data,
    affido_recapitista_con016_data,
    accettazione_recapitista_con018_data,
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
    demat_23l_ar_stato,
    demat_23l_ar_data_rendicontazione,
    demat_plico_stato,
    demat_plico_data_rendicontazione,
    demat_arcad_stato,
    demat_arcad_data_rendicontazione,
    accettazione_23l_recag012_data,
    fine_recapito_stato,
    fine_recapito_data,
    fine_recapito_data_rendicontazione,
    perfezionamento_data,
    perfezionamento_tipo,
    tms_cancelled,
    statusrequest,
    flag_multi_destinatario,
    flag_destinatario_deceduto,
    flag_errore_rendicontazione,
    CASE
        WHEN stato_avanzamento_oggetti IN (
            'Errore rendicontazione/assenza eventi intermedi',
            'Oggetto con rendicontazione corretta in analisi Team Prodotto'
        )
        THEN dettaglio_rendicontazione
        ELSE NULL
    END AS dettaglio_rendicontazione,
    stato_avanzamento_oggetti,
    gestione_economica_ciclo_passivo
FROM finale;