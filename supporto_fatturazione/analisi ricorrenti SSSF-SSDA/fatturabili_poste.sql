WITH gold_base_raw AS (
    SELECT
        p.senderpaid,
        p.iun,
        p.data_deposito,
        p.requestid,
        p.requesttimestamp,
        p.prodotto,
        p.geokey,
        p.zona,
        CASE
            WHEN p.codice_oggetto LIKE 'R14%' THEN 'Fulmine'
            WHEN p.codice_oggetto LIKE '777%' OR p.codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
            WHEN p.codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
            WHEN p.codice_oggetto LIKE '69%' OR p.codice_oggetto LIKE '381%' OR p.codice_oggetto LIKE 'RB1%' THEN 'Poste'
            ELSE p.recapitista_unificato
        END AS recapitista,
        COALESCE(
            p.accettazione_recapitista_con018_data,
            p.affido_recapitista_con016_data + INTERVAL 1 DAY
        ) AS data_riferimento_con,
        p.lotto,
        p.codice_oggetto,
        p.grammatura,
        p.costo_recapitista,
        p.scarto_consolidatore_stato,
        p.scarto_consolidatore_data,
        p.affido_recapitista_con016_data,
        p.accettazione_recapitista_con018_data,
        p.tentativo_recapito_stato,
        p.tentativo_recapito_data,
        p.tentativo_recapito_data_rendicontazione,
        p.messaingiacenza_recapito_stato,
        p.messaingiacenza_recapito_data,
        p.messaingiacenza_recapito_data_rendicontazione,
        p.certificazione_recapito_stato,
        p.certificazione_recapito_dettagli,
        p.certificazione_recapito_data,
        p.certificazione_recapito_data_rendicontazione,
        p.demat_23l_ar_stato,
        p.demat_23l_ar_data_rendicontazione,
        p.demat_plico_stato,
        p.demat_plico_data_rendicontazione,
        p.demat_arcad_stato,
        p.demat_arcad_data_rendicontazione,
        p.accettazione_23l_recag012_data,
        p.accettazione_23l_recag012_data_rendicontazione,
        p.fine_recapito_stato,
        p.fine_recapito_data,
        p.fine_recapito_data_rendicontazione,
        p.perfezionamento_data,
        p.perfezionamento_tipo,
        p.tms_cancelled,
        p.statusrequest,
        p.stato_richiesta,
        p.flag_wi7_consolidatore,
        p.flag_wi7_report_postalizzazioni_incomplete,
        p.attempt_rank,
        p.attempt_number,
        p.pcretry_rank,
        p.flag_prodotto_estero,
        p.recindex_number,
        b.riferimento_blocco,
        b.gestione_economica_ciclo_passivo
    FROM send.gold_postalizzazione_analytics p
    LEFT JOIN send_dev.oggetti_bloccati b
        ON p.requestid = b.requestid
    WHERE p.pcretry_rank = 1
      AND p.fine_recapito_stato IS NOT NULL
      AND p.fine_recapito_stato NOT IN (
          'RECRS006', 'RECRS013',
          'RECRN006', 'RECRN013',
          'RECAG004', 'RECAG013',
          'RECRSI005', 'RECRI005'
      )
      AND p.statusrequest NOT IN ('PN998')
),
gold_base AS (
    SELECT *
    FROM gold_base_raw
    WHERE recapitista = 'Poste'
      AND data_riferimento_con >= '2025-01-01'
      AND data_riferimento_con <  '2025-04-01'
),
iun_base AS (
    SELECT DISTINCT iun
    FROM gold_base
),
temp_analog_attempt AS (
    SELECT DISTINCT
        tl.iun,
        CAST(regexp_extract(tl.timelineelementid, 'ATTEMPT_([0-9]+)', 1) AS INT) AS attempt_number_timeline
    FROM send.silver_timeline tl
    INNER JOIN iun_base ib
        ON tl.iun = ib.iun
    WHERE tl.category = 'SEND_ANALOG_FEEDBACK'
),
requestid_base AS (
    SELECT DISTINCT
        requestid,
        CONCAT('pn-cons-000~', requestid) AS requestid_prefixed
    FROM gold_base
),
silver_base AS (
    SELECT
        rb.requestid AS requestid_clean,
        sp.eventslist
    FROM requestid_base rb
    INNER JOIN send.silver_postalizzazione sp
        ON sp.requestid = rb.requestid_prefixed
),
silver_eventi_status AS (
    SELECT
        sb.requestid_clean AS requestid,
        ev.item.paperprogrstatus.statuscode AS statuscode,
        ev.item.paperprogrstatus.statusdatetime AS statusdatetime_raw,
        ev.item.paperprogrstatus.clientrequesttimestamp AS clientrequesttimestamp_raw
    FROM silver_base sb,
         sb.eventslist ev
    WHERE ev.item.paperprogrstatus.statuscode IN (
        'RECRN001A','RECRN002A','RECRN002D','RECRN003A','RECRN004A','RECRN005A',
        'RECAG001A','RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A',
        'RECAG007A','RECAG008A',
        'RECRN001C','RECRN002C','RECRN002F','RECRN003C','RECRN004C','RECRN005C',
        'RECAG001C','RECAG002C','RECAG003C','RECAG003F','RECAG005C','RECAG006C',
        'RECAG007C','RECAG008C'
    )
),
silver_demat_raw AS (
    SELECT
        sb.requestid_clean AS requestid,
        ev.item.paperprogrstatus.clientrequesttimestamp AS clientrequesttimestamp_raw,
        att.item.documenttype AS documenttype
    FROM silver_base sb,
         sb.eventslist ev,
         ev.item.paperprogrstatus.attachments att
    WHERE att.item.documenttype IN ('23L', 'AR')
),
temp_demat AS (
    SELECT
        requestid,
        documenttype,
        ROW_NUMBER() OVER (
            PARTITION BY requestid
            ORDER BY clientrequesttimestamp_raw DESC
        ) AS rn_demat
    FROM silver_demat_raw
),
fine_recapito_silver AS (
    SELECT
        x.requestid,
        x.statuscode AS fine_recapito_stato_silver,
        x.statusdatetime AS fine_recapito_data_silver,
        x.clientrequesttimestamp AS fine_recapito_data_rendicontazione_silver
    FROM (
        SELECT
            se.requestid,
            se.statuscode,
            CASE
                WHEN LENGTH(se.statusdatetime_raw) = 17
                    THEN CONCAT(SUBSTR(se.statusdatetime_raw, 1, 16), ':00Z')
                ELSE se.statusdatetime_raw
            END AS statusdatetime,
            CASE
                WHEN LENGTH(se.clientrequesttimestamp_raw) = 17
                    THEN CONCAT(SUBSTR(se.clientrequesttimestamp_raw, 1, 16), ':00.000Z')
                ELSE se.clientrequesttimestamp_raw
            END AS clientrequesttimestamp,
            ROW_NUMBER() OVER (
                PARTITION BY se.requestid
                ORDER BY se.clientrequesttimestamp_raw DESC
            ) AS rn
        FROM silver_eventi_status se
        WHERE se.statuscode IN (
            'RECRN001C','RECRN002C','RECRN002F','RECRN003C','RECRN004C','RECRN005C',
            'RECAG001C','RECAG002C','RECAG003C','RECAG003F','RECAG005C','RECAG006C',
            'RECAG007C','RECAG008C'
        )
    ) x
    WHERE x.rn = 1
),
certificazione_recapito_silver AS (
    SELECT
        x.requestid,
        x.statuscode AS certificazione_recapito_stato_silver,
        x.statusdatetime AS certificazione_recapito_data_silver,
        x.clientrequesttimestamp AS certificazione_recapito_data_rendicontazione_silver
    FROM (
        SELECT
            se.requestid,
            se.statuscode,
            CASE
                WHEN LENGTH(se.statusdatetime_raw) = 17
                    THEN CONCAT(SUBSTR(se.statusdatetime_raw, 1, 16), ':00Z')
                ELSE se.statusdatetime_raw
            END AS statusdatetime,
            CASE
                WHEN LENGTH(se.clientrequesttimestamp_raw) = 17
                    THEN CONCAT(SUBSTR(se.clientrequesttimestamp_raw, 1, 16), ':00.000Z')
                ELSE se.clientrequesttimestamp_raw
            END AS clientrequesttimestamp,
            ROW_NUMBER() OVER (
                PARTITION BY se.requestid
                ORDER BY se.clientrequesttimestamp_raw DESC
            ) AS rn
        FROM silver_eventi_status se
        WHERE se.statuscode IN (
            'RECRN001A','RECRN002A','RECRN002D','RECRN003A','RECRN004A','RECRN005A',
            'RECAG001A','RECAG002A','RECAG003A','RECAG003D','RECAG005A','RECAG006A',
            'RECAG007A','RECAG008A'
        )
    ) x
    WHERE x.rn = 1
),
temp_ultimi_eventi_silver_postalizzazione AS (
    SELECT
        COALESCE(fr.requestid, cr.requestid) AS requestid,
        fr.fine_recapito_stato_silver,
        fr.fine_recapito_data_silver,
        fr.fine_recapito_data_rendicontazione_silver,
        cr.certificazione_recapito_stato_silver,
        cr.certificazione_recapito_data_silver,
        cr.certificazione_recapito_data_rendicontazione_silver
    FROM fine_recapito_silver fr
    FULL OUTER JOIN certificazione_recapito_silver cr
        ON fr.requestid = cr.requestid
),
notif_base AS (
    SELECT
        n.iun,
        n.actual_status,
        n.type_notif,
        n.tms_viewed,
        n.tms_effective_date
    FROM send.gold_notification_analytics n
    INNER JOIN iun_base ib
        ON n.iun = ib.iun
),
sn_base AS (
    SELECT
        sn.iun,
        MAX(sn.senderdenomination) AS senderdenomination
    FROM send.silver_notification sn
    INNER JOIN iun_base ib
        ON sn.iun = ib.iun
    GROUP BY sn.iun
),
timeline_refinement AS (
    SELECT
        tl.iun,
        1 AS flag_schedule_refinement
    FROM send.silver_timeline tl
    INNER JOIN iun_base ib
        ON tl.iun = ib.iun
    WHERE tl.category = 'SCHEDULE_REFINEMENT'
    GROUP BY tl.iun
),
temp_controlli AS (
    SELECT
        g.senderpaid,
        sn.senderdenomination,
        g.iun,
        g.data_deposito,
        n.actual_status,
        g.requestid,
        g.requesttimestamp,
        g.prodotto,
        g.geokey,
        CONCAT("'", g.geokey) AS cap,
        g.zona,
        g.recapitista,
        g.lotto,
        g.codice_oggetto,
        CONCAT("'", g.codice_oggetto) AS codiceOggetto,
        g.grammatura,
        g.costo_recapitista,
        g.scarto_consolidatore_stato,
        g.scarto_consolidatore_data,
        g.affido_recapitista_con016_data,
        g.accettazione_recapitista_con018_data,
        COALESCE(accettazione_recapitista_con018_data, g.affido_recapitista_con016_data) AS affido_accettazione_rec_data,
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
        g.accettazione_23l_recag012_data,
        g.fine_recapito_stato,
        g.fine_recapito_data,
        g.fine_recapito_data_rendicontazione,
        g.perfezionamento_data,
        g.perfezionamento_tipo,
        g.tms_cancelled,
        g.statusrequest,
        g.stato_richiesta,
        g.recindex_number,
        g.flag_wi7_consolidatore,
        g.flag_wi7_report_postalizzazioni_incomplete,
        g.attempt_rank,
        g.pcretry_rank,
        g.flag_prodotto_estero,
        g.riferimento_blocco,
        g.gestione_economica_ciclo_passivo,
        LEAST(
            COALESCE(n.tms_viewed, n.tms_effective_date),
            COALESCE(n.tms_effective_date, n.tms_viewed)
        ) AS tms_perfezionamento_notification,
        COALESCE(tl.flag_schedule_refinement, 0) AS flag_schedule_refinement,
        CASE WHEN n.type_notif = 'MULTI' THEN 1 ELSE 0 END AS flag_multi_destinatario,
        CASE WHEN g.attempt_rank = 1 AND g.pcretry_rank = 1 THEN 1 ELSE 0 END AS flag_ultima_postalizzazione,
        CASE WHEN g.certificazione_recapito_dettagli = 'M02' THEN 1 ELSE 0 END AS flag_destinatario_deceduto,
        CASE WHEN g.flag_wi7_report_postalizzazioni_incomplete = 1 THEN 1 ELSE 0 END AS flag_fuori_sla,
        CASE WHEN w.requestid IS NOT NULL THEN 1 ELSE 0 END AS flag_oggetto_tavoli_poste,
        d.documenttype,
        sps.certificazione_recapito_stato_silver,
        sps.certificazione_recapito_data_silver,
        sps.certificazione_recapito_data_rendicontazione_silver,
        sps.fine_recapito_stato_silver,
        sps.fine_recapito_data_silver,
        sps.fine_recapito_data_rendicontazione_silver,
        CASE
            WHEN g.certificazione_recapito_stato NOT IN ('RECRS002A','RECRN002A','RECAG003A','RECRS002D','RECRN002D','RECAG003D')
                 AND g.certificazione_recapito_dettagli IN ('M01','M03','M04','M02','M05','M06','M07','M08','M09') THEN 1
            WHEN g.certificazione_recapito_stato IN ('RECRS002A','RECRN002A','RECAG003A')
                 AND (g.certificazione_recapito_dettagli NOT IN ('M02','M05','M06','M07','M08','M09') OR g.certificazione_recapito_dettagli IS NULL) THEN 1
            WHEN g.certificazione_recapito_stato IN ('RECRS002D','RECRN002D','RECAG003D')
                 AND (g.certificazione_recapito_dettagli NOT IN ('M01','M03','M04') OR g.certificazione_recapito_dettagli IS NULL) THEN 1
            ELSE 0
        END AS controllo_causale,
        CASE
            WHEN g.certificazione_recapito_stato = 'RECRN003A'
                 AND (g.tentativo_recapito_stato = 'RECRN010' OR g.tentativo_recapito_stato IS NULL) THEN 0
            WHEN g.certificazione_recapito_stato = 'RECRS003A'
                 AND (g.tentativo_recapito_stato = 'RECRS010' OR g.tentativo_recapito_stato IS NULL) THEN 0
            WHEN g.certificazione_recapito_stato IN ('RECAG005A','RECAG006A','RECAG007A','RECAG008A')
                 AND (g.tentativo_recapito_stato = 'RECAG010' OR g.tentativo_recapito_stato IS NULL) THEN 0
            WHEN g.certificazione_recapito_stato NOT IN (
                'RECRN003A','RECRN004A','RECRN005A',
                'RECAG005A','RECRS003A','RECRS004A','RECRS005A',
                'RECAG006A','RECAG007A','RECAG008A'
            )
                 AND g.tentativo_recapito_stato = g.certificazione_recapito_stato THEN 0
            ELSE 1
        END AS controllo_inesito_casi_giacenza,
        CASE
            WHEN g.certificazione_recapito_stato = 'RECRN001A'
                 AND (g.demat_23l_ar_stato = 'RECRN001B' OR g.demat_plico_stato = 'RECRN001B' OR g.demat_arcad_stato = 'RECRN001B')
                 AND sps.fine_recapito_stato_silver = 'RECRN001C' THEN 0
            WHEN g.certificazione_recapito_stato = 'RECRN002A'
                 AND (g.demat_23l_ar_stato = 'RECRN002B' OR g.demat_plico_stato = 'RECRN002B' OR g.demat_arcad_stato = 'RECRN002B')
                 AND sps.fine_recapito_stato_silver = 'RECRN002C' THEN 0
            WHEN g.certificazione_recapito_stato = 'RECRN002D'
                 AND (g.demat_23l_ar_stato = 'RECRN002E' OR g.demat_plico_stato = 'RECRN002E' OR g.demat_arcad_stato = 'RECRN002E')
                 AND sps.fine_recapito_stato_silver = 'RECRN002F' THEN 0
            WHEN g.certificazione_recapito_stato = 'RECRN003A'
                 AND (g.demat_23l_ar_stato = 'RECRN003B' OR g.demat_plico_stato = 'RECRN003B' OR g.demat_arcad_stato = 'RECRN003B')
                 AND sps.fine_recapito_stato_silver = 'RECRN003C' THEN 0
            WHEN g.certificazione_recapito_stato = 'RECRN004A'
                 AND (g.demat_23l_ar_stato = 'RECRN004B' OR g.demat_plico_stato = 'RECRN004B' OR g.demat_arcad_stato = 'RECRN004B')
                 AND sps.fine_recapito_stato_silver = 'RECRN004C' THEN 0
            WHEN g.certificazione_recapito_stato = 'RECRN005A'
                 AND (g.demat_23l_ar_stato = 'RECRN005B' OR g.demat_plico_stato = 'RECRN005B' OR g.demat_arcad_stato = 'RECRN005B')
                 AND sps.fine_recapito_stato_silver = 'RECRN005C' THEN 0
            WHEN g.certificazione_recapito_stato = 'RECAG001A'
                 AND (g.demat_23l_ar_stato = 'RECAG001B' OR g.demat_plico_stato = 'RECAG001B' OR g.demat_arcad_stato = 'RECAG001B')
                 AND sps.fine_recapito_stato_silver = 'RECAG001C' THEN 0
            WHEN g.certificazione_recapito_stato = 'RECAG002A'
                 AND (g.demat_23l_ar_stato = 'RECAG002B' OR g.demat_plico_stato = 'RECAG002B' OR g.demat_arcad_stato = 'RECAG002B')
                 AND sps.fine_recapito_stato_silver = 'RECAG002C' THEN 0
            WHEN g.certificazione_recapito_stato = 'RECAG003A'
                 AND (g.demat_23l_ar_stato = 'RECAG003B' OR g.demat_plico_stato = 'RECAG003B' OR g.demat_arcad_stato = 'RECAG003B')
                 AND sps.fine_recapito_stato_silver = 'RECAG003C' THEN 0
            WHEN g.certificazione_recapito_stato = 'RECAG003D'
                 AND (g.demat_23l_ar_stato = 'RECAG003E' OR g.demat_plico_stato = 'RECAG003E' OR g.demat_arcad_stato = 'RECAG003E')
                 AND sps.fine_recapito_stato_silver = 'RECAG003F' THEN 0
            WHEN g.certificazione_recapito_stato = 'RECAG005A'
                 AND (g.demat_23l_ar_stato IN ('RECAG011B','RECAG005B') OR g.demat_plico_stato IN ('RECAG011B','RECAG005B') OR g.demat_arcad_stato IN ('RECAG011B','RECAG005B'))
                 AND sps.fine_recapito_stato_silver = 'RECAG005C' THEN 0
            WHEN g.certificazione_recapito_stato = 'RECAG006A'
                 AND (g.demat_23l_ar_stato IN ('RECAG011B','RECAG006B') OR g.demat_plico_stato IN ('RECAG011B','RECAG006B') OR g.demat_arcad_stato IN ('RECAG011B','RECAG006B'))
                 AND sps.fine_recapito_stato_silver = 'RECAG006C' THEN 0
            WHEN g.certificazione_recapito_stato = 'RECAG007A'
                 AND (g.demat_23l_ar_stato IN ('RECAG011B','RECAG007B') OR g.demat_plico_stato IN ('RECAG011B','RECAG007B') OR g.demat_arcad_stato IN ('RECAG011B','RECAG007B'))
                 AND sps.fine_recapito_stato_silver = 'RECAG007C' THEN 0
            WHEN g.certificazione_recapito_stato = 'RECAG008A'
                 AND (g.demat_23l_ar_stato IN ('RECAG011B','RECAG008B') OR g.demat_plico_stato IN ('RECAG011B','RECAG008B') OR g.demat_arcad_stato IN ('RECAG011B','RECAG008B'))
                 AND sps.fine_recapito_stato_silver = 'RECAG008C' THEN 0
            ELSE 1
        END AS controllo_tripletta,
        CASE
            WHEN sps.certificazione_recapito_data_silver IS NOT NULL
                 AND sps.fine_recapito_data_silver IS NOT NULL
                 AND CAST(sps.certificazione_recapito_data_silver AS TIMESTAMP) = CAST(sps.fine_recapito_data_silver AS TIMESTAMP) THEN 0
            ELSE 1
        END AS controllo_date_business,
        CASE
            WHEN g.certificazione_recapito_stato = 'RECRN005A'
                 AND DATEDIFF(CAST(g.certificazione_recapito_data AS DATE), CAST(g.tentativo_recapito_data AS DATE)) < 30 THEN 1
            ELSE 0
        END AS controllo_tempistiche_compiuta_giacenza,
        CASE
            WHEN g.prodotto = '890' AND d.documenttype = '23L' THEN 0
            WHEN g.prodotto = 'AR'  AND d.documenttype = 'AR' THEN 0
            WHEN d.documenttype IS NULL THEN 0
            ELSE 1
        END AS controllo_documenttype,
        CASE
          WHEN a.attempt_number_timeline = 0
           AND CAST(g.attempt_number AS INT) = 0
          THEN 1
          ELSE 0
      END AS flag_feedback_attempt_0
    FROM gold_base g
    LEFT JOIN temp_demat d
        ON d.requestid = g.requestid
       AND d.rn_demat = 1
    LEFT JOIN send_dev.wi7_poste_da_escludere w
        ON g.requestid = w.requestid
    LEFT JOIN notif_base n
        ON g.iun = n.iun
    LEFT JOIN sn_base sn
        ON g.iun = sn.iun
    LEFT JOIN timeline_refinement tl
        ON g.iun = tl.iun
    LEFT JOIN temp_ultimi_eventi_silver_postalizzazione sps
        ON g.requestid = sps.requestid
    LEFT JOIN temp_analog_attempt a
        ON CAST(g.attempt_number AS INT) = CAST(a.attempt_number_timeline AS INT) AND g.iun = a.iun
),
temp_postalizzazione AS (
    SELECT
        s.*,
        IF(
            fine_recapito_stato IN ('RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C','RECAG005C','RECAG006C','RECAG007C','RECAG008C')
            AND tentativo_recapito_stato IS NULL,
            1, 0
        ) AS assenza_inesito,
        IF(
            fine_recapito_stato IN ('RECRS003C','RECRS004C','RECRS005C','RECRN003C','RECRN004C','RECRN005C','RECAG005C','RECAG006C','RECAG007C','RECAG008C')
            AND messaingiacenza_recapito_stato IS NULL,
            1, 0
        ) AS assenza_messa_in_giacenza,
        IF(certificazione_recapito_stato IS NULL, 1, 0) AS assenza_pre_esito,
        IF(
            (
                fine_recapito_stato IN ('RECAG008C')
                AND (demat_23l_ar_stato IS NULL OR demat_plico_stato IS NULL)
            )
            OR (
                demat_23l_ar_stato IS NULL AND demat_plico_stato IS NULL
            ),
            1, 0
        ) AS assenza_dematerializzazione_23l_ar_plico,
        IF(
            prodotto = '890'
            AND fine_recapito_stato IN ('RECAG005C','RECAG006C','RECAG007C','RECAG008C')
            AND demat_arcad_data_rendicontazione IS NULL,
            1, 0
        ) AS assenza_demat_arcad,
        IF(
            prodotto = '890'
            AND fine_recapito_stato IN ('RECAG005C','RECAG006C','RECAG007C','RECAG008C')
            AND accettazione_23l_recag012_data IS NULL,
            1, 0
        ) AS assenza_recag012
    FROM temp_controlli s
),
temp_assenza_eventi AS (
    SELECT
        * ,
        IF(
            assenza_inesito = 1
            OR assenza_messa_in_giacenza = 1
            OR assenza_pre_esito = 1
            OR assenza_dematerializzazione_23l_ar_plico = 1
            OR assenza_demat_arcad = 1
            OR assenza_recag012 = 1,
            1, 0
        ) AS flag_esiti_mancanti
    FROM temp_postalizzazione
),
finale AS (
    SELECT
        actual_status,
        iun,
        requestid,
        requesttimestamp,
        prodotto,
        geokey,
        cap,
        zona,
        recapitista,
        lotto,
        codice_oggetto,
        codiceOggetto,
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
        CASE
            WHEN controllo_causale = 1
              OR controllo_date_business = 1
              OR controllo_tripletta = 1
              OR controllo_tempistiche_compiuta_giacenza = 1
              OR controllo_inesito_casi_giacenza = 1
              OR controllo_documenttype = 1
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
            CASE WHEN assenza_recag012 = 1 AND fine_recapito_stato IS NOT NULL AND flag_oggetto_tavoli_poste = 0 THEN 'assenza RECAG012' END,
            CASE WHEN controllo_documenttype = 1 AND fine_recapito_stato IS NOT NULL AND flag_oggetto_tavoli_poste = 0 THEN 'errore documentType' END,
            CASE
                WHEN controllo_causale = 0
                    AND controllo_date_business = 0
                    AND controllo_tripletta = 0
                    AND controllo_tempistiche_compiuta_giacenza = 0
                    AND controllo_inesito_casi_giacenza = 0
                    AND assenza_inesito = 0
                    AND assenza_recag012 = 0
                    AND assenza_pre_esito = 0
                    AND assenza_dematerializzazione_23l_ar_plico = 0
                    AND assenza_messa_in_giacenza = 0
                    AND assenza_demat_arcad = 0
                    AND flag_destinatario_deceduto = 0
                    AND controllo_documenttype = 0
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
                    AND assenza_recag012 = 0
                    AND assenza_pre_esito = 0
                    AND assenza_dematerializzazione_23l_ar_plico = 0
                    AND assenza_messa_in_giacenza = 0
                    AND assenza_demat_arcad = 0
                    AND flag_destinatario_deceduto = 0
                    AND controllo_documenttype = 0
                    AND flag_feedback_attempt_0 = 1 
                THEN 'bloccato al primo attempt'
            END
        ) AS dettaglio_rendicontazione,
        CASE
            WHEN tms_perfezionamento_notification IS NOT NULL THEN 'Oggetto perfezionato'
            WHEN tms_cancelled IS NOT NULL THEN 'Oggetto cancellato'
            WHEN flag_schedule_refinement = 1 THEN 'Oggetto in previsione di perfezionamento'
            WHEN statusrequest IN ('PN999','PN998')
              OR fine_recapito_stato IN ('PN999','PN998')
              OR tentativo_recapito_stato IN ('PN999','PN998')
              OR certificazione_recapito_stato IN ('PN999','PN998') THEN 'Oggetto bloccato con PN998 / PN999'
            WHEN statusrequest IN ('error','internalError','syntaxError','transformationError','semanticError','authenticationError','duplicatedRequest') THEN 'Oggetto scartato con eventi di errore'
            WHEN scarto_consolidatore_stato IS NOT NULL THEN 'Oggetto scartato dal Consolidatore'
            WHEN flag_oggetto_tavoli_poste = 1 THEN 'Oggetto rientrante in tavoli Duplicati / Non Rendicontabili'
            WHEN (flag_wi7_report_postalizzazioni_incomplete = 1 OR flag_wi7_consolidatore = 1) AND flag_oggetto_tavoli_poste = 0 THEN 'Oggetto fuori sla'
            WHEN flag_destinatario_deceduto = 1 THEN 'Oggetto restituito al mittente (esito destinatario deceduto)'
            WHEN certificazione_recapito_stato IN ('RECRS006','RECRS013','RECRN006','RECRN013','RECAG004','RECAG013')
              OR fine_recapito_stato IN ('RECRS006','RECRS013','RECRN006','RECRN013','RECAG004','RECAG013') THEN 'Oggetto esitato per Furto / Non Rendicontabile'
            WHEN (
                controllo_causale = 1
                OR controllo_date_business = 1
                OR controllo_tripletta = 1
                OR controllo_tempistiche_compiuta_giacenza = 1
                OR controllo_inesito_casi_giacenza = 1
                OR controllo_documenttype = 1
                OR flag_esiti_mancanti = 1
            ) AND fine_recapito_stato IS NOT NULL THEN 'Errore rendicontazione/assenza eventi intermedi'
            WHEN (
                controllo_causale = 0
                AND controllo_date_business = 0
                AND controllo_tripletta = 0
                AND controllo_tempistiche_compiuta_giacenza = 0
                AND controllo_inesito_casi_giacenza = 0
                AND controllo_documenttype = 0
                AND assenza_inesito = 0
                AND assenza_recag012 = 0
                AND assenza_pre_esito = 0
                AND assenza_dematerializzazione_23l_ar_plico = 0
                AND assenza_messa_in_giacenza = 0
                AND assenza_demat_arcad = 0
                AND flag_destinatario_deceduto = 0
            ) AND fine_recapito_stato IS NOT NULL THEN 'Oggetto con rendicontazione corretta in analisi Team Prodotto'
            ELSE 'Oggetto in corso di postalizzazione'
        END AS stato_avanzamento_oggetti,
        --riferimento_blocco,
        gestione_economica_ciclo_passivo
    FROM temp_assenza_eventi
)
SELECT DISTINCT
    actual_status,
    iun,
    requestid,
    requesttimestamp,
    prodotto,
    geokey,
    cap,
    zona,
    recapitista,
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