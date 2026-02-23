
-- Nome sheet: 2. totale notifiche
-- Descrizione: totale notifiche digitali nel 2024 e 2025
SELECT 
    YEAR(sentat) AS anno,
    type_notif,
    COUNT(*) AS numero_notifiche
FROM send.gold_notification_analytics
WHERE senderpaid = '53b40136-65f2-424b-acfb-7fae17e35c60' -- INPS    
      --AND type_notif IS NOT NULL   -- da capire se vanno escluse quelle con type notif NULL oppure anche le MULTI
      --AND tms_cancelled IS NULL    -- da capire se vanno escluse quelle cancellate
      AND type_notif IS NOT NULL
      --AND YEAR(sentat) IN (2024, 2025)
      AND sentat < '2025-09-01'
GROUP BY YEAR(sentat), 
         type_notif
ORDER BY anno ASC, type_notif DESC



-- Nome sheet: 3. Tempistiche di perfezionamento
-- Descrizione: ...
WITH base AS (
    SELECT  
        iun,
        cast(sentat AS timestamp) AS sentat,
        type_notif,
        -- Calcolo tms_perfezionamento
        CASE 
            WHEN tms_viewed IS NULL THEN cast(tms_effective_date AS timestamp)
            WHEN tms_effective_date IS NULL THEN cast(tms_viewed AS timestamp)
            WHEN tms_viewed < tms_effective_date THEN cast(tms_viewed AS timestamp)
            ELSE cast(tms_effective_date AS timestamp)
        END AS tms_perfezionamento
    FROM send.gold_notification_analytics
    WHERE senderpaid = '53b40136-65f2-424b-acfb-7fae17e35c60' -- INPS    
      AND (tms_viewed IS NOT NULL OR tms_effective_date IS NOT NULL)
      AND sentat < '2025-09-01'
),
diff AS (
    SELECT
        iun,
        type_notif,
        sentat,
        tms_perfezionamento,
        -- Differenza in giorni (equivalente del unix_timestamp / 86400)
        ROUND(
            (unix_timestamp(tms_perfezionamento) - unix_timestamp(sentat)) 
            / (3600 * 24),
        2) AS diff_sentat_perfezionamento,
        YEAR(sentat) AS anno_deposito
    FROM base
)
SELECT
    anno_deposito,
    type_notif,
    ROUND(SUM(diff_sentat_perfezionamento), 2) AS somma_tempistiche,
    COUNT(tms_perfezionamento) AS notifiche_perfezionate,
    ROUND(
        SUM(diff_sentat_perfezionamento) / COUNT(tms_perfezionamento),
        2
    ) AS tempo_medio_perfezionamento
FROM diff
GROUP BY
    anno_deposito,
    type_notif
ORDER BY
    anno_deposito,
    type_notif;



-- Nome sheet: 4. % conversione digitale
-- Descrizione: 
WITH
    courtesy AS (
        SELECT DISTINCT
            iun,
            notificationsentat
        FROM
            send.silver_timeline
        WHERE
            category = 'SEND_COURTESY_MESSAGE'
            AND paid = '53b40136-65f2-424b-acfb-7fae17e35c60' -- INPS    
            AND notificationsentat < '2025-09-01' --- FIXED
    ),
    perfezionate AS (
        SELECT DISTINCT
            iun,
            notificationsentat
        FROM
            send.silver_timeline
        WHERE
            category IN ('NOTIFICATION_VIEWED', 'REFINEMENT')
            AND paid = '53b40136-65f2-424b-acfb-7fae17e35c60' -- INPS    
            AND notificationsentat < '2025-09-01' --- FIXED
    ),
    prepared AS (
        SELECT DISTINCT
            iun,
            notificationsentat
        FROM
            send.silver_timeline
        WHERE
            category = 'PREPARE_ANALOG_DOMICILE'
            AND paid = '53b40136-65f2-424b-acfb-7fae17e35c60' -- INPS    
            AND notificationsentat < '2025-09-01' --- FIXED
    ),
    digital AS (
        SELECT DISTINCT
            iun,
            notificationsentat
        FROM
            send.silver_timeline
        WHERE
            category = 'SEND_DIGITAL_FEEDBACK'
            AND paid = '53b40136-65f2-424b-acfb-7fae17e35c60' -- INPS    
            AND notificationsentat < '2025-09-01' --- FIXED
    ),
    joined AS (
        SELECT
            v.iun AS iun,
            MONTH (v.notificationsentat) AS mese_sentat,
            YEAR (v.notificationsentat) AS anno_sentat,
            CASE
                WHEN d.iun IS NOT NULL THEN 1
                ELSE 0
            END AS has_digital_feedback,
            CASE
                WHEN c.iun IS NOT NULL THEN 1
                ELSE 0
            END AS has_courtesy,
            CASE
                WHEN v.iun IS NOT NULL THEN 1
                ELSE 0
            END AS has_perfezionamento,
            CASE
                WHEN p.iun IS NOT NULL THEN 1
                ELSE 0
            END AS has_prepared
        FROM
            perfezionate v
            LEFT JOIN digital d ON d.iun = v.iun
            LEFT JOIN courtesy c ON v.iun = c.iun
            LEFT JOIN prepared p ON v.iun = p.iun
        WHERE v.notificationsentat < '2025-09-01' --- FIXED
    ),
    clusterized AS (
        SELECT
            iun,
            mese_sentat,
            anno_sentat,
            CASE
                WHEN has_courtesy = 1
                AND has_digital_feedback = 0
                AND has_perfezionamento = 1
                AND has_prepared = 0 THEN 'Analogiche evitate grazie ai messaggi di cortesia'
                ELSE 'Altro'
            END AS cluster
        FROM
            joined
    ),
    D_perfezionamento AS (
        SELECT
            MONTH (notificationsentat) AS mese_sentat,
            YEAR (notificationsentat) AS anno_sentat,
            COUNT(DISTINCT iun) AS totale_perfezionate
        FROM
            send.silver_timeline
        WHERE
            category IN (
                'SEND_DIGITAL_FEEDBACK',
                'SEND_ANALOG_FEEDBACK',
                'NOTIFICATION_VIEWED',
                'REFINEMENT'
            )
            AND paid = '53b40136-65f2-424b-acfb-7fae17e35c60' -- INPS    
            AND notificationsentat < '2025-09-01' --- FIXED
        GROUP BY
            MONTH (notificationsentat),
            YEAR (notificationsentat)
    ),
    B_final AS (
        SELECT
            mese_sentat,
            anno_sentat,
            cluster,
            COUNT(DISTINCT iun) AS totale_cluster
        FROM
            clusterized
        WHERE
            cluster <> 'Altro'
        GROUP BY
            cluster,
            mese_sentat,
            anno_sentat
        ORDER BY
            mese_sentat,
            anno_sentat DESC
    ),
    C_digitali_perfezionate AS (
        SELECT
            MONTH (notificationsentat) AS mese_sentat,
            YEAR (notificationsentat) AS anno_sentat,
            COUNT(DISTINCT iun) AS digitali_perfezionate
        FROM
            send.silver_timeline
        WHERE
            category = 'SEND_DIGITAL_FEEDBACK'
            AND paid = '53b40136-65f2-424b-acfb-7fae17e35c60' -- INPS    
            AND notificationsentat < '2025-09-01' --- FIXED
        GROUP BY
            MONTH (notificationsentat),
            YEAR (notificationsentat)
    ),
    F_notifiche_affidate AS (
        SELECT
            MONTH (sentat) AS mese_sentat,
            YEAR (sentat) AS anno_sentat,
            COUNT(DISTINCT iun) AS conteggio_affidate
        FROM
            send.gold_notification_analytics
        WHERE
            type_notif IS NOT NULL
            AND senderpaid = '53b40136-65f2-424b-acfb-7fae17e35c60'-- INPS    
            AND sentat < '2025-09-01' --- FIXED
        GROUP BY
            MONTH (sentat),
            YEAR (sentat)
    )
SELECT
    CONCAT (
        CAST(f.mese_sentat AS STRING),
        '_',
        CAST(f.anno_sentat AS STRING)
    ) AS `anno_mese deposito`,
    COALESCE(
        (
            CASE
                WHEN cluster = 'Analogiche evitate grazie ai messaggi di cortesia' THEN totale_cluster
            END
        ),
        0
    ) AS `Analogiche evitate grazie ai messaggi di cortesia`,
    COALESCE(digitali_perfezionate, 0) AS `Digitali perfezionate`,
    COALESCE(totale_perfezionate, 0) AS `Totale perfezionate (ricevuto feedback)`,
    COALESCE(conteggio_affidate, 0) - COALESCE(digitali_perfezionate, 0) AS `Notifiche analogiche`,
    COALESCE(conteggio_affidate, 0) AS `Notifiche affidate`,
    ROUND(
        (
            COALESCE(
                (
                    CASE
                        WHEN cluster = 'Analogiche evitate grazie ai messaggi di cortesia' THEN totale_cluster
                    END
                ),
                0
            ) + COALESCE(digitali_perfezionate, 0)
        ) / COALESCE(totale_perfezionate, 0),
        4
    ) AS `% Conversioni (mancate analogiche+pec) sul perfezionato`,
    ROUND(
        COALESCE(
            (
                CASE
                    WHEN cluster = 'Analogiche evitate grazie ai messaggi di cortesia' THEN totale_cluster
                END
            ),
            0
        ) / (
            COALESCE(conteggio_affidate, 0) - COALESCE(digitali_perfezionate, 0)
        ),
        4
    ) AS `% Conversione analogico`,
    ROUND(
        (
            COALESCE(
                (
                    CASE
                        WHEN cluster = 'Analogiche evitate grazie ai messaggi di cortesia' THEN totale_cluster
                    END
                ),
                0
            ) + COALESCE(digitali_perfezionate, 0)
        ) / COALESCE(conteggio_affidate, 0),
        4
    ) AS `% Conversioni (mancate analogiche+pec) su affidato`, -- chiedere conferma per il nome della colonna
    ROUND(
        COALESCE(
            (
                CASE
                    WHEN cluster = 'Analogiche evitate grazie ai messaggi di cortesia' THEN totale_cluster
                END
            ),
            0
        ) / COALESCE(conteggio_affidate, 0),
        4
    ) AS `% Conversione (mancate analogiche) su affidato`
FROM
    F_notifiche_affidate f
    LEFT JOIN D_perfezionamento d ON (
        f.mese_sentat = d.mese_sentat
        AND f.anno_sentat = d.anno_sentat
    )
    LEFT JOIN C_digitali_perfezionate c ON (
        f.mese_sentat = c.mese_sentat
        AND f.anno_sentat = c.anno_sentat
    )
    LEFT JOIN B_final b ON (
        b.mese_sentat = f.mese_sentat
        AND b.anno_sentat = f.anno_sentat
    )
    --WHERE f.anno_sentat <> 2023
ORDER BY
    f.anno_sentat ASC,
    f.mese_sentat ASC
    
    
-- Nome sheet: 5. Pagate ai cittadini
-- Descrizione: 
WITH postalizzazione_dedup AS (
	    SELECT DISTINCT iun, prezzo_ente
	    FROM send.gold_postalizzazione_analytics
	    WHERE pcretry_rank = 1
	    -- pcretry_rank = 1 garantisce 1 sola riga per iun  
    ),base AS (
      SELECT 
            b.iun,
            b.sentat,
            b.type_notif,
            b.tms_date_payment,
            b.tms_viewed,
            b.tms_effective_date,
            CASE 
                WHEN b.tms_viewed IS NULL THEN b.tms_effective_date
                WHEN b.tms_effective_date IS NULL THEN b.tms_viewed
                WHEN b.tms_viewed < b.tms_effective_date THEN b.tms_viewed
                ELSE b.tms_effective_date
            END AS tms_perfezionamento,
            prezzo_ente
        FROM send.gold_notification_analytics b
        LEFT JOIN postalizzazione_dedup c
           ON b.iun = c.iun
        WHERE b.senderpaid = '53b40136-65f2-424b-acfb-7fae17e35c60'-- INPS    
          AND b.type_notif IS NOT NULL
          --AND c.pcretry_rank = 1
          AND b.sentat < '2025-09-01' -- FIX
  )      
   SELECT 
        YEAR(sentat) AS anno,
        type_notif AS `tipo notifica`,
        --COUNT(*) AS numero_notifiche,
        COUNT(CASE WHEN tms_date_payment IS NOT NULL THEN 1 END) AS `Totale notifiche pagate`, -- COLONNA C
        COUNT(CASE WHEN tms_perfezionamento IS NOT NULL THEN 1 END) AS `Totale notifiche perfezionate`, -- COLONNA D 
        COUNT(CASE WHEN tms_perfezionamento IS NOT NULL AND tms_date_payment IS NOT NULL THEN 1 END) AS `Totale notifiche pagate e perfezionate`, -- COLONNA E
        COUNT(CASE WHEN tms_perfezionamento IS NULL AND tms_date_payment IS NOT NULL THEN 1 END) AS `Totale notifiche pagate no perfezionate`, -- COLONNA F
        -- percentuale su pagate e perfezionate COLONNA C/D
        ROUND(
	        COUNT(CASE WHEN tms_date_payment IS NOT NULL THEN 1 END)
	        / NULLIF(
	            COUNT(CASE WHEN tms_perfezionamento IS NOT NULL THEN 1 END),
	            0
	        )
	    , 4) AS `% pagate su perfezionate`, -- COLONNA G
        --Corrispettivo digitale pagate e perfezionate
	    CAST(
	        COUNT(CASE WHEN tms_perfezionamento IS NOT NULL AND tms_date_payment IS NOT NULL THEN 1 END)
	        AS DECIMAL(18,2)
	    ) AS `Corrispettivo digitale pagate e perfezionate`,  -- COLONNA H - impostare nel google Sheet il formato euro nella colonna
	    -- Corrispettivo pagate e perfezionate
	    SUM(CASE WHEN tms_date_payment IS NOT NULL 
		          AND tms_perfezionamento IS NOT NULL 
		     THEN prezzo_ente END
		) AS `Corrispettivo pagate e perfezionate eurocent (ANALOG)`, -- COLONNA I - impostare nel google Sheet il formato euro nella colonna
        -- Corrispettivo digitale pagate e non perfezionate
         CAST(
	        COUNT(CASE WHEN tms_perfezionamento IS NULL  AND tms_date_payment IS NOT NULL THEN 1 END)
	        AS DECIMAL(18,2)
	    ) AS `Corrispettivo digitale pagate e non perfezionate`, --COLONNA J - impostare nel google Sheet il formato euro nella colonna 
	    -- Corrispettivo pagate e non perfezionate
	    SUM(CASE WHEN tms_date_payment IS NOT NULL 
		          AND tms_perfezionamento IS NULL 
		     THEN prezzo_ente END
		) AS `Corrispettivo pagate non perfezionate eurocent (ANALOG)` -- COLONNA K - impostare nel google Sheet il formato euro nella colonna
    FROM base
    WHERE YEAR(sentat) IN (2024, 2025)
    GROUP BY YEAR(sentat), 
             type_notif
    ORDER BY anno ASC, type_notif DESC   
    
    
-- Nome sheet: 6. Tempi medi pagamento
-- Descrizione: ...
    WITH base AS (
        SELECT  
            b.iun,
            b.sentat,
            b.type_notif,
            b.tms_date_payment,
            DATEDIFF(b.tms_date_payment, b.sentat) AS giorni_pagamento
        FROM send.gold_notification_analytics b
        --LEFT JOIN send.gold_postalizzazione_analytics c 
          --  ON b.iun = c.iun
        WHERE b.senderpaid = '53b40136-65f2-424b-acfb-7fae17e35c60'-- INPS    
          AND b.type_notif IS NOT NULL
          --AND c.pcretry_rank = 1
          AND b.tms_date_payment IS NOT NULL
          AND sentat < '2025-09-01' --- FIX
    )
    SELECT
        YEAR(sentat) AS anno,
        type_notif,
        ROUND(AVG(giorni_pagamento), 2) AS media_giorni_pagamento,
        COUNT(*) AS notifiche_pagate,
        SUM(giorni_pagamento) AS somma_giorni_pagamento,
        -- Conteggi per scaglione
        SUM(CASE WHEN giorni_pagamento BETWEEN 0 AND 10 THEN 1 ELSE 0 END) AS count_0_10gg,
        SUM(CASE WHEN giorni_pagamento BETWEEN 11 AND 20 THEN 1 ELSE 0 END) AS count_11_20gg,
        SUM(CASE WHEN giorni_pagamento BETWEEN 21 AND 30 THEN 1 ELSE 0 END) AS count_21_30gg,
        SUM(CASE WHEN giorni_pagamento BETWEEN 31 AND 40 THEN 1 ELSE 0 END) AS count_31_40gg,
        SUM(CASE WHEN giorni_pagamento BETWEEN 41 AND 50 THEN 1 ELSE 0 END) AS count_41_50gg,
        SUM(CASE WHEN giorni_pagamento BETWEEN 51 AND 60 THEN 1 ELSE 0 END) AS count_51_60gg,
        SUM(CASE WHEN giorni_pagamento BETWEEN 61 AND 70 THEN 1 ELSE 0 END) AS count_61_70gg,
        SUM(CASE WHEN giorni_pagamento BETWEEN 71 AND 80 THEN 1 ELSE 0 END) AS count_71_80gg,
        SUM(CASE WHEN giorni_pagamento > 80 THEN 1 ELSE 0 END) AS count_oltre_80gg,
        -- Percentuali per scaglione
        ROUND(SUM(CASE WHEN giorni_pagamento BETWEEN 0 AND 10 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS perc_0_10gg,
        ROUND(SUM(CASE WHEN giorni_pagamento BETWEEN 11 AND 20 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS perc_11_20gg,
        ROUND(SUM(CASE WHEN giorni_pagamento BETWEEN 21 AND 30 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS perc_21_30gg,
        ROUND(SUM(CASE WHEN giorni_pagamento BETWEEN 31 AND 40 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS perc_31_40gg,
        ROUND(SUM(CASE WHEN giorni_pagamento BETWEEN 41 AND 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS perc_41_50gg,
        ROUND(SUM(CASE WHEN giorni_pagamento BETWEEN 51 AND 60 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS perc_51_60gg,
        ROUND(SUM(CASE WHEN giorni_pagamento BETWEEN 61 AND 70 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS perc_61_70gg,
        ROUND(SUM(CASE WHEN giorni_pagamento BETWEEN 71 AND 80 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS perc_71_80gg,
        ROUND(SUM(CASE WHEN giorni_pagamento > 80 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS perc_oltre_80gg
    FROM base
    GROUP BY YEAR(sentat), type_notif 
    ORDER BY anno   
    
    
    
    
---- Analisi Aggregati da aggiungere agli sheet precedenti
    
--- AGGREGATO ANNO
WITH base AS (
    SELECT
        gpa.iun,
        gpa.data_deposito,
        gna.actual_status
    FROM send.gold_postalizzazione_analytics gpa
    JOIN (
        SELECT DISTINCT iun, actual_status
        FROM send.gold_notification_analytics
    ) gna
        ON gpa.iun = gna.iun
    WHERE
        gpa.pcretry_rank = 1
        AND gpa.attempt_rank = 1
        AND gpa.senderpaid IN ( '53b40136-65f2-424b-acfb-7fae17e35c60' )
        AND gpa.data_deposito < '2025-09-01'
),
arricchita AS (
    SELECT
        YEAR(data_deposito) AS anno_deposito,
        actual_status
    FROM base
)
SELECT
    anno_deposito,
    COUNT(*) AS conteggio_totale,
    SUM(CASE 
            WHEN actual_status IN ('EFFECTIVE_DATE', 'VIEWED') 
            THEN 1 ELSE 0 
        END) AS conteggio_perfezionate,
    SUM(CASE 
            WHEN actual_status = 'DELIVERING' 
            THEN 1 ELSE 0 
        END) AS conteggio_delivering
FROM arricchita
GROUP BY anno_deposito
ORDER BY anno_deposito;

---AGGREGATO ANNO-MESE
WITH base AS (
    SELECT
        gpa.iun,
        gpa.data_deposito,
        gna.actual_status
    FROM send.gold_postalizzazione_analytics gpa
    JOIN (
        SELECT DISTINCT iun, actual_status
        FROM send.gold_notification_analytics
    ) gna
        ON gpa.iun = gna.iun
    WHERE
        gpa.pcretry_rank = 1
        AND gpa.attempt_rank = 1
        AND gpa.senderpaid IN ('53b40136-65f2-424b-acfb-7fae17e35c60')
        AND gpa.data_deposito < '2025-09-01'
),
arricchita AS (
    SELECT
        YEAR(data_deposito)  AS anno_deposito,
        MONTH(data_deposito) AS mese_deposito,
        actual_status
    FROM base
)
SELECT
    anno_deposito,
    mese_deposito,
    COUNT(*) AS conteggio_totale,
    SUM(CASE 
            WHEN actual_status IN ('EFFECTIVE_DATE', 'VIEWED') 
            THEN 1 ELSE 0 
        END) AS conteggio_perfezionate,
    SUM(CASE 
            WHEN actual_status = 'DELIVERING' 
            THEN 1 ELSE 0 
        END) AS conteggio_delivering
FROM arricchita
GROUP BY
    anno_deposito,
    mese_deposito
ORDER BY
    anno_deposito,
    mese_deposito;


    
--- Aggregato per recapitista - inserita fix sul codice_oggetto
WITH base AS (
SELECT
	gpa.iun,
	gpa.data_deposito,
	CASE
        WHEN codice_oggetto LIKE 'R14%' THEN 'Fulmine'
        WHEN codice_oggetto LIKE '777%' OR codice_oggetto LIKE 'PSTAQ777%' THEN 'POST & SERVICE'
        WHEN codice_oggetto LIKE '211%' THEN 'RTI Sailpost-Snem'
        WHEN codice_oggetto LIKE '69%' OR codice_oggetto LIKE '381%' OR codice_oggetto LIKE 'RB1%' THEN 'Poste'
        ELSE recapitista_unificato
    END AS recapitista_unif,
	gna.actual_status
FROM
	send.gold_postalizzazione_analytics gpa
JOIN (
	SELECT
		DISTINCT 
            iun,
		actual_status
	FROM
		send.gold_notification_analytics
    ) gna
        ON
	gpa.iun = gna.iun
WHERE
	gpa.pcretry_rank = 1
	AND gpa.attempt_rank = 1
	AND gpa.senderpaid IN ('53b40136-65f2-424b-acfb-7fae17e35c60')
	AND gpa.data_deposito < '2025-09-01' -- FIX inseriti i periodi temporali
),
arricchita AS (
SELECT
	MONTH(data_deposito) AS mese_deposito,
	YEAR(data_deposito) AS anno_deposito,
	recapitista_unif,
	actual_status
FROM
	base
)
SELECT
	--mese_deposito,
	anno_deposito,
	recapitista_unif,
	-- totale indipendente dallo status
	COUNT(*) AS conteggio_totale,
	-- conteggi specifici
	SUM(CASE WHEN actual_status = 'EFFECTIVE_DATE' OR actual_status = 'VIEWED' THEN 1 ELSE 0 END) AS conteggio_perfezionate,
	SUM(CASE WHEN actual_status = 'DELIVERING' THEN 1 ELSE 0 END) AS conteggio_delivering
FROM
	arricchita
GROUP BY
	--mese_deposito,
	anno_deposito,
	recapitista_unif
ORDER BY
	anno_deposito,
	--mese_deposito,
	recapitista_unif;



--- Aggregato per anno_deposito, prodotto
WITH base AS (
SELECT
	gpa.iun,
	gpa.data_deposito,
	gpa.prodotto,
	gna.actual_status
FROM
	send.gold_postalizzazione_analytics gpa
JOIN (
	SELECT
		DISTINCT 
            iun,
		actual_status
	FROM
		send.gold_notification_analytics
    ) gna
        ON
	gpa.iun = gna.iun
WHERE
	gpa.pcretry_rank = 1
	AND gpa.attempt_rank = 1
	AND gpa.senderpaid IN ('53b40136-65f2-424b-acfb-7fae17e35c60')
	AND gpa.data_deposito < '2025-09-01' -- FIX inseriti i periodi temporali
),
arricchita AS (
SELECT
	MONTH(data_deposito) AS mese_deposito,
	YEAR(data_deposito) AS anno_deposito,
	prodotto,
	actual_status
FROM
	base
)
SELECT
	--mese_deposito,
	anno_deposito,
	prodotto,
	-- totale indipendente dallo status
	COUNT(*) AS conteggio_totale,
	-- conteggi specifici
	SUM(CASE WHEN actual_status = 'EFFECTIVE_DATE' OR actual_status = 'VIEWED' THEN 1 ELSE 0 END) AS conteggio_perfezionate,
	SUM(CASE WHEN actual_status = 'DELIVERING' THEN 1 ELSE 0 END) AS conteggio_delivering
FROM
	arricchita
GROUP BY
	--mese_deposito,
	anno_deposito,
	prodotto
ORDER BY
	anno_deposito,
	--mese_deposito,
	prodotto ;
    

--- Scarico totale delle requestid in delivering
WITH base AS (
SELECT
	gpa.iun,
	gpa.data_deposito,
	gpa.prodotto,
	gna.actual_status
FROM
	send.gold_postalizzazione_analytics gpa
JOIN (
	SELECT
		DISTINCT 
            iun,
		actual_status
	FROM
		send.gold_notification_analytics
    ) gna
        ON
	gpa.iun = gna.iun
WHERE
	gpa.pcretry_rank = 1
	AND gpa.attempt_rank = 1
	AND gpa.senderpaid IN ('53b40136-65f2-424b-acfb-7fae17e35c60')
	AND gpa.data_deposito < '2025-09-01' -- FIX inseriti i periodi temporali
),
arricchita AS (
SELECT
	MONTH(data_deposito) AS mese_deposito,
	YEAR(data_deposito) AS anno_deposito,
	prodotto,
	actual_status
FROM
	base
)
SELECT
	SUM(CASE WHEN actual_status = 'DELIVERING' THEN 1 ELSE 0 END) AS conteggio_delivering
FROM
	arricchita

