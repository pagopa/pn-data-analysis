SELECT
    a.iun,
    a.requestid,
    a.statusrequest,
    a.senderpaid,
    c.senderdenomination,
    b.gestione_economica_ciclo_passivo,
    a.tentativo_recapito_stato,
    a.certificazione_recapito_stato,
    a.fine_recapito_stato
FROM send.gold_postalizzazione_analytics a
LEFT JOIN send_dev.oggetti_bloccati b
    ON a.iun = b.iun
LEFT JOIN send.silver_notification c
    ON a.iun = c.iun
WHERE
    (
        a.statusrequest IN ('PN998', 'PN999')
        OR a.tentativo_recapito_stato IN ('PN998', 'PN999')
        OR a.certificazione_recapito_stato IN ('PN998', 'PN999')
        OR a.fine_recapito_stato IN ('PN998', 'PN999')
    )
    AND (
        b.gestione_economica_ciclo_passivo IS NULL
        OR UPPER(TRIM(b.gestione_economica_ciclo_passivo)) = 'TBD'
    )
    AND a.data_deposito >= DATE '2025-01-01'
    AND a.data_deposito < DATE '2026-07-01';