WITH viewed AS (
		SELECT DISTINCT 
	             iun,
	             notificationsentat,
				 category AS category_viewed,
				 GET_JSON_OBJECT(details, '$.sourceChannel') AS sourceChannel_viewed,
				 GET_JSON_OBJECT(details, '$.eventTimestamp') AS eventTimestamp_viewed,
		         GET_JSON_OBJECT(details, '$.sourceChannelDetails') AS sourceChannelDetails_viewed
		FROM
			send.silver_timeline
		WHERE
		    GET_JSON_OBJECT(details, '$.sourceChannel') = 'IO'
			AND category = 'NOTIFICATION_VIEWED_CREATION_REQUEST'
			AND (GET_JSON_OBJECT(details, '$.sourceChannelDetails') IS NULL
				 OR GET_JSON_OBJECT(details, '$.sourceChannelDetails') <> 'QR_CODE')
), ship AS (
	SELECT DISTINCT 
	  a.sentat AS data_deposito,
	  a.iun,
	  a.recipients_size > 1 as multidestinatario,
	  p.item.f24.metadataattachment.`ref`.`key` like 'PN_F24%' as f24meta_corretto,
	  p.item.f24.metadataattachment.`ref`.`key` as f24meta
	FROM
	  send.gold_notification_analytics a,
	  a.recipients r,
	  r.payments p
	WHERE
	   p.item.f24.metadataattachment.`ref`.`key` IS NOT NULL
	   AND a.actual_status != "CANCELLED"
) 
SELECT DISTINCT *
FROM viewed v LEFT JOIN ship s  ON v.iun = s.iun
WHERE f24meta_corretto IS TRUE
      AND v.eventTimestamp_viewed >= '2025-11-12'
      AND v.eventTimestamp_viewed <= '2026-02-05';