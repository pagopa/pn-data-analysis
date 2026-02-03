--COSTI NOTIFICA ASYNC v3
WITH selectedNotification AS (
  SELECT a.iun, 
    a.recipients_size > 1 as multidestinatario,
    pagopaintmode,
    r.pos as recindex,
    p.creditorTaxId, 
    p.noticecode, 
    p.applyCost, 
    actual_status='REFUSED' as refused, 
    actual_status='CANCELLED' as cancelled
  FROM gold_notification_analytics a, a.recipients r, r.payments p
  -- effettuare in questo punto la s
), analogCost AS (
  SELECT iun, get_json_object(details, '$.recIndex') as recindex, sum(cast(get_json_object(details, '$.analogCost') as int) ) as analogCost
  FROM silver_timeline WHERE category IN ('SEND_ANALOG_DOMICILE', 'SEND_SIMPLE_REGISTERED_LETTER')
  GROUP BY iun,  get_json_object(details, '$.recIndex') 
), costDetails AS( 
  SELECT n.senderpaid, a.*, n.sentat as data_creazione_notifica, 
    notificationfeepolicy,
    coalesce(n.amount, 0) as notifica_costo_imputato_pa,
    n.amount is not NUll as  notifica_costo_imputato_pa_valorizzato,
    coalesce(pafee, 0) as notifica_costi_pa_eurocent,
    coalesce(vat, 22) as notifica_iva_pa,
    coalesce(c.analogCost, 0) as costi_spedizione_eurocent,
    row_number() over (partition by a.creditorTaxId, a.noticecode order by n.sentat desc) as indice
  FROM selectedNotification a LEFT JOIN silver_notification n ON a.iun = n.iun
  LEFT JOIN analogCost c ON a.iun = c.iun AND a.recindex = cast(c.recindex as BIGINT)
  WHERE year(n.sentat) > 2023 AND noticecode is not NULL 
) SELECT *,
  CASE 
    WHEN NOT applyCost THEN 0
    WHEN refused THEN 0 
    WHEN cancelled THEN 0
    WHEN notificationfeepolicy = 'FLAT_RATE' THEN 0
    ELSE cast(round(notifica_costi_pa_eurocent + 100 + (costi_spedizione_eurocent * (1+ notifica_iva_pa/100))) as int)
  END as costi_notifica_cittadino_eurocent
FROM costDetails
WHERE indice = 1
ORDER BY data_creazione_notifica DESC, iun;