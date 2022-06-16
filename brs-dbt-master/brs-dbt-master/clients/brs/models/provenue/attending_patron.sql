SELECT
-- ES.TICKET_ID,
apa.account_id,
es.attending_patron_account_id,
-- apa.account_name as attending_account_name,
-- apa.account_type_code as attending_account_type,
apc.contact_id as attending_contact_id,
-- apc.formatted_name as attending_contact_formatted_name,
-- apce.email as attending_contact_email,
es.financial_patron_account_id,
-- fpa.account_name as financial_account_name,
-- fpa.account_type_code as financial_account_type,
-- es.price,
-- es.price_scale,
-- ps.price_scale_code,
-- ps.public_description,
-- e.event_code,
-- e.event_date,
-- s.section_code,
-- td.delivery_id,
-- td.ticket_delivery_status_code,
-- tds.ticket_delivery_status_desc,
-- td.delivery_forward_status_code,
-- td.transaction_id,
-- d.created_date,
COUNT(ES.Ticket_ID) as num_tix

FROM {{ ref('tdc__event_seat__base') }} ES
INNER JOIN {{ ref('tdc__event__base') }} E
	ON ES.EVENT_ID = E.EVENT_ID
	AND (E.EVENT_CODE like CONCAT(SUBSTR(CAST(EXTRACT(YEAR FROM CURRENT_DATE()) AS STRING), 3, 2), 'RS%') 
					OR E.EVENT_CODE like CONCAT(SUBSTR(CAST(EXTRACT(YEAR FROM current_date()) AS STRING) , 3, 2),  'SD%'))
-- INNER JOIN {{ ref('tdc__section__base') }} S
-- 	ON ES.SECTION_ID = S.SECTION_ID
-- INNER JOIN {{ ref('tdc__price_scale__base') }} PS
-- 	ON ES.PRICE_SCALE = PS.PRICE_SCALE_ID
INNER JOIN {{ ref('tdc__ticket_delivery__base') }} TD
	ON ES.TICKET_ID = TD.TICKET_ID
	AND TD.TICKET_DELIVERY_STATUS_CODE = 'A'
-- INNER JOIN {{ ref('tdc__ticket_delivery_status__base') }} TDS
-- 	ON TD.TICKET_DELIVERY_STATUS_CODE = TDS.TICKET_DELIVERY_STATUS_CODE
INNER JOIN {{ ref('tdc__delivery__base') }} D
	ON TD.DELIVERY_ID = D.DELIVERY_ID
INNER JOIN {{ ref('tdc__patron_account__base') }} FPA
	ON ES.FINANCIAL_PATRON_ACCOUNT_ID = FPA.ACCOUNT_ID
INNER JOIN {{ ref('tdc__patron_account__base') }} APA
	ON ES.ATTENDING_PATRON_ACCOUNT_ID = APA.ACCOUNT_ID
	AND APA.ACCOUNT_ID != 416006
INNER JOIN {{ ref('tdc__patron_contact__base') }} APC
	ON APA.ACCOUNT_ID = APC.ACCOUNT_NAME
	AND APC.IS_PRIMARY = TRUE
-- INNER JOIN {{ ref('tdc__patron_contact_email__base') }} APCE
-- 	ON APC.CONTACT_ID = APCE.CONTACT_ID
-- 	AND APCE.IS_PRIMARY = TRUE

WHERE APA.ACCOUNT_ID != FPA.ACCOUNT_ID
	AND EXTRACT(YEAR FROM e.event_date) = 2021

GROUP BY
--ES.TICKET_ID,
es.attending_patron_account_id,
apa.account_id,
apa.account_type_code,
apc.contact_id,
-- apc.formatted_name,
-- apce.email,
es.financial_patron_account_id
-- fpa.account_name,
-- fpa.account_type_code,
-- es.price,
-- es.price_scale,
-- ps.price_scale_code,
-- ps.public_description,
-- e.event_code,
-- e.event_date,
-- td.delivery_id,
-- td.ticket_delivery_status_code,
-- tds.ticket_delivery_status_desc,
-- td.delivery_forward_status_code,
-- td.transaction_id,
-- d.created_date,
-- s.section_code
