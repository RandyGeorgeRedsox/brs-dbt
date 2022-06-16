SELECT 
      po1.order_id AS "order id"
    , tn.transaction_id
    , oli2.order_line_item_id
    , po1.order_date AS "order date"
    , EXTRACT(year FROM e1.event_date) AS "season"
    , CASE 
        WHEN oli2.transaction_type_code IN ( 'SA', 'CS', 'RT', 'ER') THEN 'paid'
        WHEN oli2.transaction_type_code IN ( 'RV', 'RL', 'CR' ) THEN 'reserved'  
        ELSE 'UK'
    END AS "payment status"
    , CASE 
        WHEN e1.event_code LIKE '%CA%' THEN 'catch all' 
        WHEN e1.event_code LIKE 'RS%LFE' THEN 'tier license fee' 
        WHEN bt.buyer_type_code LIKE 'MV%' 
            AND sc.service_charge_code LIKE '%MV%' THEN 'misc value' 
        WHEN (bt.buyer_type_code LIKE 'LV%' 
                OR bt.buyer_type_code LIKE 'RD%') 
            AND sc.service_charge_code like '%LV%' THEN 'loaded value' 
        WHEN e1.event_code LIKE '%RS%' THEN 'regular season' 
        ELSE 'unknown' 
    END AS "ticket type"
    , CASE
        WHEN e1.event_code LIKE '%CA%'
        THEN
            (CASE WHEN oli2.transaction_type_code IN ( 'RT', 'ER', 'RL', 'CR' ) 
            THEN SUM(IFNULL(t.price, 0)) * -1 
            ELSE SUM(IFNULL(t.price, 0))
            END)
        ELSE 0
    END AS "catch ALL revenue"
    , CASE
        WHEN e1.event_code LIKE 'RS%LFE'
        THEN 
            (CASE WHEN oli2.transaction_type_code IN ( 'RT', 'ER', 'RL', 'CR' ) 
                THEN SUM(IFNULL(t.price, 0)) * -1 
                ELSE SUM(IFNULL(t.price, 0))
            END)
        ELSE 0
    END AS "tier license fee revenue"
    , CASE
        WHEN bt.buyer_type_code LIKE 'MV%'
            AND sc.service_charge_code LIKE '%MV%' 
            AND e1.event_code NOT LIKE '%CA%'
            AND e1.event_code NOT LIKE 'RS%LFE'
        THEN
            SUM(IFNULL(sc.scamount, 0))
        ELSE 0
        END AS "misc value revenue"
    , CASE
        WHEN ( bt.buyer_type_code LIKE 'LV%' 
            OR bt.buyer_type_code LIKE 'RD%' ) 
            AND sc.service_charge_code LIKE '%LV%'
            AND e1.event_code NOT LIKE '%CA%'
            AND e1.event_code NOT LIKE 'RS%LFE' 
        THEN SUM(IFNULL(sc.scamount, 0))
        ELSE 0
        END AS "loaded value revenue"
    , CASE
        WHEN e1.event_code LIKE '%RS%' THEN
            (CASE WHEN oli2.transaction_type_code IN ( 'RT', 'ER', 'RL', 'CR' ) 
                THEN ((SUM(IFNULL(t.price, 0)) * -1) -  SUM(IFNULL(sc.scamount, 0)))
                ELSE ((SUM(IFNULL(t.price, 0))) -  SUM(IFNULL(sc.scamount, 0)))
            END)
        ELSE 0
    END AS "regular season revenue"
    , e1.event_code AS "event code"
    , e1.event_date AS "event date"
    , transaction_date AS "transaction date"
    , CASE 
        WHEN oli2.transaction_type_code IN ( 'RT', 'ER', 'RL', 'CR' )  THEN SUM(IFNULL(t.price, 0)) * -1
        ELSE SUM(IFNULL(t.price, 0)) 
    END AS "revenue"
    , CASE WHEN e1.event_code LIKE '%RS%' THEN
          (CASE WHEN oli2.transaction_type_code IN ( 'RT', 'ER', 'RL', 'CR' )  THEN count(distinct t.ticket_id) * -1 
          ELSE count(distinct t.ticket_id) END)
    ELSE 0 
    END AS "tickets"
    , SUM(IFNULL(sc.scamount, 0)) AS "service charge amount"
    , CASE
        WHEN oli2.transaction_type_code IN ( 'RT', 'ER', 'RL', 'CR' ) 
        THEN (SUM(IFNULL(t.price, 0)) * -1) - SUM(IFNULL(sc.scamount, 0))
        ELSE (SUM(IFNULL(t.price, 0))) - SUM(IFNULL(sc.scamount, 0))
        END AS "revenue less sc"
    , CASE
        WHEN (((bt.buyer_type_code LIKE 'LV%' 
                    OR bt.buyer_type_code LIKE 'RD%' ) 
                AND sc.service_charge_code LIKE '%LV%')
            OR (bt.buyer_type_code like 'MV%'
                AND sc.service_charge_code LIKE '%MV%'))
        THEN (CASE 
            WHEN oli2.transaction_type_code IN ( 'RT', 'ER', 'RL', 'CR' ) 
            THEN (SUM(IFNULL(t.price, 0)) * -1) - SUM(IFNULL(sc.scamount, 0))
            ELSE (SUM(IFNULL(t.price, 0))) - SUM(IFNULL(sc.scamount, 0)) END)
        ELSE 0
    END AS "lv AND mv rev less sc"
    , sr.formatted_name AS "sales rep"
    , hc.description AS "hold code desc"
    , bt.buyer_type_code AS "buyer type code"
    , bt.description AS "buyer type description"
    , bt.public_description AS "buyer type pub description"
    , CASE
        WHEN bt.buyer_type_code LIKE 'MV%' OR bt.buyer_type_code LIKE 'W%'
        THEN 1
        ELSE 0
        END AS "mv buyer type code"
    , pa.patron_account_name AS "patron account name"
    , pa.patron_account_id AS "patron account id"

FROM 

       (SELECT DISTINCT po1.order_id
             , po1.financial_patron_account_id
             , po1.sales_rep_id
             , po1.order_date
        FROM {{ ref('tdc__patron_order__base') }} po1
        INNER JOIN {{ ref('tdc__order_line_item__base') }} oli1 
            ON oli1.order_id = po1.order_id
                AND transaction_type_code IN ( 'SA', 'CS', 'RT', 'ER', 'RV', 'RL', 'CR' ) --AND market_type_code in ( 'P', 'S' ) 
        INNER JOIN {{ ref('tdc__event__base') }} e1
            ON oli1.event_id = e1.event_id 
                AND EXTRACT(year FROM event_date) >= 2021 AND (event_code LIKE '%CA%' 
                                OR event_code LIKE '%RS%') 
        INNER JOIN {{ ref('tdc__ticket__base') }} t1
            ON po1.order_id = t1.order_id 
                AND (oli1.order_line_item_id = t1.order_line_item_id 
                        OR oli1.order_line_item_id = t1.remove_order_line_item_id )   
        INNER JOIN {{ ref('tdc__buyer_type__base') }} bt 
            ON bt.buyer_type_id = t1.buyer_type_id 
        INNER JOIN {{ ref('tdc__buyer_type_bt_grp__base') }} btbg  
            ON btbg.buyer_type_id = bt.buyer_type_id 
        INNER JOIN {{ ref('tdc__buyer_type_group__base') }} btg 
            ON btbg.buyer_type_group_id = btg.buyer_type_group_id 
                AND btg.description = 'all boston groups'
) po1
        
INNER JOIN {{ ref('tdc__order_line_item__base') }} oli2
    ON oli2.order_id = po1.order_id
        AND oli2.market_type_code IN ('P', 'S')
        AND oli2.transaction_type_code IN ('SA', 'CS', 'RV', 'RT', 'ER', 'RL', 'CR' ) 

INNER JOIN {{ ref('tdc__event__base') }} e1
    ON oli2.event_id = e1.event_id 
        AND EXTRACT(year FROM e1.event_date) >= 2020
        AND (event_code LIKE '%CA%' OR event_code LIKE '%RS%') 

INNER JOIN {{ ref('tdc__ticket__base') }} t 
    ON t.order_id = po1.order_id
        AND (oli2.order_line_item_id = t.order_line_item_id 
            OR oli2.order_line_item_id = t.remove_order_line_item_id )   

INNER JOIN {{ ref('tdc__transaction__base') }} tn 
    on oli2.transaction_id = tn.transaction_id
        AND tn.market_offer_id IS NULL
        --AND tn.transaction_date < '01-sep-19'
                 
INNER JOIN {{ ref('tdc__buyer_type__base') }} bt 
    ON bt.buyer_type_id = t.buyer_type_id 
    AND bt.buyer_type_code != 'BBC'

/**
INNER JOIN redsoxr.buyer_type_bt_grp btbg 
    ON btbg.buyer_type_id = bt.buyer_type_id 
INNER JOIN redsoxr.buyer_type_group btg 
    ON btbg.buyer_type_group_id = btg.buyer_type_group_id 
        AND btg.description = 'all boston groups'
**/

LEFT JOIN {{ ref('tdc__patron_account__base') }} pa 
    ON po1.financial_patron_account_id = pa.patron_account_id 

LEFT JOIN {{ ref('tdc__sales_rep__base') }} sr 
    ON po1.sales_rep_id = sr.sales_rep_id 

LEFT JOIN {{ ref('tdc__hold_code__base') }} hc 
    ON hc.hold_code_id = t.hold_code_id 

LEFT JOIN (SELECT sci.ticket_id, service_charge_code, SUM(sci.actual_amount) AS scamount 
        FROM {{ ref('tdc__service_charge_item__base') }} sci 
        INNER JOIN {{ ref('tdc__service_charge__base') }} sc 
            ON sci.service_charge_id = sc.service_charge_id 
                AND include_in_ticket_price = 1
                AND (service_charge_code LIKE '%MV%' OR service_charge_code LIKE '%LV%')
        GROUP BY sci.ticket_id, service_charge_code) sc 
    ON t.ticket_id = sc.ticket_id 

WHERE bt.buyer_type_code NOT IN ('DXONE', 'CIONE', 'REG', 'RD25', 'WEB')
--AND oli2.transaction_type_code in ( 'SA', 'CS', 'RT', 'ER')
--AND sr.formatted_name LIKE '%Tieri'
--AND po1.order_id = 22044826
--AND tn.transaction_id = 7464141
--AND bt.buyer_type_code = 'REG'
--AND patron_account_id = 7991080

GROUP BY
      po1.order_id
    , tn.transaction_id
    , oli2.order_line_item_id
    , po1.order_date
    , oli2.transaction_type_code
    , e1.event_code
    , e1.event_date
    , tn.transaction_date,
    , sc.service_charge_code
    , sr.formatted_name
    , hc.description
    , bt.buyer_type_code
    , bt.description
    , bt.public_description
    , patron_account_name
    , patron_account_id