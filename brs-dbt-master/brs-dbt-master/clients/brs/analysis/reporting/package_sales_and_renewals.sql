select 
        SR.FORMATTED_NAME, 
        OLI.TRANSACTION_TYPE_CODE,
        OLI.Section_Description,
        OLI.Order_ID,
        OLI.Package_ID,
        OLI.Package_Line_ID,
        PO.patron_account_type_code,
        PO.FINANCIAL_PATRON_ACCOUNT_ID,
        SUBSTR(E.Description,1, 4) as Season,
        E.DESCRIPTION,
        RS.DESCRIPTION as Sales_Group,
        OLI.Price as revenueNoRR,
        OLI.Package_Description,
        NVL(RRPrice, 0) as RRRevenue,
        OLI.Price + NVL(RRPrice,0) as revenue,
        PO.PATRON_ACCOUNT_NAME,
        PO.CREATED_DATE AS TRANSACTION_DATE,
        E.EVENT_DATE,
        PRICE_SCALE_DESC,
        OLI.PRICE_SCALE_CODE,
        CASE WHEN SUBSTR(E.Description,1, 4) = 2021 and FULL_SEASON_EQUIV is NULL then 
            (CASE WHEN OLI.Package_Description like '%Tenth%' then 10/81
                WHEN OLI.Package_Description like '%Twenty%' then 20/81
                WHEN OLI.Package_Description like '%TGW%' then 20/81
                WHEN OLI.Package_Description like '%Plan B' then 52/81
                WHEN OLI.Package_Description like '%Plan C' then 29/81
                WHEN OLI.Package_Description like '%Half%' then 40/81
                WHEN OLI.Package_Description like '%Plan S' then 15/81
                END)
            ELSE FULL_SEASON_EQUIV END as FULL_SEASON_EQUIV,
        TOT_SEATS,
        NVL(CASE WHEN OLI.TRANSACTION_TYPE_CODE IN ('SA','CS') THEN TOT_SEATS END, 0) AS SOLD_SEATS,
        NVL(CASE WHEN OLI.TRANSACTION_TYPE_CODE IN ('RV') THEN TOT_SEATS END, 0) AS RESV_SEATS
    FROM
        (select                     
            SA.Price,
            SA.Section_Description,
            NVL(SC.Actual_Amount,0) as RRPrice,
            OL.ORDER_ID,
            OL.EVENT_ID,
            OL.ORDER_LINE_ITEM_ID,
            OL.TRANSACTION_TYPE_CODE,
            PL.PACKAGE_LINE_ID,
            PL.DESCRIPTION AS Package_Description,
            PL.PACKAGE_ID,
            OL.TRANSACTION_ID,
            PL.Price_Scale AS Price_SCALE_DESC,
            FULL_SEASON_EQUIV,
            PL.Price_Scale,
            PL.PRICE_SCALE_CODE,
            TOT_SEATS
        from 
            (select 
                ORDER_LINE_ITEM_ID,
                ORDER_ID,
                EVENT_ID,
                TRANSACTION_TYPE_CODE,
                TRANSACTION_ID,
                PACKAGE_LINE_ID,
                PACKAGE_ID,
                PACKAGE_LIST_LINE_ID
            from REDSOXR.ORDER_LINE_ITEM where TRANSACTION_TYPE_CODE in ('CS', 'SA', 'RV') 
            )OL
      
        INNER join 
            (select 
                T.ORDER_ID,
                T.TRANSACTION_ID,
                T.ORDER_LINE_ITEM_ID,
                T.REMOVE_ORDER_LINE_ITEM_ID,
                SUM(T.PRICE) AS PRice,
                T.PRICE_SCALE_ID,
                SC.Public_Description as Section_Description
            from REDSOXR.TICKET T
            inner join REDSOXR.SEAT S
                ON T.Seat_ID = S.Seat_ID
            INNER JOIN REDSOXR.SECTION SC
                ON SC.section_id = s.section_id
            GROUP BY
                T.ORDER_ID,
                T.TRANSACTION_ID,
                T.ORDER_LINE_ITEM_ID,
                T.REMOVE_ORDER_LINE_ITEM_ID,
                T.PRICE_SCALE_ID,
                SC.Public_Description
            ) SA
            on SA.ORDER_LINE_ITEM_ID = OL.ORDER_LINE_ITEM_ID                   
            and SA.TRANSACTION_ID = OL.TRANSACTION_ID  
            and SA.ORDER_ID = OL.ORDER_ID
            and SA.REMOVE_ORDER_LINE_ITEM_ID is NULL
        
        --pulls in service charges associated with the Royal Rooters Add on
        LEFT JOIN 
                (SELECT 
                    I.ORDER_ID,
                    sum(I.Actual_Amount) as Actual_Amount
                from REDSOXR.service_charge_item I 
                INNER JOIN REDSOXR.service_charge C 
                    ON C.service_charge_id = I.service_charge_id 
                    AND C.public_description LIKE '%Royal%' 
                    AND I.TRANSACTION_TYPE_CODE IN ('CS', 'SA')
                GROUP BY 
                    I.ORDER_ID
                ) SC
                on SC.ORDER_ID = OL.ORDER_ID 
        
        inner JOIN 
            (SELECT DISTINCT 
                PL.PACKAGE_LINE_ID,
                PL.PACKAGE_ID,
                PL.ORDER_ID,
                P.DESCRIPTION,
                FULL_SEASON_EQUIV,
                PS.DESCRIPTION as Price_Scale,
                PS.PRICE_SCALE_CODE,
                PLLS.REFERENCE_PRICE_SCALE_ID,
                COUNT(DISTINCT PLLS.REFERENCE_SEAT_ID) AS TOT_SEATS,
                PLLS.PACKAGE_LIST_LINE_ID,
                PLLS.Section_Description
            FROM REDSOXR.PACKAGE_LINE PL
            INNER JOIN REDSOXR.PACKAGE P
                ON P.PACKAGE_ID = PL.PACKAGE_ID
                and (P.Description like '2021%'
                        OR P.Description like '2020%'
                        OR P.Description like '2019%'
                        OR P.Description like '2018%'
                    ) and P.Description NOT LIKE '%Parking%' and P.Description NOT LIKE '%Spring Training%'
            inner join  REDSOXR.PACKAGE_LIST_LINE PLL
                on PLL.PACKAGE_LINE_ID = PL.PACKAGE_LINE_ID
                and PLL.PACKAGE_ID = PL.PACKAGE_ID
                and PLL.ORDER_ID = PL.ORDER_ID
                and PLL.TRANSACTION_ID = PL.TRANSACTION_ID
            inner join  
                (SELECT 
                    PLLS.PACKAGE_LIST_LINE_ID, 
                    PLLS.PACKAGE_LINE_ID,
                    PLLS.ORDER_ID, 
                    PLLS.CURRENT_TRANSACTION_TYPE_CODE, 
                    PLLS.REFERENCE_PRICE_SCALE_ID,
                    PLLS.REFERENCE_SEAT_ID,
                    SC.Public_Description as Section_Description
                FROM REDSOXR.PACKAGE_LIST_LINE_SEAT PLLS
                inner join REDSOXR.SEAT S
                    ON PLLS.REFERENCE_Seat_ID = S.Seat_ID
                INNER JOIN REDSOXR.SECTION SC
                    ON SC.section_id = s.section_id
                GROUP BY  
                    PLLS.PACKAGE_LIST_LINE_ID, 
                    PLLS.PACKAGE_LINE_ID,
                    PLLS.ORDER_ID, 
                    PLLS.CURRENT_TRANSACTION_TYPE_CODE, 
                    PLLS.REFERENCE_PRICE_SCALE_ID,
                    PLLS.REFERENCE_SEAT_ID,
                    PLLS.REFERENCE_PRICE_SCALE_ID,
                    SC.Public_Description
                ) PLLS
               on PLLS.PACKAGE_LIST_LINE_ID = PLL.PACKAGE_LIST_LINE_ID
               and PLLS.PACKAGE_LINE_ID = PLL.PACKAGE_LINE_ID
               and PLLS.ORDER_ID = PLL.ORDER_ID
               and PLLS.CURRENT_TRANSACTION_TYPE_CODE IN ('RV', 'SA', 'CS')
                
            left join REDSOXR.PRICE_SCALE PS
                on PS.PRICE_SCALE_ID = PLLS.REFERENCE_PRICE_SCALE_ID

            GROUP BY 
                pl.package_line_id,
                PL.PACKAGE_ID,
                PL.ORDER_ID,
                P.DESCRIPTION,
                FULL_SEASON_EQUIV,
                PS.DESCRIPTION,
                PS.PRICE_SCALE_CODE,
                PLLS.REFERENCE_PRICE_SCALE_ID,
                PLLS.PACKAGE_LIST_LINE_ID,
                Section_Description
            ) PL                 
               on PL.PACKAGE_ID = OL.PACKAGE_ID
               and PL.ORDER_ID = OL.ORDER_ID
               and PL.REFERENCE_PRICE_SCALE_ID = SA.PRICE_SCALE_ID
               and OL.PACKAGE_LIST_LINE_ID = PL.PACKAGE_LIST_LINE_ID
               and PL.Section_Description = SA.Section_DESCRIPTION
               
              -- where OL.ORDER_ID = 25364472
        ) OLI
               
    inner join 
        (select 
            FINANCIAL_PATRON_ACCOUNT_ID,
            SALES_REP_ID,
            ORDER_ID,
            PATRON_ACCOUNT_TYPE_CODE,
            PA.PATRON_ACCOUNT_NAME,
            P1.CREATED_DATE
        from REDSOXR.PATRON_ORDER P1
        inner join REDSOXR.PATRON_ACCOUNT PA
            on PA.PATRON_ACCOUNT_ID = P1.FINANCIAL_PATRON_ACCOUNT_ID
            AND PA.Patron_Account_ID != 1112111
        ) PO                 
        on PO.ORDER_ID = OLI.ORDER_ID      
      
    inner join 
        (select 
            EVENT_ID,
            EVENT_CODE,
            EVENT_DATE,
            DESCRIPTION               
        from REDSOXR.EVENT
        WHERE Description like '%Plan%'
        ) E     
        on E.EVENT_ID = OLI.EVENT_ID         
        and (EVENT_CODE like '%21%' or EVENT_CODE like 'RS21%' OR
            EVENT_CODE like '%20%' or EVENT_CODE like 'RS20%' OR
            EVENT_CODE like '%19%' or EVENT_CODE like 'RS19%' OR
            EVENT_CODE like '%18%' or EVENT_CODE like 'RS18%' OR
            EVENT_CODE like '%17%' or EVENT_CODE like 'RS17%')
      
    left join 
        (select 
            SALES_REP_ID,
            SALES_REP_SUB_GROUP_ID,
            FORMATTED_NAME                
        from REDSOXR.SALES_REP
        ) SR     
        on SR.SALES_REP_ID = PO.SALES_REP_ID    

    left join REDSOXR.SALES_REP_SUB_GROUP RS                 
        on RS.SALES_REP_SUB_GROUP_ID = SR.SALES_REP_SUB_GROUP_ID                   
        --and RS.DESCRIPTION IN ('Sales Academy','Boston Group Sales')      
          
    group by 
        SR.FORMATTED_NAME, 
        OLI.TRANSACTION_TYPE_CODE, 
        OLI.Section_Description,
        PO.patron_account_type_code, 
        PO.FINANCIAL_PATRON_ACCOUNT_ID,
        E.DESCRIPTION,
        RS.DESCRIPTION,
        OLI.Price,
        OLI.RRPrice,
        OLI.Order_ID,
        OLI.Package_ID,
        OLI.Package_Line_ID,
        OLI.Package_Description,
        PO.PATRON_ACCOUNT_NAME,
        PO.CREATED_DATE,
        E.EVENT_DATE,
        OLI.PRICE_SCALE_DESC,
        OLI.PRICE_SCALE_CODE,
        TOT_SEATS,
        FULL_SEASON_EQUIV