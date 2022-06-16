SELECT  AGENCY_ID AS agency_id
    --   , CREATED_BY_OPERATOR_ID
      , CREATED_DATE AS created_ts
    --   , LAST_UPDATED_BY_OPERATOR_ID
      , LAST_UPDATED_DATE AS last_updated_ts
      , AGENCY_CODE AS agency_code
      , DESCRIPTION AS agency_description
    --   , DISPLAY_ORDER
      , IF(ACTIVE = 1, TRUE, FALSE) AS is_active
    --   , EXTERNAL_CLIENT
    --   , SETTLEMENT_SALES_CHANNEL_CODE
    --   , TIME_ZONE_CODE
    --   , AGENCY_ORG_GROUP_ID
    --   , CASH_CARD_PAYMENT_FORMAT_ID
    --   , CASH_CARD_REVENUE_FORMAT_ID
    --   , CHANNEL_ID
    --   , PARENT_AGENCY_ID
    --   , EMAIL_CONFIG_PROFILE_ID
    --   , DATA_PROT_UNIT_INHERITED

FROM    {{ source('wheelhouse_red_sox_ticketing', 'agency') }}