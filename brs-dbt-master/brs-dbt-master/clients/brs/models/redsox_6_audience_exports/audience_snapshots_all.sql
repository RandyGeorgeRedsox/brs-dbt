{% set audience_relations = dbt_utils.get_relations_by_pattern('redsox_6_audience_exports', 'redsox_6_snapshot%%') %}


{% set table_exists=audience_relations != []   %}

{% if table_exists %}

SELECT *
FROM {{ dbt_utils.union_relations(relations=audience_relations, source_column_name='source_table') }}

{% else %}

select
  CAST(null AS STRING) AS source
, CAST(null AS INT64) AS audience_id
, CAST(null AS STRING) AS audience_name
, CAST(null AS TIMESTAMP) AS snapshot_ts
, CAST(null AS STRING) AS email
, CAST(null AS STRING) AS last_name
, CAST(null AS STRING) AS first_name
, CAST(null AS INT64) AS device_id
, CAST(null AS BOOL) AS is_control
, CAST(null AS STRING) AS user_id
, CAST(null AS STRING) AS destination
, CAST(null AS STRING) AS email_type
, CAST(null AS INT64) AS mlb_scrub_query_id
, CAST(null AS INT64) AS email_list_id
, CAST(null AS STRING) AS campaign_id
, CAST(null AS STRING) AS campaign_type
, CAST(null AS STRING) AS dynamic_content
, CAST(null AS BOOL) AS info_collected_via_mlb_privacy_policy
, CAST(null AS BOOL) AS end_user_opt_in
, CAST(null AS BOOL) AS opt_in_language_approved_vetted
, CAST(null AS STRING) AS organization

{% endif %}
