{{
  config(
    materialized="ephemeral",
    unique_key="audience_snapshot_id",
    cluster_by=["audience_id"]
  )
}}

{%- set metrics = get_metrics() -%}

SELECT  
  snapshot_date
  , audience_id
  , is_control
  , CONCAT(audience_id, '_', CASE WHEN is_control THEN 'control' ELSE 'treatment' END, '_', snapshot_date) AS audience_snapshot_id
  , COUNT(*) AS size

  -- Metrics
  {% for metric in metrics %}
    , SUM({{metric.name}}) AS {{metric.name}}
  {% endfor %}
FROM {{ ref('2_evaluation_user_level_metrics') }}
{% if is_incremental() %}
  WHERE snapshot_date > (SELECT MAX(snapshot_date) FROM {{ this }})
{% endif %}
GROUP BY 
  snapshot_date
  , audience_id
  , is_control
