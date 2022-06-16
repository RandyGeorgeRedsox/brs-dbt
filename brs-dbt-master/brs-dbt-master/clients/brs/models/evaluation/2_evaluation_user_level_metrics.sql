{{
  config(
    materialized="incremental",
    unique_key="user_snapshot_id",
    cluster_by=["audience_id"]
  )
}}

{%- set metrics = get_metrics() -%}

WITH date_spine AS (
  SELECT
    snapshot_date
  FROM {{ ref('0_evaluation_date_spine') }}
)

SELECT
  d.snapshot_date
  , a.user_id
  , a.audience_id
  , CONCAT(a.user_id, '_', a.audience_id, '_', d.snapshot_date) AS user_snapshot_id
  , a.date_entered_campaign
  , a.is_control
  {% for metric in metrics %}
    , {{metric.name}}.value AS {{metric.name}}
  {% endfor %}
FROM {{ ref('1_evaluation_ids') }} AS a
INNER JOIN date_spine AS d
  ON a.date_entered_campaign <= d.snapshot_date

{% for metric in metrics %}
  LEFT JOIN  {{ ref(metric.name)}} AS {{metric.name}}
    ON a.user_id = {{metric.name}}.user_id
    AND a.audience_id = {{metric.name}}.audience_id
    AND d.snapshot_date = {{metric.name}}.snapshot_date
{% endfor %}



