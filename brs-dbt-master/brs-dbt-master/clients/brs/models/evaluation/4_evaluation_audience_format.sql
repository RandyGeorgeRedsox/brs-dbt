{{
  config(
    materialized="incremental",
    unique_key="audience_snapshot_id",
    cluster_by=["audience_id"]
  )
}}

{%- set metrics = get_metrics() -%}

WITH control AS (
  SELECT *
  FROM {{ ref('3_evaluation_audience_level_metrics') }}
  WHERE is_control IS True
)

, treatment AS (
  SELECT *
  FROM {{ ref('3_evaluation_audience_level_metrics') }}
  WHERE is_control IS False
)

SELECT  
  COALESCE(control.snapshot_date, treatment.snapshot_date) AS snapshot_date
  , COALESCE(control.audience_id, treatment.audience_id) AS audience_id
  , COALESCE(control.audience_snapshot_id, treatment.audience_snapshot_id) AS audience_snapshot_id
  , COALESCE(control.size, 0) AS control_size
  , COALESCE(treatment.size, 0) AS treatment_size
  , STRUCT(
        COALESCE(treatment.size, 0) + COALESCE(control.size, 0) AS total_size
        , COALESCE(treatment.size, 0) AS treatment_size
        , COALESCE(control.size, 0) AS control_size
        , COALESCE(treatment.size, 0) - COALESCE(control.size, 0) AS uplift
        , COALESCE(treatment.size, 0) / (NULLIF((COALESCE(treatment.size, 0) + COALESCE(control.size, 0)),0) * 100) AS treatment_percent
    ) AS sizing
  , STRUCT(
    {%- for metric in metrics -%}
      STRUCT(
          COALESCE(treatment.{{metric.name}}, 0) AS treatment
          , COALESCE(control.{{metric.name}}, 0) AS control
          , COALESCE(treatment.{{metric.name}}, 0) + COALESCE(control.{{metric.name}}, 0) AS total
          
          , COALESCE(treatment.{{metric.name}}, 0) / NULLIF(COALESCE(treatment.size, 0), 0) AS treatment_norm
          , COALESCE(control.{{metric.name}}, 0) / NULLIF(COALESCE(control.size, 0), 0) AS control_norm
          , COALESCE(control.{{metric.name}}, 0) / NULLIF(COALESCE(control.size, 0), 0) * treatment.size AS adjusted_control
          , (COALESCE(treatment.{{metric.name}}, 0) - (COALESCE(control.{{metric.name}}, 0) / NULLIF(COALESCE(control.size, 0), 0)) * treatment.size) / NULLIF(((COALESCE(control.{{metric.name}}, 0) / NULLIF(COALESCE(control.size, 0), 0)) * treatment.size), 0) AS  perc_diff
          , (COALESCE(treatment.{{metric.name}}, 0) / NULLIF(COALESCE(treatment.size, 0), 0) - COALESCE(control.{{metric.name}}, 0) / NULLIF(COALESCE(control.size, 0), 0)) * COALESCE(treatment.size, 0) AS uplift          
          , '{{metric.unit}}' AS unit -- count, ptc, or currency
          , '{{metric.exclude_types}}' AS exclude_types
          , '{{metric.significance_tests}}' AS significance_tests
      ) AS {{metric.name}}{%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    ) AS metrics
FROM control 
FULL OUTER JOIN treatment
  ON control.snapshot_date = treatment.snapshot_date
  AND control.audience_id = treatment.audience_id