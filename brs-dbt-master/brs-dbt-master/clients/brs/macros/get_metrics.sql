{# Return the metrics for evaluation #}

{% macro get_metrics() %}
  {# Add all metrics here 👇 #}
  {{ return([{"name":"revenue", "unit":"currency", "exclude_types":"", "significance_tests":""}, {"name":"opportunity_total", "unit":"", "exclude_types":"uplift, perc_diff", "significance_tests":""}, {"name":"opportunity_is_closed_won", "unit":"", "exclude_types":"uplift, perc_diff", "significance_tests":""}, {"name":"opportunity_closed_won_value", "unit":"", "exclude_types":"uplift, perc_diff", "significance_tests":""}]) }}
{% endmacro %}