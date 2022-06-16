SELECT  survey_id
      , response_id
      , element_name
      , CONCAT(response_id, '_', element_name) AS pk
      , element_value
      , start_date
      , end_date
      , recorded_date
      , team_nickname

FROM    {{ source('wheelhouse_red_sox', 'qualtrics_responses_full')}}
