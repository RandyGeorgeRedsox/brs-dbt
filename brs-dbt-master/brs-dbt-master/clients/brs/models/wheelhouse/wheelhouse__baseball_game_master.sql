{{
    config(
        cluster_by=['game_pk']
      , materialized= 'view'
    )
}}

SELECT  game_pk
      , game_type
      , game_nbr
      , double_header
      , double_header_type
      , venue_name
      , venue_city
      , venue_state
      , turf_type
      , bis_game_id
      , makeup_game_flag
      , game_description
      , game_time_local
      , day_night_code
      , start_time
      , game_status
      , game_elapsed_mins
      , home_team
      , home_team_code
      , home_score
      , home_hits
      , home_errors
      , away_team
      , away_team_code
      , away_score
      , away_hits
      , away_errors
      , innings
      , attendance
      , temperature
      , wind_speed
      , wind_direction
      , home_series_nbr
      , away_series_nbr
      , series_num_games
      , game_time_home
      , game_time_away
      , game_time_tbd
      , tickets_link
      , wp_last_name
      , wp_first_name
      , wp_player_id
      , wp_team_code
      , wp_wins
      , wp_losses
      , lp_last_name
      , lp_first_name
      , lp_player_id
      , lp_team_code
      , lp_wins
      , lp_losses
      , game_rank_nbr
      , weather_condition

FROM    {{ source('wheelhouse_red_sox', 'baseball_game_master') }} wh
  
