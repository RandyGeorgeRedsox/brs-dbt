SELECT  *
FROM    {{ source('wheelhouse_red_sox', 'baseball_game_master') }} 