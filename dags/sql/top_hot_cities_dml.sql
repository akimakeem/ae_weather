
-- Top hot cities in your list per day

CREATE OR REPLACE VIEW top_hot_cities as

With w_readings as
(
SELECT utc_recorded_at,
       To_char(utc_recorded_at, 'yyyy-mm-dd')  AS dt_day,
       Cast(NULLIF
            (Replace(( To_char(utc_recorded_at, 'yyyy-mm-dd') ), '-', ''), '')
            AS INTEGER) AS dt_int,
       city_name,
       temp_deg
FROM   current_weather
)
, daily_temp as
(

 SELECT
      city_name, dt_int, dt_day, max(temp_deg) as max_temp
  FROM
  w_readings
  GROUP BY 1,2,3
), Top_city as
(
SELECT
      city_name, dt_int as day, max_temp as Current_Temp,
      RANK() OVER (PARTITION by dt_int  order by max_temp desc ) as rk
from daily_temp
)
SELECT  day, city_name, Current_Temp FROM Top_city WHERE RK = 1
