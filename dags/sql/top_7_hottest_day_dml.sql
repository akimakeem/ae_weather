
---- Top 7 hottest day per city in each calendar year
CREATE OR REPLACE VIEW top_7_hottest_day as

With w_readings as
(
SELECT utc_recorded_at,
       To_char(utc_recorded_at, 'yyyy-mm-dd')  AS dt_day,
       Cast(NULLIF
            (Replace(( To_char(utc_recorded_at, 'yyyy-mm-dd') ), '-', ''), '')
            AS INTEGER) AS dt_int,
       EXTRACT(YEAR FROM utc_recorded_at) dt_year,
       city_name,
       temp_deg
FROM   current_weather
)
, daily_temp as
(

 SELECT
      city_name, dt_int, dt_day, dt_year, max(temp_deg) as max_temp
  FROM
      w_readings
  GROUP BY 1,2,3,4

), Top_city as
(
SELECT
      city_name, dt_int as day, max_temp as Current_Temp,
      RANK() over ( partition by city_name, dt_year, dt_int order by max_temp desc ) as rk
from daily_temp
)
SELECT  day, city_name, Current_Temp FROM Top_city WHERE RK <= 7

