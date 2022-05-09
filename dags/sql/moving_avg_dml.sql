--Moving average of the temperature per city for 5 readings

---Assumption : One reading is average of all reading in a day
CREATE OR REPLACE VIEW moving_avg as

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
      city_name, dt_int, dt_day, avg(temp_deg) as daily_temp
  FROM
  w_readings
  GROUP BY 1,2,3
)


SELECT
       dt_day,
       city_name,
       ROUND(CAST(avg(daily_temp)
             OVER (order by dt_int ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)AS NUMERIC),2)
             AS avg_temp
 FROM
 daily_temp
