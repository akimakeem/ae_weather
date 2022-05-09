CREATE TABLE IF NOT EXISTS latest_weather (
    city_id             BIGINT,
    city_name           VARCHAR(256),
    Temp_deg            FLOAT,
    utc_recorded_at     TIMESTAMPTZ,
    PRIMARY KEY (city_id, utc_recorded_at)
);

INSERT INTO  latest_weather
SELECT  city_id,city_name,temp_deg,utc_recorded_at
from
(
SELECT city_id, city_name,temp_deg,utc_recorded_at
 ,row_number() over (partition by city_name order by  utc_recorded_at desc) rn
FROM CURRENT_WEATHER
group by city_id, city_name,temp_deg,utc_recorded_at
 ) aa where rn = 1
ON CONFLICT (city_id) DO UPDATE set temp_deg = excluded.temp_deg