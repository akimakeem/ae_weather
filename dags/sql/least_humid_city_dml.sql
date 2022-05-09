--The least humid city per state

CREATE OR REPLACE VIEW east_humid_city as
select  city_name, min_humid
from
(
select city_name,  min_humid,
rank() over (partition by city_name, country  order by min_humid desc ) as rk
from
(

select
-- state not present in API response data
	city_name, country, min(humidity_pct) as min_humid

from
(
select city_name, temp_deg,humidity_pct,country
From current_weather
)  a group by 1,2
order by 2 desc ,1
) b
) c where rk = 1