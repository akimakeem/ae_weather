INSERT INTO current_weather
SELECT
	CAST(raw_data ->> 'id' AS BIGINT) city_id,
	raw_data ->> 'name' city_name,
	raw_data -> 'sys'->> 'country' country,
	to_timestamp( cast(raw_data ->> 'dt' as int)) at time zone 'UTC' utc_recorded_at,
	CAST(raw_data ->> 'timezone' AS INTERVAL) tz_offset_hours,
	CAST(raw_data -> 'coord' ->> 'lat' AS NUMERIC) lat,
	CAST(raw_data -> 'coord' ->> 'lon' AS NUMERIC) lon,
	jsonb_array_elements(raw_data -> 'weather') ->> 'main' weather_type,
	jsonb_array_elements(raw_data -> 'weather') ->> 'description' weather_desc,
	'Metrics' measure_units,
	CAST(raw_data ->> 'visibility' AS FLOAT) visibility_pct, --pct?
	CAST(raw_data -> 'clouds' ->> 'all' AS FLOAT) cloud_pct,
	CAST(raw_data -> 'main' ->> 'temp' AS FLOAT) temp_deg,
	CAST(raw_data -> 'main' ->> 'humidity' AS FLOAT) humidity_pct,
	CAST(raw_data -> 'main' ->> 'pressure' AS FLOAT) pressure,
	CAST(raw_data -> 'main' ->> 'temp_min' AS FLOAT) temp_min,
	CAST(raw_data -> 'main' ->> 'temp_max' AS FLOAT) temp_max,
	CAST(raw_data -> 'main' ->> 'feels_like' AS FLOAT) feels_like,
	CAST(raw_data -> 'wind' ->> 'deg' AS FLOAT) wind_deg,
	CAST(raw_data -> 'wind' ->> 'gust' AS FLOAT) wind_gust,
	CAST(raw_data -> 'wind' ->> 'speed' AS FLOAT) wind_speed
FROM raw_current_weather
ON CONFLICT DO NOTHING;