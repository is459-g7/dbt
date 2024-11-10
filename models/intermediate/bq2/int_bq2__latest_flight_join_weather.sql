{{ config(
    schema='BQ2_data',
    materialized='table',
    alias='latest_joined_flights_full_weather'
) }}

-- Step 1: Create the joined table without average values for missing data
CREATE SCHEMA IF NOT EXISTS BQ2_data;

-- Create the joined table within the BQ2_data schema
CREATE TABLE BQ2_data.latest_joined_flights_full_weather AS
SELECT 
    -- Flight data fields
    f.year,
    f.month,
    f.dayofweek,
    f.dayofmonth,
    f.crsdeptime,
    f.crsarrtime,
    f.deptime,
    f.arrtime,
    f.depdelay,
    f.arrdelay,
    f.distance,
    f.flightnum,
    f.uniquecarrier,
    f.cancelled,
    f.cancellationcode,
    f.carrierdelay,
    f.weatherdelay,
    f.nasdelay,
    f.securitydelay,
    f.lateaircraftdelay,
    f.origin,
    f.dest,

    -- Origin weather data
    wo.temperature_2m AS origin_temperature_2m,
    wo.relative_humidity_2m AS origin_relative_humidity_2m,
    wo.dew_point_2m AS origin_dew_point_2m,
    wo.precipitation AS origin_precipitation,
    wo.snow_depth AS origin_snow_depth,
    wo.pressure_msl AS origin_pressure_msl,
    wo.surface_pressure AS origin_surface_pressure,
    wo.cloud_cover AS origin_cloud_cover,
    wo.wind_speed_10m AS origin_wind_speed_10m,
    wo.wind_direction_10m AS origin_wind_direction_10m,
    wo.wind_gusts_10m AS origin_wind_gusts_10m,

    -- Destination weather data
    wd.temperature_2m AS dest_temperature_2m,
    wd.relative_humidity_2m AS dest_relative_humidity_2m,
    wd.dew_point_2m AS dest_dew_point_2m,
    wd.precipitation AS dest_precipitation,
    wd.snow_depth AS dest_snow_depth,
    wd.pressure_msl AS dest_pressure_msl,
    wd.surface_pressure AS dest_surface_pressure,
    wd.cloud_cover AS dest_cloud_cover,
    wd.wind_speed_10m AS dest_wind_speed_10m,
    wd.wind_direction_10m AS dest_wind_direction_10m,
    wd.wind_gusts_10m AS dest_wind_gusts_10m

FROM flight_data.filtered_cali_flights AS f

-- Join with origin weather data based on nearest hour and matching date
LEFT JOIN weather_data.latest_weather_data AS wo
ON f.origin = wo.airport_code
   AND f.year = EXTRACT(year FROM wo.time)
   AND f.month = EXTRACT(month FROM wo.time)
   AND f.dayofmonth = EXTRACT(day FROM wo.time)
   AND EXTRACT(hour FROM to_timestamp(LPAD(f.crsdeptime::text, 4, '0'), 'HH24MI')) = EXTRACT(hour FROM wo.time)

-- Join with destination weather data based on nearest hour and matching date
LEFT JOIN weather_data.latest_weather_data AS wd
ON f.dest = wd.airport_code
   AND f.year = EXTRACT(year FROM wd.time)
   AND f.month = EXTRACT(month FROM wd.time)
   AND f.dayofmonth = EXTRACT(day FROM wd.time)
   AND EXTRACT(hour FROM to_timestamp(LPAD(f.crsarrtime::text, 4, '0'), 'HH24MI')) = EXTRACT(hour FROM wd.time);

-- Calculate averages for weather columns in a CTE
WITH avg_weather AS (
    SELECT 
        AVG(temperature_2m) AS avg_temperature_2m,
        AVG(relative_humidity_2m) AS avg_relative_humidity_2m,
        AVG(dew_point_2m) AS avg_dew_point_2m,
        AVG(precipitation) AS avg_precipitation,
        AVG(snow_depth) AS avg_snow_depth,
        AVG(pressure_msl) AS avg_pressure_msl,
        AVG(surface_pressure) AS avg_surface_pressure,
        AVG(cloud_cover) AS avg_cloud_cover,
        AVG(wind_speed_10m) AS avg_wind_speed_10m,
        AVG(wind_direction_10m) AS avg_wind_direction_10m,
        AVG(wind_gusts_10m) AS avg_wind_gusts_10m
    FROM weather_data.historical_weather_data
)

-- Update the joined table with average values where data is missing
UPDATE BQ2_data.latest_joined_flights_full_weather
SET 
    origin_temperature_2m = COALESCE(origin_temperature_2m, (SELECT avg_temperature_2m FROM avg_weather)),
    origin_relative_humidity_2m = COALESCE(origin_relative_humidity_2m, (SELECT avg_relative_humidity_2m FROM avg_weather)),
    origin_dew_point_2m = COALESCE(origin_dew_point_2m, (SELECT avg_dew_point_2m FROM avg_weather)),
    origin_precipitation = COALESCE(origin_precipitation, (SELECT avg_precipitation FROM avg_weather)),
    origin_snow_depth = COALESCE(origin_snow_depth, (SELECT avg_snow_depth FROM avg_weather)),
    origin_pressure_msl = COALESCE(origin_pressure_msl, (SELECT avg_pressure_msl FROM avg_weather)),
    origin_surface_pressure = COALESCE(origin_surface_pressure, (SELECT avg_surface_pressure FROM avg_weather)),
    origin_cloud_cover = COALESCE(origin_cloud_cover, (SELECT avg_cloud_cover FROM avg_weather)),
    origin_wind_speed_10m = COALESCE(origin_wind_speed_10m, (SELECT avg_wind_speed_10m FROM avg_weather)),
    origin_wind_direction_10m = COALESCE(origin_wind_direction_10m, (SELECT avg_wind_direction_10m FROM avg_weather)),
    origin_wind_gusts_10m = COALESCE(origin_wind_gusts_10m, (SELECT avg_wind_gusts_10m FROM avg_weather)),
    
    dest_temperature_2m = COALESCE(dest_temperature_2m, (SELECT avg_temperature_2m FROM avg_weather)),
    dest_relative_humidity_2m = COALESCE(dest_relative_humidity_2m, (SELECT avg_relative_humidity_2m FROM avg_weather)),
    dest_dew_point_2m = COALESCE(dest_dew_point_2m, (SELECT avg_dew_point_2m FROM avg_weather)),
    dest_precipitation = COALESCE(dest_precipitation, (SELECT avg_precipitation FROM avg_weather)),
    dest_snow_depth = COALESCE(dest_snow_depth, (SELECT avg_snow_depth FROM avg_weather)),
    dest_pressure_msl = COALESCE(dest_pressure_msl, (SELECT avg_pressure_msl FROM avg_weather)),
    dest_surface_pressure = COALESCE(dest_surface_pressure, (SELECT avg_surface_pressure FROM avg_weather)),
    dest_cloud_cover = COALESCE(dest_cloud_cover, (SELECT avg_cloud_cover FROM avg_weather)),
    dest_wind_speed_10m = COALESCE(dest_wind_speed_10m, (SELECT avg_wind_speed_10m FROM avg_weather)),
    dest_wind_direction_10m = COALESCE(dest_wind_direction_10m, (SELECT avg_wind_direction_10m FROM avg_weather)),
    dest_wind_gusts_10m = COALESCE(dest_wind_gusts_10m, (SELECT avg_wind_gusts_10m FROM avg_weather))
WHERE
    origin_temperature_2m IS NULL
    OR origin_relative_humidity_2m IS NULL
    OR origin_dew_point_2m IS NULL
    OR origin_precipitation IS NULL
    OR origin_snow_depth IS NULL
    OR origin_pressure_msl IS NULL
    OR origin_surface_pressure IS NULL
    OR origin_cloud_cover IS NULL
    OR origin_wind_speed_10m IS NULL
    OR origin_wind_direction_10m IS NULL
    OR origin_wind_gusts_10m IS NULL
    OR dest_temperature_2m IS NULL
    OR dest_relative_humidity_2m IS NULL
    OR dest_dew_point_2m IS NULL
    OR dest_precipitation IS NULL
    OR dest_snow_depth IS NULL
    OR dest_pressure_msl IS NULL
    OR dest_surface_pressure IS NULL
    OR dest_cloud_cover IS NULL
    OR dest_wind_speed_10m IS NULL
    OR dest_wind_direction_10m IS NULL
    OR dest_wind_gusts_10m IS NULL;

