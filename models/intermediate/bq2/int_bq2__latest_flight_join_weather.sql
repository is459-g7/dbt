{{ config(
    schema='BQ2_data',
    materialized='table',
    alias='latest_joined_flights_full_weather'
) }}

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

    -- Origin weather data with COALESCE for default values
    COALESCE(wo.temperature_2m, (SELECT avg_temperature_2m FROM avg_weather)) AS origin_temperature_2m,
    COALESCE(wo.relative_humidity_2m, (SELECT avg_relative_humidity_2m FROM avg_weather)) AS origin_relative_humidity_2m,
    COALESCE(wo.dew_point_2m, (SELECT avg_dew_point_2m FROM avg_weather)) AS origin_dew_point_2m,
    COALESCE(wo.precipitation, (SELECT avg_precipitation FROM avg_weather)) AS origin_precipitation,
    COALESCE(wo.snow_depth, (SELECT avg_snow_depth FROM avg_weather)) AS origin_snow_depth,
    COALESCE(wo.pressure_msl, (SELECT avg_pressure_msl FROM avg_weather)) AS origin_pressure_msl,
    COALESCE(wo.surface_pressure, (SELECT avg_surface_pressure FROM avg_weather)) AS origin_surface_pressure,
    COALESCE(wo.cloud_cover, (SELECT avg_cloud_cover FROM avg_weather)) AS origin_cloud_cover,
    COALESCE(wo.wind_speed_10m, (SELECT avg_wind_speed_10m FROM avg_weather)) AS origin_wind_speed_10m,
    COALESCE(wo.wind_direction_10m, (SELECT avg_wind_direction_10m FROM avg_weather)) AS origin_wind_direction_10m,
    COALESCE(wo.wind_gusts_10m, (SELECT avg_wind_gusts_10m FROM avg_weather)) AS origin_wind_gusts_10m,

    -- Destination weather data with COALESCE for default values
    COALESCE(wd.temperature_2m, (SELECT avg_temperature_2m FROM avg_weather)) AS dest_temperature_2m,
    COALESCE(wd.relative_humidity_2m, (SELECT avg_relative_humidity_2m FROM avg_weather)) AS dest_relative_humidity_2m,
    COALESCE(wd.dew_point_2m, (SELECT avg_dew_point_2m FROM avg_weather)) AS dest_dew_point_2m,
    COALESCE(wd.precipitation, (SELECT avg_precipitation FROM avg_weather)) AS dest_precipitation,
    COALESCE(wd.snow_depth, (SELECT avg_snow_depth FROM avg_weather)) AS dest_snow_depth,
    COALESCE(wd.pressure_msl, (SELECT avg_pressure_msl FROM avg_weather)) AS dest_pressure_msl,
    COALESCE(wd.surface_pressure, (SELECT avg_surface_pressure FROM avg_weather)) AS dest_surface_pressure,
    COALESCE(wd.cloud_cover, (SELECT avg_cloud_cover FROM avg_weather)) AS dest_cloud_cover,
    COALESCE(wd.wind_speed_10m, (SELECT avg_wind_speed_10m FROM avg_weather)) AS dest_wind_speed_10m,
    COALESCE(wd.wind_direction_10m, (SELECT avg_wind_direction_10m FROM avg_weather)) AS dest_wind_direction_10m,
    COALESCE(wd.wind_gusts_10m, (SELECT avg_wind_gusts_10m FROM avg_weather)) AS dest_wind_gusts_10m

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
   AND EXTRACT(hour FROM to_timestamp(LPAD(f.crsarrtime::text, 4, '0'), 'HH24MI')) = EXTRACT(hour FROM wd.time)
