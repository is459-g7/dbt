{{ config(
    schema='BQ2_data',
    materialized='table',
    alias='joined_flights_full_weather'
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
    COALESCE(wo.temperature_2m, avg_weather.avg_temperature_2m) AS origin_temperature_2m,
    COALESCE(wo.relative_humidity_2m, avg_weather.avg_relative_humidity_2m) AS origin_relative_humidity_2m,
    COALESCE(wo.dew_point_2m, avg_weather.avg_dew_point_2m) AS origin_dew_point_2m,
    COALESCE(wo.precipitation, avg_weather.avg_precipitation) AS origin_precipitation,
    COALESCE(wo.snow_depth, avg_weather.avg_snow_depth) AS origin_snow_depth,
    COALESCE(wo.pressure_msl, avg_weather.avg_pressure_msl) AS origin_pressure_msl,
    COALESCE(wo.surface_pressure, avg_weather.avg_surface_pressure) AS origin_surface_pressure,
    COALESCE(wo.cloud_cover, avg_weather.avg_cloud_cover) AS origin_cloud_cover,
    COALESCE(wo.wind_speed_10m, avg_weather.avg_wind_speed_10m) AS origin_wind_speed_10m,
    COALESCE(wo.wind_direction_10m, avg_weather.avg_wind_direction_10m) AS origin_wind_direction_10m,
    COALESCE(wo.wind_gusts_10m, avg_weather.avg_wind_gusts_10m) AS origin_wind_gusts_10m,

    -- Destination weather data with COALESCE for default values
    COALESCE(wd.temperature_2m, avg_weather.avg_temperature_2m) AS dest_temperature_2m,
    COALESCE(wd.relative_humidity_2m, avg_weather.avg_relative_humidity_2m) AS dest_relative_humidity_2m,
    COALESCE(wd.dew_point_2m, avg_weather.avg_dew_point_2m) AS dest_dew_point_2m,
    COALESCE(wd.precipitation, avg_weather.avg_precipitation) AS dest_precipitation,
    COALESCE(wd.snow_depth, avg_weather.avg_snow_depth) AS dest_snow_depth,
    COALESCE(wd.pressure_msl, avg_weather.avg_pressure_msl) AS dest_pressure_msl,
    COALESCE(wd.surface_pressure, avg_weather.avg_surface_pressure) AS dest_surface_pressure,
    COALESCE(wd.cloud_cover, avg_weather.avg_cloud_cover) AS dest_cloud_cover,
    COALESCE(wd.wind_speed_10m, avg_weather.avg_wind_speed_10m) AS dest_wind_speed_10m,
    COALESCE(wd.wind_direction_10m, avg_weather.avg_wind_direction_10m) AS dest_wind_direction_10m,
    COALESCE(wd.wind_gusts_10m, avg_weather.avg_wind_gusts_10m) AS dest_wind_gusts_10m

FROM flight_data.filtered_cali_flights AS f
LEFT JOIN weather_data.historical_weather_data AS wo
ON f.origin = wo.airport_code
   AND f.year = EXTRACT(year FROM wo.time)
   AND f.month = EXTRACT(month FROM wo.time)
   AND f.dayofmonth = EXTRACT(day FROM wo.time)
   AND EXTRACT(hour FROM to_timestamp(LPAD(f.crsdeptime::text, 4, '0'), 'HH24MI')) = EXTRACT(hour FROM wo.time)
LEFT JOIN weather_data.historical_weather_data AS wd
ON f.dest = wd.airport_code
   AND f.year = EXTRACT(year FROM wd.time)
   AND f.month = EXTRACT(month FROM wd.time)
   AND f.dayofmonth = EXTRACT(day FROM wd.time)
   AND EXTRACT(hour FROM to_timestamp(LPAD(f.crsarrtime::text, 4, '0'), 'HH24MI')) = EXTRACT(hour FROM wd.time)
