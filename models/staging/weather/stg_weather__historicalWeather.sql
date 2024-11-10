-- models/staging/weather/stg_weather__historicalWeather.sql

{{ config(
    materialized='table'
) }}

CREATE SCHEMA IF NOT EXISTS weather_data;

CREATE TABLE historical_weather_data (
    airport_code VARCHAR(10),
    time TIMESTAMP,
    temperature_2m FLOAT,
    relative_humidity_2m FLOAT,
    dew_point_2m FLOAT,
    precipitation FLOAT,
    snow_depth FLOAT,
    pressure_msl FLOAT,
    surface_pressure FLOAT,
    cloud_cover INT,
    wind_speed_10m FLOAT,
    wind_direction_10m INT,
    wind_gusts_10m FLOAT
);

COPY weather_data.historical_weather_data
FROM 's3://airline-is459/data-source/weather_data.csv'
IAM_ROLE 'arn:aws:iam::820242926303:role/service-role/AmazonRedshift-CommandsAccessRole-20241017T010122'
FORMAT AS CSV
IGNOREHEADER 1
TIMEFORMAT 'auto';
