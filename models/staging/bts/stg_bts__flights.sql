-- models/staging/kaggle/stg_kaggle__flights.sql

{{ config(
    materialized='table'
) }}

CREATE SCHEMA IF NOT EXISTS flight_data;

CREATE TABLE IF NOT EXISTS flight_data.latest_flights (
    Year SMALLINT NULL,
    Month SMALLINT NULL,
    DayOfWeek SMALLINT NULL,
    DayOfMonth SMALLINT NULL,
    CRSDepTime SMALLINT NULL,
    CRSArrTime SMALLINT NULL,
    DepTime SMALLINT NULL,
    ArrTime SMALLINT NULL,
    DepDelay SMALLINT NULL,
    ArrDelay SMALLINT NULL,
    Distance SMALLINT NULL,
    FlightNum VARCHAR(10) NULL,
    UniqueCarrier VARCHAR(10) NULL,
    Cancelled VARCHAR(5) NULL,
    CancellationCode VARCHAR(5) NULL,
    CarrierDelay SMALLINT NULL,
    WeatherDelay SMALLINT NULL,
    NASDelay SMALLINT NULL,
    SecurityDelay SMALLINT NULL,
    LateAircraftDelay SMALLINT NULL,
    Origin VARCHAR(5) NULL,
    Dest VARCHAR(5) NULL
);

COPY flight_data.latest_flights
FROM 's3://airline-is459/data-source/restructured_airline_18_23.csv'
IAM_ROLE 'arn:aws:iam::820242926303:role/service-role/AmazonRedshift-CommandsAccessRole-20241017T010122'
CSV
DELIMITER ','
IGNOREHEADER 1
NULL AS 'NA'
ACCEPTINVCHARS;