-- models/flights.sql

{{ config(
    materialized='table'
) }}

CREATE SCHEMA IF NOT EXISTS flight_data;

CREATE TABLE IF NOT EXISTS flight_data.flights (
    Year VARCHAR(10),
    Month VARCHAR(10),
    DayofMonth VARCHAR(10),
    DayOfWeek VARCHAR(10),
    DepTime VARCHAR(10),
    CRSDepTime VARCHAR(10),
    ArrTime VARCHAR(10),
    CRSArrTime VARCHAR(10),
    UniqueCarrier VARCHAR(10),
    FlightNum VARCHAR(10),
    TailNum VARCHAR(20),
    ActualElapsedTime VARCHAR(10),
    CRSElapsedTime VARCHAR(10),
    AirTime VARCHAR(10),
    ArrDelay VARCHAR(10),
    DepDelay VARCHAR(10),
    Origin VARCHAR(10),
    Dest VARCHAR(10),
    Distance VARCHAR(10),
    TaxiIn VARCHAR(10),
    TaxiOut VARCHAR(10),
    Cancelled VARCHAR(10),
    CancellationCode VARCHAR(10),
    Diverted VARCHAR(10),
    CarrierDelay VARCHAR(10),
    WeatherDelay VARCHAR(10),
    NASDelay VARCHAR(10),
    SecurityDelay VARCHAR(10),
    LateAircraftDelay VARCHAR(10)
);

COPY flight_data.flights
FROM 's3://airline-is459/data-source/airline.csv.shuffle'
ACCEPTINVCHARS
DELIMITER ','
IGNOREHEADER 1
IAM_ROLE 'arn:aws:iam::820242926303:role/service-role/AmazonRedshift-CommandsAccessRole-20241017T010122';
