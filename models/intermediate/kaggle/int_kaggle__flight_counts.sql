-- models/intermediate/kaggle/int_kaggle__flights_count.sql

{{ config(
    materialized='table'
) }}

SELECT COUNT(*) AS flight_count
FROM flight_data.flights;