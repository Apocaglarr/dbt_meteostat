WITH flight_data AS (
    SELECT 
        origin AS airport_code, 
        dest AS destination_code, 
        cancelled, 
        diverted, 
        airline,
        --flight_number,
        tail_number
    FROM {{ref('prep_flights')}}
    UNION ALL
    SELECT 
        dest AS airport_code, 
        origin AS destination_code, 
        cancelled, 
        diverted, 
        airline,
        --flight_number,
        tail_number
    FROM {{ref('prep_flights')}}
)
SELECT 
    a.name AS airport_name, 
    a.city, 
    a.country,
    COUNT(DISTINCT f.destination_code) AS unique_connections,
    COUNT(*) AS total_flights_planned,
    SUM(CASE WHEN f.cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled,
    SUM(CASE WHEN f.diverted = 1 THEN 1 ELSE 0 END) AS total_diverted,
    SUM(CASE WHEN f.cancelled = 0 AND f.diverted = 0 THEN 1 ELSE 0 END) AS actual_flights,
    COUNT(DISTINCT f.tail_number) AS unique_airplanes,
    COUNT(DISTINCT f.airline) AS unique_airlines
FROM flight_data f
JOIN prep_airports a ON f.airport_code = a.faa
GROUP BY a.name, a.city, a.country
ORDER BY total_flights_planned DESC