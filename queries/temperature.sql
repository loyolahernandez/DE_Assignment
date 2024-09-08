SELECT ROUND(AVG(temperature), 2) AS avg_temperature
FROM weather_obs
WHERE 
    TO_TIMESTAMP(timestamp, 'YYYY-MM-DD"T"HH24:MI:SS') AT TIME ZONE 'UTC' >= (NOW() - INTERVAL '7 days')
ORDER BY TO_TIMESTAMP(timestamp, 'YYYY-MM-DD"T"HH24:MI:SS') AT TIME ZONE 'UTC';