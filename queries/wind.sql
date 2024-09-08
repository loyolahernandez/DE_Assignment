WITH wind_diffs AS (
    SELECT 
        TO_TIMESTAMP(timestamp, 'YYYY-MM-DD"T"HH24:MI:SS') AT TIME ZONE 'UTC' AS timestamp,
        wind_speed - LAG(wind_speed) OVER (ORDER BY TO_TIMESTAMP(timestamp, 'YYYY-MM-DD"T"HH24:MI:SS') AT TIME ZONE 'UTC') AS wind_speed_diff
    FROM weather_obs
    WHERE TO_TIMESTAMP(timestamp, 'YYYY-MM-DD"T"HH24:MI:SS') AT TIME ZONE 'UTC' >= (NOW() - INTERVAL '7 days')
)
SELECT MAX(ABS(wind_speed_diff)) AS max_wind_speed_change
FROM wind_diffs;