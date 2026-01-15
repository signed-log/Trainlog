WITH user_totals AS (
    SELECT 
        user_id,
        SUM(trip_length) AS total_km,
        COUNT(*) AS total_trips
    FROM trips
    WHERE (:tripType = 'combined' OR trip_type = :tripType)
    AND EXTRACT(YEAR FROM COALESCE(utc_start_datetime, start_datetime))::text = :year
    AND is_project = false
    AND COALESCE(utc_start_datetime, start_datetime) < NOW()
    GROUP BY user_id
),
with_percentiles AS (
    SELECT 
        user_id,
        total_km,
        total_trips,
        PERCENT_RANK() OVER (ORDER BY total_km) AS km_percentile,
        PERCENT_RANK() OVER (ORDER BY total_trips) AS trips_percentile
    FROM user_totals
)
SELECT 
    total_km,
    total_trips,
    km_percentile * 100 AS km_percentile,
    trips_percentile * 100 AS trips_percentile,
    (SELECT COUNT(DISTINCT user_id) FROM user_totals) AS total_users
FROM with_percentiles
WHERE user_id = :user_id
