WITH base_filter AS (
    SELECT *, COALESCE(utc_start_datetime, start_datetime) AS filtered_datetime
    FROM trips
    WHERE (:tripType = 'combined' OR trip_type = :tripType)
    AND user_id = :user_id
    AND EXTRACT(YEAR FROM COALESCE(utc_start_datetime, start_datetime))::text = :year
    AND is_project = false
)
SELECT 
    EXTRACT(MONTH FROM filtered_datetime)::int AS month,
    COUNT(*) AS trips,
    SUM(trip_length) AS km
FROM base_filter
WHERE filtered_datetime < NOW()
GROUP BY month
ORDER BY trips DESC
LIMIT 1
