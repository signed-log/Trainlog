WITH base_filter AS (
    SELECT *, COALESCE(utc_start_datetime, start_datetime) AS filtered_datetime
    FROM trips
    WHERE (:tripType = 'combined' OR trip_type = :tripType)
    AND user_id = :user_id
    AND EXTRACT(YEAR FROM COALESCE(utc_start_datetime, start_datetime))::text = :prev_year
    AND is_project = false
)
SELECT 
    COUNT(*) AS total_trips,
    COALESCE(SUM(trip_length), 0) AS total_km
FROM base_filter
WHERE filtered_datetime < NOW()
