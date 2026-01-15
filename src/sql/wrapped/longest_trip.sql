WITH base_filter AS (
    SELECT *, COALESCE(utc_start_datetime, start_datetime) AS filtered_datetime
    FROM trips
    WHERE (:tripType = 'combined' OR trip_type = :tripType)
    AND trip_type not in ('accommodation', 'poi', 'restaurant')
    AND user_id = :user_id
    AND EXTRACT(YEAR FROM COALESCE(utc_start_datetime, start_datetime))::text = :year
    AND is_project = false
)
SELECT 
    origin_station,
    destination_station,
    trip_length,
    COALESCE(
        EXTRACT(EPOCH FROM (utc_end_datetime - utc_start_datetime)),
        manual_trip_duration,
        estimated_trip_duration,
        0
    ) AS trip_duration
FROM base_filter
WHERE filtered_datetime < NOW()
ORDER BY trip_length DESC
LIMIT 1
