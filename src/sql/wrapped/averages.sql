WITH base_filter AS (
    SELECT *, COALESCE(utc_start_datetime, start_datetime) AS filtered_datetime
    FROM trips
    WHERE (:tripType = 'combined' OR trip_type = :tripType)
    AND trip_type not in ('accommodation', 'poi', 'restaurant')
    AND user_id = :user_id
    AND EXTRACT(YEAR FROM COALESCE(utc_start_datetime, start_datetime))::text = :year
    AND is_project = false
    AND COALESCE(utc_start_datetime, start_datetime) < NOW()
)
SELECT 
    AVG(trip_length) AS avg_trip_length,
    AVG(CASE 
        WHEN COALESCE(
            EXTRACT(EPOCH FROM (utc_end_datetime - utc_start_datetime)),
            manual_trip_duration,
            estimated_trip_duration,
            0
        ) BETWEEN 0 AND (10 * 24 * 60 * 60)
        THEN COALESCE(
            EXTRACT(EPOCH FROM (utc_end_datetime - utc_start_datetime)),
            manual_trip_duration,
            estimated_trip_duration,
            0
        )
        ELSE NULL
    END) AS avg_trip_duration,
    COUNT(DISTINCT DATE(filtered_datetime)) AS days_traveled
FROM base_filter
