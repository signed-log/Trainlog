WITH base_filter AS (
    SELECT *, COALESCE(utc_start_datetime, start_datetime) AS filtered_datetime
    FROM trips
    WHERE (:tripType = 'combined' OR trip_type = :tripType)
    AND user_id = :user_id
    AND EXTRACT(YEAR FROM COALESCE(utc_start_datetime, start_datetime))::text = :year
    AND is_project = false
),
with_speed AS (
    SELECT 
        origin_station,
        destination_station,
        trip_length,
        COALESCE(
            EXTRACT(EPOCH FROM (utc_end_datetime - utc_start_datetime)),
            manual_trip_duration,
            estimated_trip_duration,
            0
        ) AS trip_duration,
        CASE 
            WHEN COALESCE(
                EXTRACT(EPOCH FROM (utc_end_datetime - utc_start_datetime)),
                manual_trip_duration,
                estimated_trip_duration,
                0
            ) > 0 
            THEN (trip_length / 1000.0) / (COALESCE(
                EXTRACT(EPOCH FROM (utc_end_datetime - utc_start_datetime)),
                manual_trip_duration,
                estimated_trip_duration,
                0
            ) / 3600.0)
            ELSE 0
        END AS avg_speed_kmh
    FROM base_filter
    WHERE filtered_datetime < NOW()
    AND trip_length > 0
)
SELECT 
    origin_station,
    destination_station,
    trip_length,
    trip_duration,
    avg_speed_kmh
FROM with_speed
WHERE avg_speed_kmh > 0
ORDER BY avg_speed_kmh DESC
LIMIT 1