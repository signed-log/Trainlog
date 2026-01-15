WITH base_filter AS (
    SELECT *, COALESCE(utc_start_datetime, start_datetime) AS filtered_datetime
    FROM trips
    WHERE (:tripType = 'combined' OR trip_type = :tripType)
    AND user_id = :user_id
    AND EXTRACT(YEAR FROM COALESCE(utc_start_datetime, start_datetime))::text = :year
    AND is_project = false
)
SELECT 
    COUNT(*) AS total_trips,
    COALESCE(SUM(trip_length), 0) AS total_km,
    COALESCE(SUM(
        CASE
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
            ELSE 0
        END
    ), 0) AS total_duration,
    COALESCE(SUM(carbon), 0) AS total_co2
FROM base_filter
WHERE filtered_datetime < NOW()
