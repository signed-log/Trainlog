WITH base_filter AS (
    SELECT *, COALESCE(utc_start_datetime, start_datetime) AS filtered_datetime
    FROM trips
    WHERE (:tripType = 'combined' OR trip_type = :tripType)
    AND user_id = :user_id
    AND EXTRACT(YEAR FROM COALESCE(utc_start_datetime, start_datetime))::text = :year
    AND is_project = false
    AND COALESCE(utc_start_datetime, start_datetime) < NOW()
),
first_trip AS (
    SELECT 
        'first' AS trip_type,
        origin_station,
        destination_station,
        filtered_datetime,
        trip_length
    FROM base_filter
    ORDER BY filtered_datetime ASC
    LIMIT 1
),
last_trip AS (
    SELECT 
        'last' AS trip_type,
        origin_station,
        destination_station,
        filtered_datetime,
        trip_length
    FROM base_filter
    ORDER BY filtered_datetime DESC
    LIMIT 1
)
SELECT * FROM first_trip
UNION ALL
SELECT * FROM last_trip
