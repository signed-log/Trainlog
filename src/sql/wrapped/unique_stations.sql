WITH base_filter AS (
    SELECT *, COALESCE(utc_start_datetime, start_datetime) AS filtered_datetime
    FROM trips
    WHERE (:tripType = 'combined' OR trip_type = :tripType)
    AND user_id = :user_id
    AND EXTRACT(YEAR FROM COALESCE(utc_start_datetime, start_datetime))::text = :year
    AND is_project = false
    AND COALESCE(utc_start_datetime, start_datetime) < NOW()
),
all_stations AS (
    SELECT origin_station AS station FROM base_filter WHERE origin_station IS NOT NULL
    UNION
    SELECT destination_station AS station FROM base_filter WHERE destination_station IS NOT NULL
)
SELECT COUNT(DISTINCT station) AS unique_stations
FROM all_stations
