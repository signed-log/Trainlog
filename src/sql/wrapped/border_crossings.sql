WITH base_filter AS (
    SELECT *, COALESCE(utc_start_datetime, start_datetime) AS filtered_datetime
    FROM trips
    WHERE (:tripType = 'combined' OR trip_type = :tripType)
    AND user_id = :user_id
    AND EXTRACT(YEAR FROM COALESCE(utc_start_datetime, start_datetime))::text = :year
    AND is_project = false
    AND COALESCE(utc_start_datetime, start_datetime) < NOW()
    AND countries IS NOT NULL
),
trip_crossings AS (
    SELECT 
        trip_id,
        -- Count number of unique countries in the JSON object
        jsonb_object_keys(countries::jsonb) AS country_code
    FROM base_filter
),
trips_with_counts AS (
    SELECT 
        trip_id,
        COUNT(DISTINCT country_code) AS countries_in_trip
    FROM trip_crossings
    GROUP BY trip_id
)
SELECT 
    SUM(GREATEST(countries_in_trip - 1, 0))::int AS total_border_crossings,
    COUNT(DISTINCT trip_id) AS trips_with_crossings
FROM trips_with_counts
WHERE countries_in_trip > 1
