WITH base_filter AS (
    SELECT 
        trip_id,
        start_datetime AS local_start,
        COALESCE(
            end_datetime,
            start_datetime + INTERVAL '1 second' * COALESCE(
                EXTRACT(EPOCH FROM (utc_end_datetime - utc_start_datetime)),
                manual_trip_duration,
                estimated_trip_duration,
                3600
            )
        ) AS local_end
    FROM trips
    WHERE (:tripType = 'combined' OR trip_type = :tripType)
    AND user_id = :user_id
    AND EXTRACT(YEAR FROM COALESCE(utc_start_datetime, start_datetime))::text = :year
    AND is_project = false
    AND COALESCE(utc_start_datetime, start_datetime) < NOW()
    AND start_datetime IS NOT NULL
),
-- Generate hour slots for each trip
trip_hours AS (
    SELECT 
        trip_id,
        generate_series(
            date_trunc('hour', local_start),
            date_trunc('hour', local_end),
            INTERVAL '1 hour'
        ) AS hour_slot
    FROM base_filter
),
-- Count hours per trip per category
trip_category_hours AS (
    SELECT 
        trip_id,
        CASE 
            WHEN EXTRACT(HOUR FROM hour_slot) BETWEEN 5 AND 11 THEN 'morning'
            WHEN EXTRACT(HOUR FROM hour_slot) BETWEEN 12 AND 17 THEN 'afternoon'
            WHEN EXTRACT(HOUR FROM hour_slot) BETWEEN 18 AND 21 THEN 'evening'
            ELSE 'night'
        END AS time_category,
        COUNT(*) AS hours_in_category
    FROM trip_hours
    GROUP BY trip_id, time_category
),
-- Get total hours per trip
trip_total_hours AS (
    SELECT trip_id, SUM(hours_in_category) AS total_hours
    FROM trip_category_hours
    GROUP BY trip_id
),
-- Calculate weighted trip contribution per category
weighted_trips AS (
    SELECT 
        tch.time_category,
        SUM(tch.hours_in_category::float / GREATEST(tth.total_hours, 1)) AS weighted_trips
    FROM trip_category_hours tch
    JOIN trip_total_hours tth ON tch.trip_id = tth.trip_id
    GROUP BY tch.time_category
)
SELECT 
    time_category,
    ROUND(weighted_trips)::int AS trips
FROM weighted_trips
ORDER BY weighted_trips DESC