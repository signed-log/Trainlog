WITH base_filter AS (
    SELECT *, COALESCE(utc_start_datetime, start_datetime) AS filtered_datetime
    FROM trips
    WHERE (:tripType = 'combined' OR trip_type = :tripType)
    AND user_id = :user_id
    AND EXTRACT(YEAR FROM COALESCE(utc_start_datetime, start_datetime))::text = :year
    AND is_project = false
    AND COALESCE(utc_start_datetime, start_datetime) < NOW()
),
travel_days AS (
    SELECT DISTINCT DATE(filtered_datetime) AS travel_date
    FROM base_filter
),
with_gaps AS (
    SELECT 
        travel_date,
        travel_date - (ROW_NUMBER() OVER (ORDER BY travel_date))::int AS grp
    FROM travel_days
),
streaks AS (
    SELECT 
        MIN(travel_date) AS streak_start,
        MAX(travel_date) AS streak_end,
        COUNT(*) AS streak_length
    FROM with_gaps
    GROUP BY grp
)
SELECT 
    streak_start,
    streak_end,
    streak_length
FROM streaks
ORDER BY streak_length DESC
LIMIT 1
