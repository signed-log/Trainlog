WITH UTC_Filtered AS (
    SELECT *,
    CASE
        WHEN utc_start_datetime IS NOT NULL THEN utc_start_datetime
        ELSE start_datetime 
    END AS utc_filtered_start_datetime,
    CASE
        WHEN utc_end_datetime IS NOT NULL THEN utc_end_datetime
        ELSE end_datetime 
    END AS utc_filtered_end_datetime
    FROM trip
),
YearlyFiltered AS (
    SELECT *,
           strftime('%Y', utc_filtered_start_datetime) AS trip_year
    FROM UTC_Filtered
)

SELECT *,
       CASE
           WHEN  
               (
                   julianday('now') > julianday(utc_filtered_end_datetime) 
                   OR utc_filtered_start_datetime = -1
               )
               AND utc_filtered_start_datetime != 1
           THEN 1
           ELSE 0
       END AS past,
       CASE
           WHEN julianday('now') BETWEEN julianday(utc_filtered_start_datetime) AND julianday(utc_filtered_end_datetime)
           THEN 1
           ELSE 0 
       END AS current,
       CASE
           WHEN julianday('now') <= julianday(utc_filtered_start_datetime)
           THEN 1
           ELSE 0 
       END AS plannedFuture,
       CASE
           WHEN utc_filtered_start_datetime = 1
           THEN 1
           ELSE 0 
       END AS future,
       count(*) AS count
FROM YearlyFiltered 
WHERE username = :username
  AND (:lastLocal = 'all' OR julianday(last_modified) > julianday(:lastLocal))
  AND (
      :public = 0
      OR (:public = 1 AND visibility = 'public')
      OR (:friend = 1 AND visibility = 'friends')
      OR (visibility IS NULL AND type IN ('train', 'air', 'bus', 'ferry', 'aerialway', 'tram', 'metro'))
  )
GROUP BY origin_station, destination_station, trip_length, trip_year, past, current, plannedFuture, future
ORDER BY start_datetime = 1 DESC, start_datetime DESC;