UPDATE trips SET visibility =
    CASE trip_type
        WHEN 'accommodation' THEN 'private'
        WHEN 'aerialway' THEN 'public'
        WHEN 'bus' THEN 'public'
        WHEN 'car' THEN 'private'
        WHEN 'cycle' THEN 'private'
        WHEN 'ferry' THEN 'public'
        WHEN 'helicopter' THEN 'public'
        WHEN 'metro' THEN 'public'
        WHEN 'poi' THEN 'private'
        WHEN 'restaurant' THEN 'private'
        WHEN 'train' THEN 'public'
        WHEN 'tram' THEN 'public'
        WHEN 'walk' THEN 'public'
        ELSE 'public'
    END
WHERE visibility IS NULL;