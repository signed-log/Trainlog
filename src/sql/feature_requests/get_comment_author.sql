SELECT username, feature_request_id, parent_id
FROM feature_request_comments
WHERE id = :comment_id;
