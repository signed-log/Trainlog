-- list_comments.sql
SELECT id, feature_request_id, parent_id, username, content, created, modified
FROM feature_request_comments
WHERE feature_request_id = :request_id
ORDER BY created ASC;
