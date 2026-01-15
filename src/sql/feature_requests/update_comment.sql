UPDATE feature_request_comments
SET content = :content, modified = now()
WHERE id = :comment_id;
