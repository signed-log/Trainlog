INSERT INTO feature_request_comments (feature_request_id, parent_id, username, content, created, modified)
VALUES (:request_id, :parent_id, :username, :content, now(), now())
RETURNING id;
