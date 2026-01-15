-- Feature Request Comments table
CREATE TABLE feature_request_comments (
    id SERIAL PRIMARY KEY,
    feature_request_id INTEGER NOT NULL,
    parent_id INTEGER,
    username TEXT NOT NULL,
    content TEXT NOT NULL,
    created TIMESTAMP DEFAULT now(),
    modified TIMESTAMP DEFAULT now(),
    FOREIGN KEY (feature_request_id) REFERENCES feature_requests(id) ON DELETE CASCADE,
    FOREIGN KEY (parent_id) REFERENCES feature_request_comments(id) ON DELETE CASCADE
);

CREATE INDEX idx_comments_feature_request ON feature_request_comments(feature_request_id);
CREATE INDEX idx_comments_parent ON feature_request_comments(parent_id);
