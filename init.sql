-- init.sql
DROP TABLE IF EXISTS user_clickstream;

CREATE TABLE user_clickstream (
    event_time TIMESTAMP NOT NULL,
    event_type VARCHAR(50),
    product_id BIGINT NOT NULL,
    category_id BIGINT,
    category_code VARCHAR(255),
    brand VARCHAR(100),
    price DOUBLE PRECISION,
    user_id BIGINT NOT NULL,
    user_session VARCHAR(255),
    PRIMARY KEY (product_id, user_id, event_time, event_type)
);
