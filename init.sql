
CREATE DATABASE airflow;

\connect clickdb;

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
    PRIMARY KEY (user_session, event_time)
);

DROP TABLE IF EXISTS session_behavior_daily;

CREATE TABLE session_behavior_daily (
    user_session VARCHAR(255) NOT NULL,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    session_length_sec BIGINT,
    view BIGINT,
    cart BIGINT,
    purchase BIGINT,
    converted INT,
    interest_category VARCHAR(255),
    date DATE,
    created_at TIMESTAMP,
    PRIMARY KEY (user_session, date)
);
