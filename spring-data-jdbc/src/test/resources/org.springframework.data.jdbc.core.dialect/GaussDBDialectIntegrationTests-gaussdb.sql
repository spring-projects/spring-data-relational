DROP TABLE customers;

CREATE TABLE customers (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    person_data JSONB,
    session_data JSONB
);
