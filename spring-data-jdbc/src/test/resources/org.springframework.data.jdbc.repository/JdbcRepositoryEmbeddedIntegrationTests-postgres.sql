DROP TABLE dummy_entity;
DROP TABLE "SORT_EMBEDDED_ENTITY";
DROP TABLE WITH_DOT_COLUMN;

CREATE TABLE dummy_entity
(
    id                  SERIAL PRIMARY KEY,
    TEST                VARCHAR(100),
    PREFIX2_ATTR        BIGINT,
    PREFIX_TEST         VARCHAR(100),
    PREFIX_PREFIX2_ATTR BIGINT
);

CREATE TABLE "SORT_EMBEDDED_ENTITY"
(
    id           SERIAL PRIMARY KEY,
    first_name   VARCHAR(100),
    address      VARCHAR(255),
    email        VARCHAR(255),
    phone_number VARCHAR(255)
);

CREATE TABLE WITH_DOT_COLUMN
(
    id             SERIAL PRIMARY KEY,
    "address.city" VARCHAR(255)
);