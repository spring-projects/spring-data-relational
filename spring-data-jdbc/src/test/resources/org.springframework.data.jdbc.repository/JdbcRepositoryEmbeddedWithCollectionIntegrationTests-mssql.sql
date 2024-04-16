DROP TABLE IF EXISTS dummy_entity;
CREATE TABLE dummy_entity
(
    id          BIGINT IDENTITY PRIMARY KEY,
    TEST        VARCHAR(100),
    PREFIX_TEST VARCHAR(100)
);
DROP TABLE IF EXISTS dummy_entity2;
CREATE TABLE dummy_entity2
(
    dummy_id  BIGINT,
    ORDER_KEY BIGINT,
    TEST      VARCHAR(100),
    CONSTRAINT dummym_entity2_pk PRIMARY KEY (dummy_id, ORDER_KEY)
);