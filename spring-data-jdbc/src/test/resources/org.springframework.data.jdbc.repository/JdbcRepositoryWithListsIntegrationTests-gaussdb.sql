DROP TABLE element;
DROP TABLE dummy_entity;

DROP TABLE root;
DROP TABLE intermediate;
DROP TABLE leaf;

CREATE TABLE dummy_entity
(
    id   SERIAL PRIMARY KEY,
    NAME VARCHAR(100)
);
CREATE TABLE element
(
    id               SERIAL PRIMARY KEY,
    content          VARCHAR(100),
    dummy_entity_key BIGINT,
    dummy_entity     BIGINT
);

CREATE TABLE root
(
    id SERIAL PRIMARY KEY
);
CREATE TABLE intermediate
(
    id       SERIAL PRIMARY KEY,
    root     BIGINT  NOT NULL,
    root_key INTEGER NOT NULL
);
CREATE TABLE leaf
(
    name             VARCHAR(100),
    intermediate     BIGINT  NOT NULL,
    intermediate_key INTEGER NOT NULL
);