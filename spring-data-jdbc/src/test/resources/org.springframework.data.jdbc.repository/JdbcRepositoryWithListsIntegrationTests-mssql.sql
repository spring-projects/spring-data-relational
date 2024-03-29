DROP TABLE IF EXISTS dummy_entity;
DROP TABLE IF EXISTS element;

DROP TABLE IF EXISTS root;
DROP TABLE IF EXISTS intermediate;
DROP TABLE IF EXISTS leaf;

CREATE TABLE dummy_entity
(
    id   BIGINT IDENTITY PRIMARY KEY,
    NAME VARCHAR(100)
);
CREATE TABLE element
(
    id               BIGINT IDENTITY PRIMARY KEY,
    content          VARCHAR(100),
    Dummy_Entity_key BIGINT,
    dummy_entity     BIGINT
);

CREATE TABLE root
(
    id BIGINT IDENTITY PRIMARY KEY
);
CREATE TABLE intermediate
(
    id       BIGINT IDENTITY PRIMARY KEY,
    root     BIGINT  NOT NULL,
    root_key INTEGER NOT NULL
);
CREATE TABLE leaf
(
    name             VARCHAR(100),
    intermediate     BIGINT  NOT NULL,
    intermediate_key INTEGER NOT NULL
);
