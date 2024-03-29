CREATE TABLE dummy_entity
(
    id   BIGINT GENERATED BY DEFAULT AS IDENTITY ( START WITH 1 ) PRIMARY KEY,
    NAME VARCHAR(100)
);
CREATE TABLE element
(
    id               BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1) PRIMARY KEY,
    content          VARCHAR(100),
    Dummy_Entity_key BIGINT,
    dummy_entity     BIGINT
);

CREATE TABLE root
(
    id BIGINT GENERATED BY DEFAULT AS IDENTITY ( START WITH 1 ) PRIMARY KEY
);
CREATE TABLE intermediate
(
    id       BIGINT GENERATED BY DEFAULT AS IDENTITY ( START WITH 1 ) PRIMARY KEY,
    root     BIGINT  NOT NULL,
    root_key INTEGER NOT NULL
);
CREATE TABLE leaf
(
    name             VARCHAR(100),
    intermediate     BIGINT  NOT NULL,
    intermediate_key INTEGER NOT NULL
);
