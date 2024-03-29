CREATE TABLE dummy_entity
(
    id   BIGINT AUTO_INCREMENT PRIMARY KEY,
    NAME VARCHAR(100)
);
CREATE TABLE element
(
    id               BIGINT AUTO_INCREMENT PRIMARY KEY,
    content          VARCHAR(100),
    Dummy_Entity_key BIGINT,
    dummy_entity     BIGINT
);

CREATE TABLE root
(
    id BIGINT AUTO_INCREMENT PRIMARY KEY
);
CREATE TABLE intermediate
(
    id       BIGINT AUTO_INCREMENT PRIMARY KEY,
    root     BIGINT  NOT NULL,
    root_key INTEGER NOT NULL
);
CREATE TABLE leaf
(
    name             VARCHAR(100),
    intermediate     BIGINT  NOT NULL,
    intermediate_key INTEGER NOT NULL
);
