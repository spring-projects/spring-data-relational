SET
    SQL_MODE = 'ALLOW_INVALID_DATES';

CREATE TABLE DUMMY_ENTITY
(
    ID_PROP          BIGINT AUTO_INCREMENT PRIMARY KEY,
    NAME             VARCHAR(100),
    POINT_IN_TIME    TIMESTAMP(3) DEFAULT NULL,
    OFFSET_DATE_TIME TIMESTAMP(3) DEFAULT NULL,
    FLAG             BIT(1),
    REF              BIGINT,
    DIRECTION        VARCHAR(100),
    BYTES            BINARY(8)
);

CREATE TABLE ROOT
(
    ID   BIGINT AUTO_INCREMENT PRIMARY KEY,
    NAME VARCHAR(100)
);

CREATE TABLE INTERMEDIATE
(
    ID       BIGINT AUTO_INCREMENT PRIMARY KEY,
    NAME     VARCHAR(100),
    ROOT     BIGINT,
    ROOT_ID  BIGINT,
    ROOT_KEY INTEGER
);

CREATE TABLE LEAF
(
    ID               BIGINT AUTO_INCREMENT PRIMARY KEY,
    NAME             VARCHAR(100),
    INTERMEDIATE     BIGINT,
    INTERMEDIATE_ID  BIGINT,
    INTERMEDIATE_KEY INTEGER
);

CREATE TABLE WITH_DELIMITED_COLUMN
(
    ID                      BIGINT AUTO_INCREMENT PRIMARY KEY,
    `ORG.XTUNIT.IDENTIFIER` VARCHAR(100),
    STYPE                   VARCHAR(100)
);

CREATE TABLE PROVIDED_ID_ENTITY
(
    ID   BIGINT PRIMARY KEY,
    NAME VARCHAR(30)
);
