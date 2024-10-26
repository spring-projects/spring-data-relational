DROP TABLE dummy_entity;
DROP TABLE ROOT;
DROP TABLE INTERMEDIATE;
DROP TABLE LEAF;
DROP TABLE WITH_DELIMITED_COLUMN;
DROP TABLE ENTITY_WITH_SEQUENCE;
DROP SEQUENCE ENTITY_SEQUENCE;

CREATE TABLE dummy_entity
(
    id_Prop          SERIAL PRIMARY KEY,
    NAME             VARCHAR(100),
    POINT_IN_TIME    TIMESTAMP,
    OFFSET_DATE_TIME TIMESTAMP WITH TIME ZONE,
    FLAG             BOOLEAN,
    REF              BIGINT,
    DIRECTION        VARCHAR(100),
    BYTES            BYTEA
);

CREATE TABLE ROOT
(
    ID   SERIAL PRIMARY KEY,
    NAME VARCHAR(100)
);

CREATE TABLE INTERMEDIATE
(
    ID         SERIAL PRIMARY KEY,
    NAME       VARCHAR(100),
    ROOT       BIGINT,
    "ROOT_ID"  BIGINT,
    "ROOT_KEY" INTEGER
);

CREATE TABLE LEAF
(
    ID                 SERIAL PRIMARY KEY,
    NAME               VARCHAR(100),
    INTERMEDIATE       BIGINT,
    "INTERMEDIATE_ID"  BIGINT,
    "INTERMEDIATE_KEY" INTEGER
);

CREATE TABLE "WITH_DELIMITED_COLUMN"
(
    ID                      SERIAL PRIMARY KEY,
    "ORG.XTUNIT.IDENTIFIER" VARCHAR(100),
    "STYPE"                 VARCHAR(100)
);

CREATE TABLE ENTITY_WITH_SEQUENCE
(
    ID BIGINT,
    NAME VARCHAR(100)
);

CREATE SEQUENCE ENTITY_SEQUENCE START WITH 1 INCREMENT BY 1 NO MAXVALUE;