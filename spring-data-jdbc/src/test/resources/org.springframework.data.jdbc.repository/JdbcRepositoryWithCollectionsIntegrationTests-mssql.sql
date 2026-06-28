DROP TABLE IF EXISTS dummy_entity;
DROP TABLE IF EXISTS element;
DROP TABLE IF EXISTS collection_aggregate;
DROP TABLE IF EXISTS collection_element;
CREATE TABLE dummy_entity ( id BIGINT identity PRIMARY KEY, NAME VARCHAR(100));
CREATE TABLE element (id BIGINT identity PRIMARY KEY, content VARCHAR(100), dummy_entity BIGINT);
CREATE TABLE collection_aggregate ( id BIGINT identity PRIMARY KEY, NAME VARCHAR(100));
CREATE TABLE collection_element (id BIGINT identity PRIMARY KEY, content VARCHAR(100), collection_aggregate BIGINT);
