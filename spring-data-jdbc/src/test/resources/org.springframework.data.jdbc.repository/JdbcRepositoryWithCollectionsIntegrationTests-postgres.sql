DROP TABLE element;
DROP TABLE dummy_entity;
DROP TABLE collection_element;
DROP TABLE collection_aggregate;
CREATE TABLE dummy_entity ( id SERIAL PRIMARY KEY, NAME VARCHAR(100));
CREATE TABLE element (id SERIAL PRIMARY KEY, content VARCHAR(100), dummy_entity BIGINT);
CREATE TABLE collection_aggregate ( id SERIAL PRIMARY KEY, NAME VARCHAR(100));
CREATE TABLE collection_element (id SERIAL PRIMARY KEY, content VARCHAR(100), collection_aggregate BIGINT);
