DROP TABLE IF EXISTS dummy_entity;
DROP TABLE IF EXISTS element;
CREATE TABLE dummy_entity ( id BIGINT identity PRIMARY KEY, NAME VARCHAR(100));
CREATE TABLE element (content VARCHAR(100), dummy_entity BIGINT);
