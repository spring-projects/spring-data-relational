DROP TABLE IF EXISTS dummy_entity;
DROP TABLE IF EXISTS element;
CREATE TABLE dummy_entity ( id BIGINT IDENTITY PRIMARY KEY, NAME VARCHAR(100));
CREATE TABLE element (id BIGINT IDENTITY PRIMARY KEY, content VARCHAR(100), Dummy_Entity_key BIGINT,dummy_entity BIGINT);
