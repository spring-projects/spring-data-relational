DROP TABLE dummy_entity;
DROP TABLE element;
CREATE TABLE dummy_entity ( id SERIAL PRIMARY KEY, NAME VARCHAR(100));
CREATE TABLE element (id SERIAL PRIMARY KEY, content BIGINT, Dummy_Entity_key BIGINT,dummy_entity BIGINT);
