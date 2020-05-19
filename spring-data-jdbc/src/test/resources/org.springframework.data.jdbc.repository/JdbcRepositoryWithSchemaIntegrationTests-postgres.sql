CREATE SCHEMA IF NOT EXISTS test;
DROP TABLE test.element;
DROP TABLE test.dummy_entity;
CREATE TABLE test.dummy_entity ( id SERIAL PRIMARY KEY, NAME VARCHAR(100));
CREATE TABLE test.element (id SERIAL PRIMARY KEY, content VARCHAR(100),dummy_entity_key BIGINT, dummy_entity BIGINT);
