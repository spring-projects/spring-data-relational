DROP TABLE element;
DROP TABLE dummyentity;
CREATE TABLE dummyentity ( id SERIAL PRIMARY KEY, NAME VARCHAR(100));
CREATE TABLE element (id SERIAL PRIMARY KEY, content VARCHAR(100),dummyentity_key VARCHAR(100), dummyentity BIGINT);
