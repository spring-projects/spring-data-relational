DROP TABLE IF EXISTS LEGO_SET;
DROP TABLE IF EXISTS MANUAL;
CREATE TABLE LEGO_SET ( id BIGINT IDENTITY PRIMARY KEY, NAME VARCHAR(30));
CREATE TABLE MANUAL ( id BIGINT IDENTITY PRIMARY KEY, LEGO_SET BIGINT, CONTENT VARCHAR(2000));
ALTER TABLE MANUAL ADD FOREIGN KEY (LEGO_SET) REFERENCES LEGO_SET(id);
