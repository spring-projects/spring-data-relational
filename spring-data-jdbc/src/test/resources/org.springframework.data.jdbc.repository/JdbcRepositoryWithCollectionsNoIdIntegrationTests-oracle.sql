DROP TABLE DUMMY_ENTITY/
DROP SEQUENCE DUMMY_ENTITY_SEQ/

CREATE TABLE DUMMY_ENTITY (id NUMBER(20,0), NAME VARCHAR(100))/
CREATE SEQUENCE DUMMY_ENTITY_SEQ/

ALTER TABLE DUMMY_ENTITY
  ADD (
  CONSTRAINT DUMMY_ENTITY_PK PRIMARY KEY (id)
  )/

CREATE OR REPLACE TRIGGER DUMMY_ENTITY_ON_INSERT
BEFORE INSERT ON DUMMY_ENTITY
FOR EACH ROW
BEGIN
SELECT DUMMY_ENTITY_SEQ.nextval
       INTO :new.id
FROM dual;
END;/


DROP TABLE ELEMENT/

CREATE TABLE ELEMENT (CONTENT VARCHAR(100), DUMMY_ENTITY NUMBER(20,0))/

