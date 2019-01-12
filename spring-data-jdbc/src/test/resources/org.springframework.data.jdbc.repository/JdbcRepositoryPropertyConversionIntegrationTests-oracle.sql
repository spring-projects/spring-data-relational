CREATE TABLE ENTITY (
  ID_TIMESTAMP TIMESTAMP,
  BOOL CHAR(1),
  SOME_ENUM VARCHAR(100),
  BIG_DECIMAL DECIMAL(38),
  BIG_INTEGER NUMBER(20,0),
  DATE TIMESTAMP,
  LOCAL_DATE_TIME TIMESTAMP,
  ZONED_DATE_TIME VARCHAR(30))/


ALTER TABLE ENTITY
  ADD (
  CONSTRAINT ENTITY_PK PRIMARY KEY (ID_TIMESTAMP)
  )/
