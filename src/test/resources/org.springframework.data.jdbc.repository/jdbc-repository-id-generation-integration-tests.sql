-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE ReadOnlyIdEntity (ID BIGINT GENERATED BY DEFAULT AS IDENTITY(START WITH 1) PRIMARY KEY, NAME VARCHAR(100))

CREATE TABLE PrimitiveIdEntity (ID BIGINT GENERATED BY DEFAULT AS IDENTITY(START WITH 1) PRIMARY KEY, NAME VARCHAR(100))