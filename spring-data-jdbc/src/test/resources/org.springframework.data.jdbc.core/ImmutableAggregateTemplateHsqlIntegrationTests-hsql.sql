CREATE TABLE LEGO_SET
(
    id   BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1) PRIMARY KEY,
    NAME VARCHAR(30)
);

CREATE TABLE MANUAL
(
    id       BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1) PRIMARY KEY,
    LEGO_SET BIGINT,
    CONTENT  VARCHAR(2000)
);
ALTER TABLE MANUAL
    ADD FOREIGN KEY (LEGO_SET)
        REFERENCES LEGO_SET (id);

CREATE TABLE AUTHOR
(
    id       BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1) PRIMARY KEY,
    LEGO_SET BIGINT,
    NAME     VARCHAR(2000)
);
ALTER TABLE AUTHOR
    ADD FOREIGN KEY (LEGO_SET)
        REFERENCES LEGO_SET (id);

CREATE TABLE WITH_COPY_CONSTRUCTOR
(
    ID   BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1) PRIMARY KEY,
    NAME VARCHAR(30)
);

CREATE TABLE ROOT
(
    ID   BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1) PRIMARY KEY,
    NAME VARCHAR(30)
);
CREATE TABLE NON_ROOT
(
    ID   BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1) PRIMARY KEY,
    ROOT BIGINT NOT NULL,
    NAME VARCHAR(30)
);
ALTER TABLE NON_ROOT
    ADD FOREIGN KEY (ROOT) REFERENCES ROOT (ID);