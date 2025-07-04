DROP TABLE MANUAL;
DROP TABLE LEGO_SET;
DROP TABLE ONE_TO_ONE_PARENT;
DROP TABLE Child_No_Id;
DROP TABLE element_no_id;
DROP TABLE "LIST_PARENT";
DROP TABLE SIMPLE_LIST_PARENT;
DROP TABLE "ARRAY_OWNER";
DROP TABLE DOUBLE_LIST_OWNER;
DROP TABLE FLOAT_LIST_OWNER;
DROP TABLE BYTE_ARRAY_OWNER;
DROP TABLE CHAIN0;
DROP TABLE CHAIN1;
DROP TABLE CHAIN2;
DROP TABLE CHAIN3;
DROP TABLE CHAIN4;
DROP TABLE NO_ID_CHAIN0;
DROP TABLE NO_ID_CHAIN1;
DROP TABLE NO_ID_CHAIN2;
DROP TABLE NO_ID_CHAIN3;
DROP TABLE NO_ID_CHAIN4;
DROP TABLE NO_ID_LIST_CHAIN0;
DROP TABLE NO_ID_LIST_CHAIN1;
DROP TABLE NO_ID_LIST_CHAIN2;
DROP TABLE NO_ID_LIST_CHAIN3;
DROP TABLE NO_ID_LIST_CHAIN4;
DROP TABLE NO_ID_MAP_CHAIN0;
DROP TABLE NO_ID_MAP_CHAIN1;
DROP TABLE NO_ID_MAP_CHAIN2;
DROP TABLE NO_ID_MAP_CHAIN3;
DROP TABLE NO_ID_MAP_CHAIN4;
DROP TABLE "VERSIONED_AGGREGATE";
DROP TABLE WITH_READ_ONLY;
DROP TABLE WITH_LOCAL_DATE_TIME;
DROP TABLE WITH_ID_ONLY;
DROP TABLE WITH_INSERT_ONLY;

DROP TABLE MULTIPLE_COLLECTIONS;
DROP TABLE MAP_ELEMENT;
DROP TABLE LIST_ELEMENT;
DROP TABLE SET_ELEMENT;

DROP TABLE BOOK;
DROP TABLE AUTHOR;

DROP TABLE ENUM_MAP_OWNER;

DROP TABLE REFERENCED;
DROP TABLE WITH_ONE_TO_ONE;

DROP TABLE THIRD;
DROP TABLE SEC;
DROP TABLE FIRST;

DROP TABLE "BEFORE_CONVERT_CALLBACK_FOR_SAVE_BATCH";

CREATE TABLE LEGO_SET
(
    "id1" SERIAL PRIMARY KEY,
    NAME  VARCHAR(30)
);
CREATE TABLE MANUAL
(
    "id2"       SERIAL PRIMARY KEY,
    LEGO_SET    BIGINT,
    ALTERNATIVE BIGINT,
    CONTENT     VARCHAR(2000)
);

ALTER TABLE MANUAL
    ADD FOREIGN KEY (LEGO_SET)
        REFERENCES LEGO_SET ("id1");

CREATE TABLE ONE_TO_ONE_PARENT
(
    "id3"   SERIAL PRIMARY KEY,
    content VARCHAR(30)
);
CREATE TABLE Child_No_Id
(
    ONE_TO_ONE_PARENT INTEGER PRIMARY KEY,
    content           VARCHAR(30)
);

CREATE TABLE "LIST_PARENT"
(
    "id4" SERIAL PRIMARY KEY,
    NAME  VARCHAR(100)
);

CREATE TABLE SIMPLE_LIST_PARENT
(
    id SERIAL PRIMARY KEY,
    NAME  VARCHAR(100)
);

CREATE TABLE element_no_id
(
    content         VARCHAR(100),
    LIST_PARENT_key BIGINT,
    SIMPLE_LIST_PARENT_key BIGINT,
    "LIST_PARENT"     INTEGER,
    SIMPLE_LIST_PARENT  INTEGER
);

CREATE TABLE "ARRAY_OWNER"
(
    ID               SERIAL PRIMARY KEY,
    DIGITS           VARCHAR(20)[10],
    MULTIDIMENSIONAL VARCHAR(20)[10][10]
);

CREATE TABLE DOUBLE_LIST_OWNER
(
    ID               SERIAL PRIMARY KEY,
    DIGITS           DOUBLE PRECISION[10]
);

CREATE TABLE FLOAT_LIST_OWNER
(
    ID               SERIAL PRIMARY KEY,
    DIGITS           REAL[10]
);

CREATE TABLE BYTE_ARRAY_OWNER
(
    ID          SERIAL PRIMARY KEY,
    BINARY_DATA BYTEA NOT NULL
);

CREATE TABLE CHAIN4
(
    FOUR       SERIAL PRIMARY KEY,
    FOUR_VALUE VARCHAR(20)
);

CREATE TABLE CHAIN3
(
    THREE       SERIAL PRIMARY KEY,
    THREE_VALUE VARCHAR(20),
    CHAIN4      BIGINT,
    FOREIGN KEY (CHAIN4) REFERENCES CHAIN4 (FOUR)
);

CREATE TABLE CHAIN2
(
    TWO       SERIAL PRIMARY KEY,
    TWO_VALUE VARCHAR(20),
    CHAIN3    BIGINT,
    FOREIGN KEY (CHAIN3) REFERENCES CHAIN3 (THREE)
);

CREATE TABLE CHAIN1
(
    ONE       SERIAL PRIMARY KEY,
    ONE_VALUE VARCHAR(20),
    CHAIN2    BIGINT,
    FOREIGN KEY (CHAIN2) REFERENCES CHAIN2 (TWO)
);

CREATE TABLE CHAIN0
(
    ZERO       SERIAL PRIMARY KEY,
    ZERO_VALUE VARCHAR(20),
    CHAIN1     BIGINT,
    FOREIGN KEY (CHAIN1) REFERENCES CHAIN1 (ONE)
);

CREATE TABLE NO_ID_CHAIN4
(
    FOUR       SERIAL PRIMARY KEY,
    FOUR_VALUE VARCHAR(20)
);

CREATE TABLE NO_ID_CHAIN3
(
    THREE_VALUE  VARCHAR(20),
    NO_ID_CHAIN4 BIGINT,
    FOREIGN KEY (NO_ID_CHAIN4) REFERENCES NO_ID_CHAIN4 (FOUR)
);

CREATE TABLE NO_ID_CHAIN2
(
    TWO_VALUE    VARCHAR(20),
    NO_ID_CHAIN4 BIGINT,
    FOREIGN KEY (NO_ID_CHAIN4) REFERENCES NO_ID_CHAIN4 (FOUR)
);

CREATE TABLE NO_ID_CHAIN1
(
    ONE_VALUE    VARCHAR(20),
    NO_ID_CHAIN4 BIGINT,
    FOREIGN KEY (NO_ID_CHAIN4) REFERENCES NO_ID_CHAIN4 (FOUR)
);

CREATE TABLE NO_ID_CHAIN0
(
    ZERO_VALUE   VARCHAR(20),
    NO_ID_CHAIN4 BIGINT,
    FOREIGN KEY (NO_ID_CHAIN4) REFERENCES NO_ID_CHAIN4 (FOUR)
);


CREATE TABLE NO_ID_LIST_CHAIN4
(
    FOUR       SERIAL PRIMARY KEY,
    FOUR_VALUE VARCHAR(20)
);

CREATE TABLE NO_ID_LIST_CHAIN3
(
    THREE_VALUE           VARCHAR(20),
    NO_ID_LIST_CHAIN4     BIGINT,
    NO_ID_LIST_CHAIN4_KEY BIGINT,
    PRIMARY KEY (NO_ID_LIST_CHAIN4,
                 NO_ID_LIST_CHAIN4_KEY),
    FOREIGN KEY (NO_ID_LIST_CHAIN4) REFERENCES NO_ID_LIST_CHAIN4 (FOUR)
);

CREATE TABLE NO_ID_LIST_CHAIN2
(
    TWO_VALUE             VARCHAR(20),
    NO_ID_LIST_CHAIN4     BIGINT,
    NO_ID_LIST_CHAIN4_KEY BIGINT,
    NO_ID_LIST_CHAIN3_KEY BIGINT,
    PRIMARY KEY (NO_ID_LIST_CHAIN4,
                 NO_ID_LIST_CHAIN4_KEY,
                 NO_ID_LIST_CHAIN3_KEY),
    FOREIGN KEY (
                 NO_ID_LIST_CHAIN4,
                 NO_ID_LIST_CHAIN4_KEY
        ) REFERENCES NO_ID_LIST_CHAIN3 (
                                        NO_ID_LIST_CHAIN4,
                                        NO_ID_LIST_CHAIN4_KEY
        )
);

CREATE TABLE NO_ID_LIST_CHAIN1
(
    ONE_VALUE             VARCHAR(20),
    NO_ID_LIST_CHAIN4     BIGINT,
    NO_ID_LIST_CHAIN4_KEY BIGINT,
    NO_ID_LIST_CHAIN3_KEY BIGINT,
    NO_ID_LIST_CHAIN2_KEY BIGINT,
    PRIMARY KEY (NO_ID_LIST_CHAIN4,
                 NO_ID_LIST_CHAIN4_KEY,
                 NO_ID_LIST_CHAIN3_KEY,
                 NO_ID_LIST_CHAIN2_KEY),
    FOREIGN KEY (
                 NO_ID_LIST_CHAIN4,
                 NO_ID_LIST_CHAIN4_KEY,
                 NO_ID_LIST_CHAIN3_KEY
        ) REFERENCES NO_ID_LIST_CHAIN2 (
                                        NO_ID_LIST_CHAIN4,
                                        NO_ID_LIST_CHAIN4_KEY,
                                        NO_ID_LIST_CHAIN3_KEY
        )
);

CREATE TABLE NO_ID_LIST_CHAIN0
(
    ZERO_VALUE            VARCHAR(20),
    NO_ID_LIST_CHAIN4     BIGINT,
    NO_ID_LIST_CHAIN4_KEY BIGINT,
    NO_ID_LIST_CHAIN3_KEY BIGINT,
    NO_ID_LIST_CHAIN2_KEY BIGINT,
    NO_ID_LIST_CHAIN1_KEY BIGINT,
    PRIMARY KEY (NO_ID_LIST_CHAIN4,
                 NO_ID_LIST_CHAIN4_KEY,
                 NO_ID_LIST_CHAIN3_KEY,
                 NO_ID_LIST_CHAIN2_KEY,
                 NO_ID_LIST_CHAIN1_KEY),
    FOREIGN KEY (
                 NO_ID_LIST_CHAIN4,
                 NO_ID_LIST_CHAIN4_KEY,
                 NO_ID_LIST_CHAIN3_KEY,
                 NO_ID_LIST_CHAIN2_KEY
        ) REFERENCES NO_ID_LIST_CHAIN1 (
                                        NO_ID_LIST_CHAIN4,
                                        NO_ID_LIST_CHAIN4_KEY,
                                        NO_ID_LIST_CHAIN3_KEY,
                                        NO_ID_LIST_CHAIN2_KEY
        )
);



CREATE TABLE NO_ID_MAP_CHAIN4
(
    FOUR       SERIAL PRIMARY KEY,
    FOUR_VALUE VARCHAR(20)
);

CREATE TABLE NO_ID_MAP_CHAIN3
(
    THREE_VALUE          VARCHAR(20),
    NO_ID_MAP_CHAIN4     BIGINT,
    NO_ID_MAP_CHAIN4_KEY VARCHAR(20),
    PRIMARY KEY (NO_ID_MAP_CHAIN4,
                 NO_ID_MAP_CHAIN4_KEY),
    FOREIGN KEY (NO_ID_MAP_CHAIN4) REFERENCES NO_ID_MAP_CHAIN4 (FOUR)
);

CREATE TABLE NO_ID_MAP_CHAIN2
(
    TWO_VALUE            VARCHAR(20),
    NO_ID_MAP_CHAIN4     BIGINT,
    NO_ID_MAP_CHAIN4_KEY VARCHAR(20),
    NO_ID_MAP_CHAIN3_KEY VARCHAR(20),
    PRIMARY KEY (NO_ID_MAP_CHAIN4,
                 NO_ID_MAP_CHAIN4_KEY,
                 NO_ID_MAP_CHAIN3_KEY),
    FOREIGN KEY (
                 NO_ID_MAP_CHAIN4,
                 NO_ID_MAP_CHAIN4_KEY
        ) REFERENCES NO_ID_MAP_CHAIN3 (
                                       NO_ID_MAP_CHAIN4,
                                       NO_ID_MAP_CHAIN4_KEY
        )
);

CREATE TABLE NO_ID_MAP_CHAIN1
(
    ONE_VALUE            VARCHAR(20),
    NO_ID_MAP_CHAIN4     BIGINT,
    NO_ID_MAP_CHAIN4_KEY VARCHAR(20),
    NO_ID_MAP_CHAIN3_KEY VARCHAR(20),
    NO_ID_MAP_CHAIN2_KEY VARCHAR(20),
    PRIMARY KEY (NO_ID_MAP_CHAIN4,
                 NO_ID_MAP_CHAIN4_KEY,
                 NO_ID_MAP_CHAIN3_KEY,
                 NO_ID_MAP_CHAIN2_KEY),
    FOREIGN KEY (
                 NO_ID_MAP_CHAIN4,
                 NO_ID_MAP_CHAIN4_KEY,
                 NO_ID_MAP_CHAIN3_KEY
        ) REFERENCES NO_ID_MAP_CHAIN2 (
                                       NO_ID_MAP_CHAIN4,
                                       NO_ID_MAP_CHAIN4_KEY,
                                       NO_ID_MAP_CHAIN3_KEY
        )
);

CREATE TABLE NO_ID_MAP_CHAIN0
(
    ZERO_VALUE           VARCHAR(20),
    NO_ID_MAP_CHAIN4     BIGINT,
    NO_ID_MAP_CHAIN4_KEY VARCHAR(20),
    NO_ID_MAP_CHAIN3_KEY VARCHAR(20),
    NO_ID_MAP_CHAIN2_KEY VARCHAR(20),
    NO_ID_MAP_CHAIN1_KEY VARCHAR(20),
    PRIMARY KEY (NO_ID_MAP_CHAIN4,
                 NO_ID_MAP_CHAIN4_KEY,
                 NO_ID_MAP_CHAIN3_KEY,
                 NO_ID_MAP_CHAIN2_KEY,
                 NO_ID_MAP_CHAIN1_KEY),
    FOREIGN KEY (
                 NO_ID_MAP_CHAIN4,
                 NO_ID_MAP_CHAIN4_KEY,
                 NO_ID_MAP_CHAIN3_KEY,
                 NO_ID_MAP_CHAIN2_KEY
        ) REFERENCES NO_ID_MAP_CHAIN1 (
                                       NO_ID_MAP_CHAIN4,
                                       NO_ID_MAP_CHAIN4_KEY,
                                       NO_ID_MAP_CHAIN3_KEY,
                                       NO_ID_MAP_CHAIN2_KEY
        )
);

CREATE TABLE "VERSIONED_AGGREGATE"
(
    ID      SERIAL PRIMARY KEY,
    VERSION BIGINT
);

CREATE TABLE WITH_READ_ONLY
(
    ID        SERIAL PRIMARY KEY,
    NAME      VARCHAR(200),
    READ_ONLY VARCHAR(200) DEFAULT 'from-db'
);

CREATE TABLE WITH_LOCAL_DATE_TIME
(
    ID        BIGINT PRIMARY KEY,
    TEST_TIME TIMESTAMP(9) WITHOUT TIME ZONE
);

CREATE TABLE WITH_ID_ONLY
(
    ID SERIAL PRIMARY KEY
);

CREATE TABLE WITH_INSERT_ONLY
(
    ID        SERIAL PRIMARY KEY,
    INSERT_ONLY VARCHAR(100)
);

CREATE TABLE MULTIPLE_COLLECTIONS
(
    ID        SERIAL PRIMARY KEY,
    NAME VARCHAR(100)
);

CREATE TABLE SET_ELEMENT
(
    MULTIPLE_COLLECTIONS BIGINT,
    NAME VARCHAR(100)
);

CREATE TABLE LIST_ELEMENT
(
    MULTIPLE_COLLECTIONS BIGINT,
    MULTIPLE_COLLECTIONS_KEY INT,
    NAME VARCHAR(100)
);

CREATE TABLE MAP_ELEMENT
(
    MULTIPLE_COLLECTIONS BIGINT,
    MULTIPLE_COLLECTIONS_KEY VARCHAR(10),
    ENUM_MAP_OWNER BIGINT,
    ENUM_MAP_OWNER_KEY VARCHAR(10),
    NAME VARCHAR(100)
);

CREATE TABLE AUTHOR
(
    ID SERIAL PRIMARY KEY
);

CREATE TABLE BOOK
(
    AUTHOR BIGINT,
    NAME VARCHAR(100)
);

CREATE TABLE ENUM_MAP_OWNER
(
    ID BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1) PRIMARY KEY,
    NAME VARCHAR(100)
);

CREATE TABLE WITH_ONE_TO_ONE
(
    ID VARCHAR(100)
);

CREATE TABLE REFERENCED
(
    "renamed" VARCHAR(100),
    ID BIGINT
);

CREATE TABLE FIRST
(
    ID   BIGINT      NOT NULL PRIMARY KEY,
    NAME VARCHAR(20) NOT NULL
);

CREATE TABLE SEC
(
    ID    BIGINT      NOT NULL PRIMARY KEY,
    FIRST BIGINT      NOT NULL,
    NAME  VARCHAR(20) NOT NULL,
    FOREIGN KEY (FIRST) REFERENCES FIRST (ID)
);

CREATE TABLE THIRD
(
    SEC BIGINT      NOT NULL,
    NAME   VARCHAR(20) NOT NULL,
    FOREIGN KEY (SEC) REFERENCES SEC (ID)
);

CREATE TABLE "BEFORE_CONVERT_CALLBACK_FOR_SAVE_BATCH"
(
    ID VARCHAR PRIMARY KEY,
    NAME VARCHAR
);
