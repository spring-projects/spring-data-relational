CREATE TABLE ENTITY_WITH_STRINGY_BIG_DECIMAL ( id SERIAL PRIMARY KEY, Stringy_number DECIMAL(20,10));
CREATE TABLE OTHER_ENTITY ( ID SERIAL PRIMARY KEY, CREATED DATE, ENTITY_WITH_STRINGY_BIG_DECIMAL INTEGER);
CREATE TABLE ENTITY_WITH_ZONED_DATE_TIME (id SERIAL PRIMARY KEY, CREATED_AT TIMESTAMPTZ);
CREATE TABLE ENTITY_WITH_OFFSET_DATE_TIME (id SERIAL PRIMARY KEY, CREATED_AT TIMESTAMPTZ);