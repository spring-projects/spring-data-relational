DROP TABLE dummy_entity;
CREATE TABLE dummy_entity
(
    "ID"          SERIAL PRIMARY KEY,
    TEST        VARCHAR(100),
    PREFIX_TEST VARCHAR(100)
);
DROP TABLE dummy_entity2;
CREATE TABLE dummy_entity2
(
    "ID"   SERIAL PRIMARY KEY,
    TEST VARCHAR(100)
);
--
-- SELECT "dummy_entity"."ID" AS "ID",
--        "dummy_entity"."test" AS "test",
--        "dummy_entity"."prefix_test" AS "prefix_test",
--        "PREFIX_dummyEntity2"."id" AS "prefix_dummyentity2_id",
--        "PREFIX_dummyEntity2"."test" AS "prefix_dummyentity2_test"
-- FROM "dummy_entity"
--     LEFT OUTER JOIN "dummy_entity2" AS "PREFIX_dummyEntity2" ON
--         "PREFIX_dummyEntity2"."ID" = "dummy_entity"."ID"