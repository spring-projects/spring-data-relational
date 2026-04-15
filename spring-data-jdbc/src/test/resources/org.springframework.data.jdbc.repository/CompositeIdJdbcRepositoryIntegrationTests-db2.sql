DROP TABLE "with_composite_id";

CREATE TABLE "with_composite_id"
(
    "col_one" INTEGER      NOT NULL,
    "col_two" VARCHAR(255) NOT NULL,
    "NAME"    VARCHAR(255),
    PRIMARY KEY ("col_one", "col_two")
);

INSERT INTO "with_composite_id" ("col_one", "col_two", "NAME") VALUES (42, 'HBAR', 'Walter');
INSERT INTO "with_composite_id" ("col_one", "col_two", "NAME") VALUES (23, '2PI', 'Jesse');
