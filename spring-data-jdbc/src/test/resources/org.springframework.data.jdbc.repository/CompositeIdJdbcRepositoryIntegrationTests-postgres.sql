DROP TABLE IF EXISTS "with_composite_id";

CREATE TABLE "with_composite_id"
(
    "col_one" INTEGER      NOT NULL,
    "col_two" VARCHAR(255) NOT NULL,
    "NAME"    VARCHAR(255),
    PRIMARY KEY ("col_one", "col_two")
);
