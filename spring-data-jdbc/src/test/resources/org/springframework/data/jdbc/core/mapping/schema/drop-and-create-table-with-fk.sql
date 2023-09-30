CREATE TABLE group_of_persons
(
    id         int primary key
);

CREATE TABLE table_to_drop
(
    id         int primary key,
    persons int,
    constraint fk_to_drop foreign key (persons) references group_of_persons(id)
);
