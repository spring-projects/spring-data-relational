CREATE TABLE group_of_persons
(
    id         int primary key
);

CREATE TABLE person
(
    id         int,
    first_name varchar(255),
    last_name varchar(255),
    group_id int,
    group_id_to_drop int,
    constraint fk_to_drop foreign key (group_id_to_drop) references group_of_persons(id)
);
