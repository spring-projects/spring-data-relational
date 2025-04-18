DROP TABLE person;
DROP TABLE address;
CREATE TABLE person ( id SERIAL PRIMARY KEY, name VARCHAR(100));
CREATE TABLE address ( id SERIAL PRIMARY KEY, street VARCHAR(100), person_id INT);
ALTER TABLE address ADD FOREIGN KEY (person_id) REFERENCES person(id);