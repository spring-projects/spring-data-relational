DROP TABLE IF EXISTS address;
DROP TABLE IF EXISTS person;

CREATE TABLE person ( id int IDENTITY(1,1) PRIMARY KEY, name VARCHAR(100));
CREATE TABLE address ( id int IDENTITY(1,1) PRIMARY KEY, street VARCHAR(100), person_id INT);
ALTER TABLE address ADD FOREIGN KEY (person_id) REFERENCES person(id);