DROP TABLE account_line;
DROP TABLE versioned_account;
DROP TABLE unversioned_account;

CREATE TABLE versioned_account (
    id BIGINT PRIMARY KEY,
    version BIGINT NOT NULL,
    name TEXT
);

CREATE TABLE account_line (
    account BIGINT NOT NULL REFERENCES versioned_account(id),
    position INTEGER NOT NULL,
    description TEXT NOT NULL,
    PRIMARY KEY (account, position)
);

CREATE TABLE unversioned_account (
    id BIGINT PRIMARY KEY,
    name TEXT
);
