package org.springframework.data.relational.core.mapping;

import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.InsertWithDefaultValues;

/**
 * In the scope of Insert with default values SQL statement, for example
 * <b>INSERT INTO SCHEMA.TABLE VALUES (DEFAULT)</b>
 * this enum represents the default values part in different {@link Dialect}s
 *
 * @author Mikhail Polivakha
 * @see InsertWithDefaultValues
 */
public enum InsertDefaultValues {

    DEFAULT(" VALUES (DEFAULT) "),
    MS_SQL_SERVER(" DEFAULT VALUES ");

    private final String defaultInsertPart;

    InsertDefaultValues(String defaultInsertPart) {
        this.defaultInsertPart = defaultInsertPart;
    }

    public String getDefaultInsertPart() {
        return defaultInsertPart;
    }
}