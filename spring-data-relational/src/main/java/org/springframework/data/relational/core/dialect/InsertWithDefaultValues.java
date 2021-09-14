package org.springframework.data.relational.core.dialect;

/**
 * This interface indicates
 * @author Mikhail Polivakha
 */
public interface InsertWithDefaultValues {

    default String getDefaultInsertPart() {
        return " VALUES (DEFAULT) ";
    }
}