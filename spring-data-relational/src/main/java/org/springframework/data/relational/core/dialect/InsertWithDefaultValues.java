package org.springframework.data.relational.core.dialect;

import org.springframework.data.relational.core.sql.Insert;

/**
 * This interface aggregates information about an {@link Insert} with default values statement.
 * @author Mikhail Polivakha
 */
public interface InsertWithDefaultValues {

    /**
     * @return the part of the sql statement, that follows after <b>INSERT INTO table</b>
     */
    default String getDefaultInsertPart() {
        return " VALUES (DEFAULT) ";
    }
}