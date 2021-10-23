package org.springframework.data.relational.core.dialect;

/**
 * This interface aggregates information about an Insert with default values statement.
 * @author Mikhail Polivakha
 */
public interface InsertWithDefaultValues {

    /**
     * @return the part of the sql statement, that follows after <b>INSERT INTO table</b>
     */
    default String getDefaultInsertPart() {
        return InsertDefaultValues.DEFAULT.getDefaultInsertPart();
    }
}