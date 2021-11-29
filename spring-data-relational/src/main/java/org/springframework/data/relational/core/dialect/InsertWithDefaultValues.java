package org.springframework.data.relational.core.dialect;

/**
 * This interface aggregates information about an Insert with default values statement.
 * 
 * @author Mikhail Polivakha
 * @since 2.4
 */
public interface InsertWithDefaultValues {

	/**
	 * @return the part of the sql statement, that follows after <b>INSERT INTO table</b>
	 */
	String getDefaultInsertPart();
}
