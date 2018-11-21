package org.springframework.data.r2dbc.dialect;

/**
 * Represents a dialect that is implemented by a particular database.
 *
 * @author Mark Paluch
 */
public interface Dialect {

	/**
	 * Returns the {@link BindMarkersFactory} used by this dialect.
	 *
	 * @return the {@link BindMarkersFactory} used by this dialect.
	 */
	BindMarkersFactory getBindMarkersFactory();

	/**
	 * Returns the statement to include for returning generated keys. The returned query is directly appended to
	 * {@code INSERT} statements.
	 *
	 * @return the statement to include for returning generated keys.
	 */
	String returnGeneratedKeys();

	/**
	 * Return the {@link LimitClause} used by this dialect.
	 *
	 * @return the {@link LimitClause} used by this dialect.
	 */
	LimitClause limit();
}
