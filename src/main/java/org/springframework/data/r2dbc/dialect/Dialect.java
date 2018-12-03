package org.springframework.data.r2dbc.dialect;

/**
 * Represents a dialect that is implemented by a particular database.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public interface Dialect {

	/**
	 * Returns the {@link BindMarkersFactory} used by this dialect.
	 *
	 * @return the {@link BindMarkersFactory} used by this dialect.
	 */
	BindMarkersFactory getBindMarkersFactory();

	/**
	 * Returns the clause to include for returning generated keys. The returned query is directly appended to
	 * {@code INSERT} statements.
	 *
	 * @return the clause to include for returning generated keys.
	 * @deprecated to be removed after upgrading to R2DBC 1.0M7 in favor of using the driver's direct support for
	 *             retrieving generated keys.
	 */
	@Deprecated
	String generatedKeysClause();

	/**
	 * Return the {@link LimitClause} used by this dialect.
	 *
	 * @return the {@link LimitClause} used by this dialect.
	 */
	LimitClause limit();
}
