package org.springframework.data.r2dbc.dialect;

import io.r2dbc.spi.Statement;

/**
 * A bind marker represents a single bindable parameter within a query. Bind markers are dialect-specific and provide a
 * {@link #getPlaceholder() placeholder} that is used in the actual query.
 *
 * @author Mark Paluch
 * @see Statement#bind
 * @see BindMarkers
 * @see BindMarkersFactory
 */
public interface BindMarker {

	/**
	 * Returns the database-specific placeholder for a given substitution.
	 *
	 * @return the database-specific placeholder for a given substitution.
	 */
	String getPlaceholder();

	/**
	 * Bind the given {@code value} to the {@link Statement} using the underlying binding strategy.
	 *
	 * @param statement the statement to bind the value to.
	 * @param value the actual value. Must not be {@literal null}. Use {@link #bindNull(Statement, Class)} for
	 *          {@literal null} values.
	 * @see Statement#bind
	 */
	void bindValue(Statement<?> statement, Object value);

	/**
	 * Bind a {@literal null} value to the {@link Statement} using the underlying binding strategy.
	 *
	 * @param statement the statement to bind the value to.
	 * @param valueType value type, must not be {@literal null}.
	 * @see Statement#bindNull
	 */

	void bindNull(Statement<?> statement, Class<?> valueType);
}
