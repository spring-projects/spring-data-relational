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
 * @deprecated since 1.2 in favor of Spring R2DBC. Use {@link org.springframework.r2dbc.core.binding} instead.
 */
@Deprecated
public interface BindMarker extends org.springframework.r2dbc.core.binding.BindMarker {

	/**
	 * Returns the database-specific placeholder for a given substitution.
	 *
	 * @return the database-specific placeholder for a given substitution.
	 */
	String getPlaceholder();

	/**
	 * Bind the given {@code value} to the {@link Statement} using the underlying binding strategy.
	 *
	 * @param bindTarget the target to bind the value to.
	 * @param value the actual value. Must not be {@literal null}. Use {@link #bindNull(BindTarget, Class)} for
	 *          {@literal null} values.
	 * @see Statement#bind
	 */
	void bind(BindTarget bindTarget, Object value);

	/**
	 * Bind a {@literal null} value to the {@link Statement} using the underlying binding strategy.
	 *
	 * @param bindTarget the target to bind the value to.
	 * @param valueType value type, must not be {@literal null}.
	 * @see Statement#bindNull
	 */
	void bindNull(BindTarget bindTarget, Class<?> valueType);
}
