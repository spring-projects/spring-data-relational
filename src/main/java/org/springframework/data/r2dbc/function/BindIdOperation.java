package org.springframework.data.r2dbc.function;

import io.r2dbc.spi.Statement;

/**
 * Extension to {@link BindableOperation} for operations that allow parameter substitution for a single {@code id}
 * column that accepts either a single value or multiple values, depending on the underlying operation.
 *
 * @author Mark Paluch
 * @see Statement#bind
 * @see Statement#bindNull
 */
public interface BindIdOperation extends BindableOperation {

	/**
	 * Bind the given {@code value} to the {@link Statement} using the underlying binding strategy.
	 *
	 * @param statement the statement to bind the value to.
	 * @param value the actual value. Must not be {@literal null}.
	 * @see Statement#bind
	 */
	void bindId(Statement<?> statement, Object value);

	/**
	 * Bind the given {@code values} to the {@link Statement} using the underlying binding strategy.
	 *
	 * @param statement the statement to bind the value to.
	 * @param values the actual values.
	 * @see Statement#bind
	 */
	void bindIds(Statement<?> statement, Iterable<? extends Object> values);
}
