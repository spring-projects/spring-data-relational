package org.springframework.data.r2dbc.dialect;

/**
 * A clause representing Dialect-specific {@code LIMIT}.
 *
 * @author Mark Paluch
 */
public interface LimitClause {

	/**
	 * Returns the {@code LIMIT} clause
	 *
	 * @param limit the actual limit to use.
	 * @return rendered limit clause.
	 */
	String getClause(long limit);

	/**
	 * Returns the {@code LIMIT} clause
	 *
	 * @param limit the actual limit to use.
	 * @param offset the offset to start from.
	 * @return rendered limit clause.
	 */
	String getClause(long limit, long offset);

	/**
	 * Returns the {@link Position} where to apply the {@link #getClause(long) clause}.
	 */
	Position getClausePosition();

	/**
	 * Enumeration of where to render the clause within the SQL statement.
	 */
	enum Position {

		/**
		 * Append the clause at the end of the statement.
		 */
		END
	}
}
