package org.springframework.data.r2dbc.dialect;

/**
 * An SQL dialect for H2 in Postgres Compatibility mode.
 *
 * @author Mark Paluch
 */
public class H2Dialect extends PostgresDialect {

	/**
	 * Singleton instance.
	 */
	public static final H2Dialect INSTANCE = new H2Dialect();

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.Dialect#returnGeneratedKeys()
	 */
	@Override
	public String generatedKeysClause() {
		return "";
	}
}
