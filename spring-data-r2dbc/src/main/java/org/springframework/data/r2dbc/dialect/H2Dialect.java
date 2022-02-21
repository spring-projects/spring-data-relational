package org.springframework.data.r2dbc.dialect;

import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * An SQL dialect for H2 in Postgres Compatibility mode.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public class H2Dialect extends PostgresDialect {

	/**
	 * Singleton instance.
	 */
	public static final H2Dialect INSTANCE = new H2Dialect();

	@Override
	public String renderForGeneratedValues(SqlIdentifier identifier) {
		return identifier.getReference(getIdentifierProcessing());
	}
}
