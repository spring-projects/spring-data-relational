package org.springframework.data.r2dbc.dialect;

import org.springframework.data.relational.core.dialect.AnsiDialect;
import org.springframework.data.relational.core.dialect.LockClause;
import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * An SQL dialect for H2 in Postgres Compatibility mode.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Diego Krupitza
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

	@Override
	public LockClause lock() {
		// H2 Dialect does not support the same lock keywords as PostgreSQL, but it supports the ANSI SQL standard.
		// see https://www.h2database.com/html/commands.html
		// and https://www.h2database.com/html/features.html#compatibility
		return AnsiDialect.INSTANCE.lock();
	}

}
