package org.springframework.data.jdbc.core.dialect;

import java.sql.JDBCType;
import java.sql.SQLType;

/**
 * Interface for defining what to {@link SQLType} to use for {@literal null} values.
 * 
 * @author Jens Schauder
 * @since 4.0
 */
public interface NullTypeStrategy {

	/**
	 * Implementation that always uses {@link JDBCType#NULL}. Suitable for all databases that actually support this
	 * {@link JDBCType}.
	 */
	NullTypeStrategy DEFAULT = sqlType -> JDBCType.NULL;

	/**
	 * Implementation that uses what ever type was past in as an argument. Suitable for databases that do not support
	 * {@link JDBCType#NULL}.
	 */
	NullTypeStrategy NOOP = sqlType -> sqlType;

	/**
	 * {@link SQLType} to use for {@literal null} values.
	 * 
	 * @param sqlType a fallback value that is considered suitable by the caller.
	 * @return Guaranteed not to be {@literal null}.
	 */
	SQLType getNullType(SQLType sqlType);
}
