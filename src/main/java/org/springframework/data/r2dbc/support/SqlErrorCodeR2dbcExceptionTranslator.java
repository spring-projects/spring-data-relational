/*
 * Copyright 2018-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.r2dbc.support;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.R2dbcException;

import java.sql.SQLException;
import java.util.Arrays;

import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.CannotSerializeTransactionException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.PermissionDeniedDataAccessException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.data.r2dbc.BadSqlGrammarException;
import org.springframework.data.r2dbc.InvalidResultAccessException;
import org.springframework.jdbc.support.SQLErrorCodes;
import org.springframework.jdbc.support.SQLErrorCodesFactory;
import org.springframework.jdbc.support.SQLExceptionTranslator;
import org.springframework.lang.Nullable;

/**
 * Implementation of {@link R2dbcExceptionTranslator} that analyzes vendor-specific error codes. More precise than an
 * implementation based on SQL state, but heavily vendor-specific.
 * <p>
 * This class applies the following matching rules:
 * <ul>
 * <li>Try custom translation implemented by any subclass. Note that this class is concrete and is typically used
 * itself, in which case this rule doesn't apply.
 * <li>Apply error code matching. Error codes are obtained from the SQLErrorCodesFactory by default. This factory loads
 * a "sql-error-codes.xml" file from the class path, defining error code mappings for database names from database
 * meta-data.
 * <li>Fallback to a fallback translator. {@link SqlStateR2dbcExceptionTranslator} is the default fallback translator,
 * analyzing the exception's SQL state only.
 * </ul>
 * <p>
 * The configuration file named "sql-error-codes.xml" is by default read from the
 * {@code org.springframework.jdbc.support} package. It can be overridden through a file of the same name in the root of
 * the class path (e.g. in the "/WEB-INF/classes" directory), as long as the Spring JDBC package is loaded from the same
 * ClassLoader.
 *
 * @author Mark Paluch
 * @see SQLErrorCodesFactory
 * @see SqlStateR2dbcExceptionTranslator
 * @deprecated since 1.2. Use Spring R2DBC's
 *             {@link org.springframework.r2dbc.connection.ConnectionFactoryUtils#convertR2dbcException(String, String, R2dbcException)}
 *             instead.
 */
@Deprecated
public class SqlErrorCodeR2dbcExceptionTranslator extends AbstractFallbackR2dbcExceptionTranslator {

	/** Error codes used by this translator */
	@Nullable private SQLErrorCodes sqlErrorCodes;

	/**
	 * Creates a new {@link SqlErrorCodeR2dbcExceptionTranslator}. The {@link SQLErrorCodes} or
	 * {@link io.r2dbc.spi.ConnectionFactory} property must be set.
	 */
	public SqlErrorCodeR2dbcExceptionTranslator() {}

	/**
	 * Create a SQL error code translator for the given DataSource. Invoking this constructor will cause a Connection to
	 * be obtained from the DataSource to get the meta-data.
	 *
	 * @param connectionFactory {@link ConnectionFactory} to use to find meta-data and establish which error codes are
	 *          usable.
	 * @see SQLErrorCodesFactory
	 */
	public SqlErrorCodeR2dbcExceptionTranslator(ConnectionFactory connectionFactory) {
		this();
		setConnectionFactory(connectionFactory);
	}

	/**
	 * Create a SQL error code translator for the given database product name. Invoking this constructor will avoid
	 * obtaining a Connection from the DataSource to get the meta-data.
	 *
	 * @param dbName the database product name that identifies the error codes entry
	 * @see SQLErrorCodesFactory
	 * @see java.sql.DatabaseMetaData#getDatabaseProductName()
	 */
	public SqlErrorCodeR2dbcExceptionTranslator(String dbName) {
		this();
		setDatabaseProductName(dbName);
	}

	/**
	 * Create a SQLErrorCode translator given these error codes. Does not require a database meta-data lookup to be
	 * performed using a connection.
	 *
	 * @param sec error codes
	 */
	public SqlErrorCodeR2dbcExceptionTranslator(@Nullable SQLErrorCodes sec) {
		this();
		this.sqlErrorCodes = sec;
	}

	/**
	 * Set the DataSource for this translator.
	 * <p>
	 * Setting this property will cause a Connection to be obtained from the DataSource to get the meta-data.
	 *
	 * @param connectionFactory {@link ConnectionFactory} to use to find meta-data and establish which error codes are
	 *          usable.
	 * @see SQLErrorCodesFactory#getErrorCodes(String)
	 * @see io.r2dbc.spi.ConnectionFactoryMetadata#getName()
	 */
	public void setConnectionFactory(ConnectionFactory connectionFactory) {
		this.sqlErrorCodes = SQLErrorCodesFactory.getInstance().getErrorCodes(connectionFactory.getMetadata().getName());
	}

	/**
	 * Set the database product name for this translator.
	 * <p>
	 * Setting this property will avoid obtaining a Connection from the DataSource to get the meta-data.
	 *
	 * @param dbName the database product name that identifies the error codes entry.
	 * @see SQLErrorCodesFactory#getErrorCodes(String)
	 * @see io.r2dbc.spi.ConnectionFactoryMetadata#getName()
	 */
	public void setDatabaseProductName(String dbName) {
		this.sqlErrorCodes = SQLErrorCodesFactory.getInstance().getErrorCodes(dbName);
	}

	/**
	 * Set custom error codes to be used for translation.
	 *
	 * @param sec custom error codes to use.
	 */
	public void setSqlErrorCodes(@Nullable SQLErrorCodes sec) {
		this.sqlErrorCodes = sec;
	}

	/**
	 * Return the error codes used by this translator. Usually determined via a DataSource.
	 *
	 * @see #setConnectionFactory
	 */
	@Nullable
	public SQLErrorCodes getSqlErrorCodes() {
		return this.sqlErrorCodes;
	}

	@Override
	@Nullable
	protected DataAccessException doTranslate(String task, @Nullable String sql, R2dbcException ex) {

		R2dbcException translated = ex;

		// First, try custom translation from overridden method.
		DataAccessException dex = customTranslate(task, sql, translated);
		if (dex != null) {
			return dex;
		}

		// Next, try the custom SQLExceptionTranslator, if available.
		if (this.sqlErrorCodes != null) {
			SQLExceptionTranslator customTranslator = this.sqlErrorCodes.getCustomSqlExceptionTranslator();
			if (customTranslator != null) {
				DataAccessException customDex = customTranslator.translate(task, sql,
						new SQLException(ex.getMessage(), ex.getSqlState(), ex));
				if (customDex != null) {
					return customDex;
				}
			}
		}

		// Check SQLErrorCodes with corresponding error code, if available.
		if (this.sqlErrorCodes != null) {
			String errorCode;
			if (this.sqlErrorCodes.isUseSqlStateForTranslation()) {
				errorCode = translated.getSqlState();
			} else {
				// Try to find R2dbcException with actual error code, looping through the causes.
				R2dbcException current = translated;
				while (current.getErrorCode() == 0 && current.getCause() instanceof R2dbcException) {
					current = (R2dbcException) current.getCause();
				}
				errorCode = Integer.toString(current.getErrorCode());
			}

			if (errorCode != null) {
				// Look for grouped error codes.
				if (Arrays.binarySearch(this.sqlErrorCodes.getBadSqlGrammarCodes(), errorCode) >= 0) {
					logTranslation(task, sql, translated);
					return new BadSqlGrammarException(task, (sql != null ? sql : ""), translated);
				} else if (Arrays.binarySearch(this.sqlErrorCodes.getInvalidResultSetAccessCodes(), errorCode) >= 0) {
					logTranslation(task, sql, translated);
					return new InvalidResultAccessException(task, (sql != null ? sql : ""), translated);
				} else if (Arrays.binarySearch(this.sqlErrorCodes.getDuplicateKeyCodes(), errorCode) >= 0) {
					logTranslation(task, sql, translated);
					return new DuplicateKeyException(buildMessage(task, sql, translated), translated);
				} else if (Arrays.binarySearch(this.sqlErrorCodes.getDataIntegrityViolationCodes(), errorCode) >= 0) {
					logTranslation(task, sql, translated);
					return new DataIntegrityViolationException(buildMessage(task, sql, translated), translated);
				} else if (Arrays.binarySearch(this.sqlErrorCodes.getPermissionDeniedCodes(), errorCode) >= 0) {
					logTranslation(task, sql, translated);
					return new PermissionDeniedDataAccessException(buildMessage(task, sql, translated), translated);
				} else if (Arrays.binarySearch(this.sqlErrorCodes.getDataAccessResourceFailureCodes(), errorCode) >= 0) {
					logTranslation(task, sql, translated);
					return new DataAccessResourceFailureException(buildMessage(task, sql, translated), translated);
				} else if (Arrays.binarySearch(this.sqlErrorCodes.getTransientDataAccessResourceCodes(), errorCode) >= 0) {
					logTranslation(task, sql, translated);
					return new TransientDataAccessResourceException(buildMessage(task, sql, translated), translated);
				} else if (Arrays.binarySearch(this.sqlErrorCodes.getCannotAcquireLockCodes(), errorCode) >= 0) {
					logTranslation(task, sql, translated);
					return new CannotAcquireLockException(buildMessage(task, sql, translated), translated);
				} else if (Arrays.binarySearch(this.sqlErrorCodes.getDeadlockLoserCodes(), errorCode) >= 0) {
					logTranslation(task, sql, translated);
					return new DeadlockLoserDataAccessException(buildMessage(task, sql, translated), translated);
				} else if (Arrays.binarySearch(this.sqlErrorCodes.getCannotSerializeTransactionCodes(), errorCode) >= 0) {
					logTranslation(task, sql, translated);
					return new CannotSerializeTransactionException(buildMessage(task, sql, translated), translated);
				}
			}
		}

		// We couldn't identify it more precisely - let's hand it over to the SQLState fallback translator.
		if (logger.isDebugEnabled()) {
			String codes;
			if (this.sqlErrorCodes != null && this.sqlErrorCodes.isUseSqlStateForTranslation()) {
				codes = "SQL state '" + translated.getSqlState() + "', error code '" + translated.getErrorCode();
			} else {
				codes = "Error code '" + translated.getErrorCode() + "'";
			}
			logger.debug("Unable to translate R2dbcException with " + codes + ", will now try the fallback translator");
		}

		return null;
	}

	/**
	 * Subclasses can override this method to attempt a custom mapping from {@link R2dbcException} to
	 * {@link DataAccessException}.
	 *
	 * @param task readable text describing the task being attempted
	 * @param sql SQL query or update that caused the problem. May be {@literal null}.
	 * @param ex the offending {@link R2dbcException}.
	 * @return null if no custom translation was possible, otherwise a {@link DataAccessException} resulting from custom
	 *         translation. This exception should include the {@link R2dbcException} parameter as a nested root cause.
	 *         This implementation always returns null, meaning that the translator always falls back to the default error
	 *         codes.
	 */
	@Nullable
	protected DataAccessException customTranslate(String task, @Nullable String sql, R2dbcException ex) {
		return null;
	}

	private void logTranslation(String task, @Nullable String sql, R2dbcException exception) {

		if (logger.isDebugEnabled()) {

			String intro = "Translating";
			logger.debug(intro + " R2dbcException with SQL state '" + exception.getSqlState() + "', error code '"
					+ exception.getErrorCode() + "', message [" + exception.getMessage() + "]"
					+ (sql != null ? "; SQL was [" + sql + "]" : "") + " for task [" + task + "]");
		}
	}
}
