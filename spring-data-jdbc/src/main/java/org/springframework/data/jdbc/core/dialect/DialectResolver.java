/*
 * Copyright 2020-present the original author or authors.
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
package org.springframework.data.jdbc.core.dialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.io.support.SpringFactoriesLoader;
import org.springframework.dao.NonTransientDataAccessException;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.Escaper;
import org.springframework.data.relational.core.dialect.IdGeneration;
import org.springframework.data.relational.core.dialect.InsertRenderContext;
import org.springframework.data.relational.core.dialect.LimitClause;
import org.springframework.data.relational.core.dialect.LockClause;
import org.springframework.data.relational.core.dialect.OrderByNullPrecedence;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SimpleFunction;
import org.springframework.data.relational.core.sql.render.SelectRenderContext;
import org.springframework.data.util.Optionals;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

/**
 * Resolves a {@link Dialect}. Resolution typically uses {@link JdbcOperations} to obtain and inspect a
 * {@link Connection}. Dialect resolution uses Spring's {@link SpringFactoriesLoader spring.factories} to determine
 * available {@link JdbcDialectProvider extensions}.
 *
 * @author Jens Schauder
 * @author Mikhail Polivakha
 * @since 3.5
 * @see Dialect
 * @see SpringFactoriesLoader
 */
public class DialectResolver {

	private static final Log LOG = LogFactory.getLog(DialectResolver.class);

	private static final List<JdbcDialectProvider> DETECTORS = SpringFactoriesLoader
			.loadFactories(JdbcDialectProvider.class, DialectResolver.class.getClassLoader());

	private static final List<org.springframework.data.jdbc.repository.config.DialectResolver.JdbcDialectProvider> LEGACY_DETECTORS = SpringFactoriesLoader
			.loadFactories(org.springframework.data.jdbc.repository.config.DialectResolver.JdbcDialectProvider.class,
					DialectResolver.class.getClassLoader());

	// utility constructor.
	private DialectResolver() {}

	/**
	 * Retrieve a {@link Dialect} by inspecting a {@link Connection}.
	 *
	 * @param operations must not be {@literal null}.
	 * @return the resolved {@link Dialect} {@link NoDialectException} if the database type cannot be determined from
	 *         {@link DataSource}.
	 * @throws NoDialectException if no {@link Dialect} can be found.
	 */
	public static JdbcDialect getDialect(JdbcOperations operations) {

		return Stream.concat(LEGACY_DETECTORS.stream(), DETECTORS.stream()) //
				.map(it -> it.getDialect(operations)) //
				.flatMap(Optionals::toStream) //
				.map(it -> it instanceof JdbcDialect jd ? jd : new JdbcDialectAdapter(it)).findFirst() //
				.orElseThrow(() -> new NoDialectException(
						String.format("Cannot determine a dialect for %s; Please provide a Dialect", operations)));
	}

	/**
	 * SPI to extend Spring's default JDBC Dialect discovery mechanism. Implementations of this interface are discovered
	 * through Spring's {@link SpringFactoriesLoader} mechanism.
	 *
	 * @author Jens Schauder
	 * @see SpringFactoriesLoader
	 */
	public interface JdbcDialectProvider {

		/**
		 * Returns a {@link Dialect} for a {@link DataSource}.
		 *
		 * @param operations the {@link JdbcOperations} to be used with the {@link Dialect}.
		 * @return {@link Optional} containing the {@link Dialect} if the {@link JdbcDialectProvider} can provide a dialect
		 *         object, otherwise {@link Optional#empty()}.
		 */
		Optional<Dialect> getDialect(JdbcOperations operations);
	}

	public static class DefaultDialectProvider implements JdbcDialectProvider {

		@Override
		public Optional<Dialect> getDialect(JdbcOperations operations) {
			return Optional.ofNullable(operations.execute((ConnectionCallback<Dialect>) DefaultDialectProvider::getDialect));
		}

		@Nullable
		private static JdbcDialect getDialect(Connection connection) throws SQLException {

			DatabaseMetaData metaData = connection.getMetaData();

			String name = metaData.getDatabaseProductName().toLowerCase(Locale.ENGLISH);

			if (name.contains("hsql")) {
				return JdbcHsqlDbDialect.INSTANCE;
			}
			if (name.contains("h2")) {
				return JdbcH2Dialect.INSTANCE;
			}
			if (name.contains("mysql")) {
				return new JdbcMySqlDialect(getIdentifierProcessing(metaData));
			}
			if (name.contains("mariadb")) {
				return new JdbcMariaDbDialect(getIdentifierProcessing(metaData));
			}
			if (name.contains("postgresql")) {
				return JdbcPostgresDialect.INSTANCE;
			}
			if (name.contains("microsoft")) {
				return JdbcSqlServerDialect.INSTANCE;
			}
			if (name.contains("db2")) {
				return JdbcDb2Dialect.INSTANCE;
			}
			if (name.contains("oracle")) {
				return JdbcOracleDialect.INSTANCE;
			}

			LOG.info(String.format("Couldn't determine Dialect for \"%s\"", name));
			return null;
		}

		private static IdentifierProcessing getIdentifierProcessing(DatabaseMetaData metaData) throws SQLException {

			// getIdentifierQuoteString() returns a space " " if identifier quoting is not
			// supported.
			String quoteString = metaData.getIdentifierQuoteString();
			IdentifierProcessing.Quoting quoting = StringUtils.hasText(quoteString)
					? new IdentifierProcessing.Quoting(quoteString)
					: IdentifierProcessing.Quoting.NONE;

			IdentifierProcessing.LetterCasing letterCasing;
			// IdentifierProcessing tries to mimic the behavior of unquoted identifiers for their quoted variants.
			if (metaData.supportsMixedCaseIdentifiers()) {
				letterCasing = IdentifierProcessing.LetterCasing.AS_IS;
			} else if (metaData.storesUpperCaseIdentifiers()) {
				letterCasing = IdentifierProcessing.LetterCasing.UPPER_CASE;
			} else if (metaData.storesLowerCaseIdentifiers()) {
				letterCasing = IdentifierProcessing.LetterCasing.LOWER_CASE;
			} else { // this shouldn't happen since one of the previous cases should be true.
				// But if it does happen, we go with the ANSI default.
				letterCasing = IdentifierProcessing.LetterCasing.UPPER_CASE;
			}

			return IdentifierProcessing.create(quoting, letterCasing);
		}
	}

	/**
	 * Exception thrown when {@link DialectResolver} cannot resolve a {@link Dialect}.
	 */
	public static class NoDialectException extends NonTransientDataAccessException {

		/**
		 * Constructor for NoDialectFoundException.
		 *
		 * @param msg the detail message
		 */
		protected NoDialectException(String msg) {
			super(msg);
		}
	}

	private static class JdbcDialectAdapter implements JdbcDialect {

		private final Dialect delegate;
		private final JdbcArrayColumnsAdapter arrayColumns;

		public JdbcDialectAdapter(Dialect delegate) {
			this.delegate = delegate;
			this.arrayColumns = new JdbcArrayColumnsAdapter(delegate.getArraySupport());
		}

		@Override
		public LimitClause limit() {
			return delegate.limit();
		}

		@Override
		public LockClause lock() {
			return delegate.lock();
		}

		@Override
		public JdbcArrayColumns getArraySupport() {
			return arrayColumns;
		}

		@Override
		public SelectRenderContext getSelectContext() {
			return delegate.getSelectContext();
		}

		@Override
		public IdentifierProcessing getIdentifierProcessing() {
			return delegate.getIdentifierProcessing();
		}

		@Override
		public Escaper getLikeEscaper() {
			return delegate.getLikeEscaper();
		}

		@Override
		public IdGeneration getIdGeneration() {
			return delegate.getIdGeneration();
		}

		@Override
		public Collection<Object> getConverters() {
			return delegate.getConverters();
		}

		@Override
		public Set<Class<?>> simpleTypes() {
			return delegate.simpleTypes();
		}

		@Override
		public InsertRenderContext getInsertRenderContext() {
			return delegate.getInsertRenderContext();
		}

		@Override
		public OrderByNullPrecedence orderByNullHandling() {
			return delegate.orderByNullHandling();
		}

		@Override
		public SimpleFunction getExistsFunction() {
			return delegate.getExistsFunction();
		}

		@Override
		public boolean supportsSingleQueryLoading() {
			return delegate.supportsSingleQueryLoading();
		}
	}

}
