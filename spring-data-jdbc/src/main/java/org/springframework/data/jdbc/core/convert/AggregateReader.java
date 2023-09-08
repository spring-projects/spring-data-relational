/*
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.sqlgeneration.AliasFactory;
import org.springframework.data.relational.core.sqlgeneration.SingleQuerySqlGenerator;
import org.springframework.data.relational.core.sqlgeneration.SqlGenerator;
import org.springframework.data.relational.domain.RowDocument;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Reads complete Aggregates from the database, by generating appropriate SQL using a {@link SingleQuerySqlGenerator}
 * through {@link org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate}. Results are converterd into an
 * intermediate {@link RowDocumentResultSetExtractor RowDocument} and mapped via
 * {@link org.springframework.data.relational.core.conversion.RelationalConverter#read(Class, RowDocument)}.
 *
 * @param <T> the type of aggregate produced by this reader.
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 3.2
 */
class AggregateReader<T> {

	private final RelationalPersistentEntity<T> aggregate;
	private final org.springframework.data.relational.core.sqlgeneration.SqlGenerator sqlGenerator;
	private final JdbcConverter converter;
	private final NamedParameterJdbcOperations jdbcTemplate;
	private final RowDocumentResultSetExtractor extractor;

	AggregateReader(Dialect dialect, JdbcConverter converter, AliasFactory aliasFactory,
			NamedParameterJdbcOperations jdbcTemplate, RelationalPersistentEntity<T> aggregate) {

		this.converter = converter;
		this.aggregate = aggregate;
		this.jdbcTemplate = jdbcTemplate;

		this.sqlGenerator = new CachingSqlGenerator(
				new SingleQuerySqlGenerator(converter.getMappingContext(), aliasFactory, dialect, aggregate));

		this.extractor = new RowDocumentResultSetExtractor(converter.getMappingContext(),
				createPathToColumnMapping(aliasFactory));
	}

	public List<T> findAll() {
		return jdbcTemplate.query(sqlGenerator.findAll(), this::extractAll);
	}

	@Nullable
	public T findById(Object id) {

		id = converter.writeValue(id, aggregate.getRequiredIdProperty().getTypeInformation());

		return jdbcTemplate.query(sqlGenerator.findById(), Map.of("id", id), this::extractZeroOrOne);
	}

	public Iterable<T> findAllById(Iterable<?> ids) {

		List<Object> convertedIds = new ArrayList<>();
		for (Object id : ids) {
			convertedIds.add(converter.writeValue(id, aggregate.getRequiredIdProperty().getTypeInformation()));
		}

		return jdbcTemplate.query(sqlGenerator.findAllById(), Map.of("ids", convertedIds), this::extractAll);
	}

	/**
	 * Extracts a list of aggregates from the given {@link ResultSet} by utilizing the
	 * {@link RowDocumentResultSetExtractor} and the {@link JdbcConverter}. When used as a method reference this conforms
	 * to the {@link org.springframework.jdbc.core.ResultSetExtractor} contract.
	 * 
	 * @param rs the {@link ResultSet} from which to extract the data. Must not be {(}@literal null}.
	 * @return a {@code List} of aggregates, fully converted.
	 * @throws SQLException
	 */
	private List<T> extractAll(ResultSet rs) throws SQLException {

		Iterator<RowDocument> iterate = extractor.iterate(aggregate, rs);
		List<T> resultList = new ArrayList<>();
		while (iterate.hasNext()) {
			resultList.add(converter.read(aggregate.getType(), iterate.next()));
		}

		return resultList;
	}

	/**
	 * Extracts a single aggregate or {@literal null} from the given {@link ResultSet} by utilizing the
	 * {@link RowDocumentResultSetExtractor} and the {@link JdbcConverter}. When used as a method reference this conforms
	 * to the {@link org.springframework.jdbc.core.ResultSetExtractor} contract.
	 *
	 * @param @param rs the {@link ResultSet} from which to extract the data. Must not be {(}@literal null}.
	 * @return The single instance when the conversion results in exactly one instance. If the {@literal ResultSet} is empty, null is returned.
	 * @throws SQLException
	 * @throws IncorrectResultSizeDataAccessException when the conversion yields more than one instance.
	 */
	@Nullable
	private T extractZeroOrOne(ResultSet rs) throws SQLException {

		Iterator<RowDocument> iterate = extractor.iterate(aggregate, rs);
		if (iterate.hasNext()) {

			RowDocument object = iterate.next();
			if (iterate.hasNext()) {
				throw new IncorrectResultSizeDataAccessException(1);
			}
			return converter.read(aggregate.getType(), object);
		}
		return null;
	}

	private PathToColumnMapping createPathToColumnMapping(AliasFactory aliasFactory) {
		return new PathToColumnMapping() {
			@Override
			public String column(AggregatePath path) {

				String alias = aliasFactory.getColumnAlias(path);
				Assert.notNull(alias, () -> "alias for >" + path + "< must not be null");
				return alias;
			}

			@Override
			public String keyColumn(AggregatePath path) {
				return aliasFactory.getKeyAlias(path);
			}
		};
	}

	/**
	 * A wrapper for the {@link org.springframework.data.relational.core.sqlgeneration.SqlGenerator} that caches the
	 * generated statements.
	 *
	 * @author Jens Schauder
	 * @since 3.2
	 */
	static class CachingSqlGenerator implements org.springframework.data.relational.core.sqlgeneration.SqlGenerator {

		private final org.springframework.data.relational.core.sqlgeneration.SqlGenerator delegate;

		private final String findAll;
		private final String findById;
		private final String findAllById;

		public CachingSqlGenerator(SqlGenerator delegate) {

			this.delegate = delegate;

			findAll = delegate.findAll();
			findById = delegate.findById();
			findAllById = delegate.findAllById();
		}

		@Override
		public String findAll() {
			return findAll;
		}

		@Override
		public String findById() {
			return findById;
		}

		@Override
		public String findAllById() {
			return findAllById;
		}

		@Override
		public AliasFactory getAliasFactory() {
			return delegate.getAliasFactory();
		}
	}
}
