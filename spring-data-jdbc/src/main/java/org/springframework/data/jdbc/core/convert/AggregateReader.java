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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.sqlgeneration.AliasFactory;
import org.springframework.data.relational.core.sqlgeneration.SingleQuerySqlGenerator;
import org.springframework.data.relational.core.sqlgeneration.SqlGenerator;
import org.springframework.data.util.Lazy;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.util.Assert;

/**
 * Reads complete Aggregates from the database, by generating appropriate SQL using a {@link SingleQuerySqlGenerator}
 * and a matching {@link AggregateResultSetExtractor} and invoking a
 * {@link org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate}
 *
 * @param <T> the type of aggregate produced by this reader.
 * @since 3.2
 * @author Jens Schauder
 */
class AggregateReader<T> {

	private final RelationalMappingContext mappingContext;
	private final RelationalPersistentEntity<T> aggregate;
	private final AliasFactory aliasFactory;
	private final org.springframework.data.relational.core.sqlgeneration.SqlGenerator sqlGenerator;
	private final JdbcConverter converter;
	private final NamedParameterJdbcOperations jdbcTemplate;

	AggregateReader(Dialect dialect, JdbcConverter converter, AliasFactory aliasFactory,
			NamedParameterJdbcOperations jdbcTemplate, RelationalPersistentEntity<T> aggregate) {

		this.converter = converter;
		this.aliasFactory = aliasFactory;
		this.mappingContext = converter.getMappingContext();
		this.aggregate = aggregate;
		this.jdbcTemplate = jdbcTemplate;

		this.sqlGenerator = new CachingSqlGenerator(
				new SingleQuerySqlGenerator(mappingContext, aliasFactory, dialect, aggregate));
	}

	public List<T> findAll() {

		String sql = sqlGenerator.findAll();

		PathToColumnMapping pathToColumn = createPathToColumnMapping(aliasFactory);
		AggregateResultSetExtractor<T> extractor = new AggregateResultSetExtractor<>(mappingContext, aggregate, converter,
				pathToColumn);

		Iterable<T> result = jdbcTemplate.query(sql, extractor);

		Assert.state(result != null, "result is null");

		return (List<T>) result;
	}

	public T findById(Object id) {

		PathToColumnMapping pathToColumn = createPathToColumnMapping(aliasFactory);
		AggregateResultSetExtractor<T> extractor = new AggregateResultSetExtractor<>(mappingContext, aggregate, converter,
				pathToColumn);

		String sql = sqlGenerator.findById();

		id = converter.writeValue(id, aggregate.getRequiredIdProperty().getTypeInformation());

		Iterator<T> result = jdbcTemplate.query(sql, Map.of("id", id), extractor).iterator();

		T returnValue = result.hasNext() ? result.next() : null;

		if (result.hasNext()) {
			throw new IncorrectResultSizeDataAccessException(1);
		}

		return returnValue;
	}

	public Iterable<T> findAllById(Iterable<?> ids) {

		PathToColumnMapping pathToColumn = createPathToColumnMapping(aliasFactory);
		AggregateResultSetExtractor<T> extractor = new AggregateResultSetExtractor<>(mappingContext, aggregate, converter,
				pathToColumn);

		String sql = sqlGenerator.findAllById();

		List<Object> convertedIds = new ArrayList<>();
		for (Object id : ids) {
			convertedIds.add(converter.writeValue(id, aggregate.getRequiredIdProperty().getTypeInformation()));
		}

		return jdbcTemplate.query(sql, Map.of("ids", convertedIds), extractor);
	}

	private PathToColumnMapping createPathToColumnMapping(AliasFactory aliasFactory) {
		return new PathToColumnMapping() {
			@Override
			public String column(AggregatePath path) {

				String alias = aliasFactory.getColumnAlias(path);
				Assert.notNull(alias, () -> "alias for >" + path + "<must not be null");
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
	 * @since 3.2
	 * @author Jens Schauder
	 */
	static class CachingSqlGenerator implements org.springframework.data.relational.core.sqlgeneration.SqlGenerator {

		private final org.springframework.data.relational.core.sqlgeneration.SqlGenerator delegate;

		private final Lazy<String> findAll;
		private final Lazy<String> findById;
		private final Lazy<String> findAllById;

		public CachingSqlGenerator(SqlGenerator delegate) {

			this.delegate = delegate;

			findAll = Lazy.of(delegate.findAll());
			findById = Lazy.of(delegate.findById());
			findAllById = Lazy.of(delegate.findAllById());
		}

		@Override
		public String findAll() {
			return findAll.get();
		}

		@Override
		public String findById() {
			return findById.get();
		}

		@Override
		public String findAllById() {
			return findAllById.get();
		}

		@Override
		public AliasFactory getAliasFactory() {
			return delegate.getAliasFactory();
		}
	}
}
