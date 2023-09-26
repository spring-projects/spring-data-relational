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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sqlgeneration.AliasFactory;
import org.springframework.data.relational.core.sqlgeneration.SingleQuerySqlGenerator;
import org.springframework.data.relational.core.sqlgeneration.SqlGenerator;
import org.springframework.data.relational.domain.RowDocument;
import org.springframework.data.util.Streamable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Reads complete Aggregates from the database, by generating appropriate SQL using a {@link SingleQuerySqlGenerator}
 * through {@link org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate}. Results are converted into an
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
	private final Table table;
	private final SqlGenerator sqlGenerator;
	private final JdbcConverter converter;
	private final NamedParameterJdbcOperations jdbcTemplate;
	private final RowDocumentResultSetExtractor extractor;

	AggregateReader(Dialect dialect, JdbcConverter converter, AliasFactory aliasFactory,
			NamedParameterJdbcOperations jdbcTemplate, RelationalPersistentEntity<T> aggregate) {

		this.converter = converter;
		this.aggregate = aggregate;
		this.jdbcTemplate = jdbcTemplate;
		this.table = Table.create(aggregate.getQualifiedTableName());
		this.sqlGenerator = new SingleQuerySqlGenerator(converter.getMappingContext(), aliasFactory, dialect, aggregate);
		this.extractor = new RowDocumentResultSetExtractor(converter.getMappingContext(),
				createPathToColumnMapping(aliasFactory));
	}

	@Nullable
	public T findById(Object id) {

		Query query = Query.query(Criteria.where(aggregate.getRequiredIdProperty().getName()).is(id)).limit(1);

		return findOne(query);
	}

	@Nullable
	public T findOne(Query query) {

		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		Condition condition = createCondition(query, parameterSource);

		return jdbcTemplate.query(sqlGenerator.findAll(condition), parameterSource, this::extractZeroOrOne);
	}

	public List<T> findAll() {
		return jdbcTemplate.query(sqlGenerator.findAll(), this::extractAll);
	}

	public List<T> findAllById(Iterable<?> ids) {

		Collection<?> identifiers = ids instanceof Collection<?> idl ? idl : Streamable.of(ids).toList();
		Query query = Query.query(Criteria.where(aggregate.getRequiredIdProperty().getName()).in(identifiers)).limit(1);

		return findAll(query);
	}

	public List<T> findAll(Query query) {

		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		Condition condition = createCondition(query, parameterSource);
		return jdbcTemplate.query(sqlGenerator.findAll(condition), parameterSource, this::extractAll);
	}

	@Nullable
	private Condition createCondition(Query query, MapSqlParameterSource parameterSource) {

		QueryMapper queryMapper = new QueryMapper(converter);

		Optional<CriteriaDefinition> criteria = query.getCriteria();
		return criteria
				.map(criteriaDefinition -> queryMapper.getMappedObject(parameterSource, criteriaDefinition, table, aggregate))
				.orElse(null);
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
	 * @return The single instance when the conversion results in exactly one instance. If the {@literal ResultSet} is
	 *         empty, null is returned.
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

}
