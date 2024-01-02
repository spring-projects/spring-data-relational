/*
 * Copyright 2023-2024 the original author or authors.
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
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;

/**
 * Reads complete Aggregates from the database, by generating appropriate SQL using a {@link SingleQuerySqlGenerator}
 * through {@link org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate}. Results are converted into an
 * intermediate {@link RowDocumentResultSetExtractor RowDocument} and mapped via
 * {@link org.springframework.data.relational.core.conversion.RelationalConverter#read(Class, RowDocument)}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 3.2
 */
class AggregateReader implements PathToColumnMapping {

	private final AliasFactory aliasFactory;
	private final SqlGenerator sqlGenerator;
	private final JdbcConverter converter;
	private final NamedParameterJdbcOperations jdbcTemplate;
	private final RowDocumentResultSetExtractor extractor;

	AggregateReader(Dialect dialect, JdbcConverter converter, NamedParameterJdbcOperations jdbcTemplate) {

		this.aliasFactory = new AliasFactory();
		this.converter = converter;
		this.jdbcTemplate = jdbcTemplate;
		this.sqlGenerator = new SingleQuerySqlGenerator(converter.getMappingContext(), aliasFactory, dialect);
		this.extractor = new RowDocumentResultSetExtractor(converter.getMappingContext(), this);
	}

	@Override
	public String column(AggregatePath path) {

		String alias = aliasFactory.getColumnAlias(path);

		if (alias == null) {
			throw new IllegalStateException(String.format("Alias for '%s' must not be null", path));
		}

		return alias;
	}

	@Override
	public String keyColumn(AggregatePath path) {
		return aliasFactory.getKeyAlias(path);
	}

	/**
	 * Select a single aggregate by its identifier.
	 *
	 * @param id the identifier, must not be {@literal null}.
	 * @param entity the persistent entity type must not be {@literal null}.
	 * @return the found aggregate root, or {@literal null} if not found.
	 * @param <T> aggregator type.
	 */
	@Nullable
	public <T> T findById(Object id, RelationalPersistentEntity<T> entity) {

		Query query = Query.query(Criteria.where(entity.getRequiredIdProperty().getName()).is(id)).limit(1);

		return findOne(query, entity);
	}

	/**
	 * Select a single aggregate by a {@link Query}.
	 *
	 * @param query the query to run, must not be {@literal null}.
	 * @param entity the persistent entity type must not be {@literal null}.
	 * @return the found aggregate root, or {@literal null} if not found.
	 * @param <T> aggregator type.
	 */
	@Nullable
	public <T> T findOne(Query query, RelationalPersistentEntity<T> entity) {
		return doFind(query, entity, rs -> extractZeroOrOne(rs, entity));
	}

	/**
	 * Select aggregates by their identifiers.
	 *
	 * @param ids the identifiers, must not be {@literal null}.
	 * @param entity the persistent entity type must not be {@literal null}.
	 * @return the found aggregate roots. The resulting list can be empty or may not contain objects that correspond to
	 *         the identifiers when the objects are not found in the database.
	 * @param <T> aggregator type.
	 */
	public <T> List<T> findAllById(Iterable<?> ids, RelationalPersistentEntity<T> entity) {

		Collection<?> identifiers = ids instanceof Collection<?> idl ? idl : Streamable.of(ids).toList();
		Query query = Query.query(Criteria.where(entity.getRequiredIdProperty().getName()).in(identifiers));

		return findAll(query, entity);
	}

	/**
	 * Select all aggregates by type.
	 *
	 * @param entity the persistent entity type must not be {@literal null}.
	 * @return the found aggregate roots.
	 * @param <T> aggregator type.
	 */
	@SuppressWarnings("ConstantConditions")
	public <T> List<T> findAll(RelationalPersistentEntity<T> entity) {
		return jdbcTemplate.query(sqlGenerator.findAll(entity),
				(ResultSetExtractor<? extends List<T>>) rs -> extractAll(rs, entity));
	}

	/**
	 * Select all aggregates by query.
	 *
	 * @param query the query to run, must not be {@literal null}.
	 * @param entity the persistent entity type must not be {@literal null}.
	 * @return the found aggregate roots.
	 * @param <T> aggregator type.
	 */
	public <T> List<T> findAll(Query query, RelationalPersistentEntity<T> entity) {
		return doFind(query, entity, rs -> extractAll(rs, entity));
	}

	@SuppressWarnings("ConstantConditions")
	private <T, R> R doFind(Query query, RelationalPersistentEntity<T> entity, ResultSetExtractor<R> extractor) {

		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		Condition condition = createCondition(query, parameterSource, entity);
		String sql = sqlGenerator.findAll(entity, condition);

		return jdbcTemplate.query(sql, parameterSource, extractor);
	}

	@Nullable
	private Condition createCondition(Query query, MapSqlParameterSource parameterSource,
			RelationalPersistentEntity<?> entity) {

		QueryMapper queryMapper = new QueryMapper(converter);

		Optional<CriteriaDefinition> criteria = query.getCriteria();
		return criteria.map(criteriaDefinition -> queryMapper.getMappedObject(parameterSource, criteriaDefinition,
				Table.create(entity.getQualifiedTableName()), entity)).orElse(null);
	}

	/**
	 * Extracts a list of aggregates from the given {@link ResultSet} by utilizing the
	 * {@link RowDocumentResultSetExtractor} and the {@link JdbcConverter}. When used as a method reference this conforms
	 * to the {@link org.springframework.jdbc.core.ResultSetExtractor} contract.
	 *
	 * @param rs the {@link ResultSet} from which to extract the data. Must not be {(}@literal null}.
	 * @return a {@code List} of aggregates, fully converted.
	 * @throws SQLException on underlying JDBC errors.
	 */
	private <T> List<T> extractAll(ResultSet rs, RelationalPersistentEntity<T> entity) throws SQLException {

		Iterator<RowDocument> iterate = extractor.iterate(entity, rs);
		List<T> resultList = new ArrayList<>();

		while (iterate.hasNext()) {
			resultList.add(converter.read(entity.getType(), iterate.next()));
		}

		return resultList;
	}

	/**
	 * Extracts a single aggregate or {@literal null} from the given {@link ResultSet} by utilizing the
	 * {@link RowDocumentResultSetExtractor} and the {@link JdbcConverter}. When used as a method reference this conforms
	 * to the {@link org.springframework.jdbc.core.ResultSetExtractor} contract.
	 *
	 * @param rs the {@link ResultSet} from which to extract the data. Must not be {(}@literal null}.
	 * @return The single instance when the conversion results in exactly one instance. If the {@literal ResultSet} is
	 *         empty, null is returned.
	 * @throws SQLException on underlying JDBC errors.
	 * @throws IncorrectResultSizeDataAccessException when the conversion yields more than one instance.
	 */
	@Nullable
	private <T> T extractZeroOrOne(ResultSet rs, RelationalPersistentEntity<T> entity) throws SQLException {

		Iterator<RowDocument> iterate = extractor.iterate(entity, rs);

		if (iterate.hasNext()) {

			RowDocument object = iterate.next();
			if (iterate.hasNext()) {
				throw new IncorrectResultSizeDataAccessException(1);
			}
			return converter.read(entity.getType(), object);
		}

		return null;
	}

}
