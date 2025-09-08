/*
 * Copyright 2017-2025 the original author or authors.
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
package org.springframework.data.jdbc.mybatis;

import static java.util.Arrays.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.session.SqlSession;
import org.jspecify.annotations.Nullable;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.CascadingDataAccessStrategy;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.DataAccessStrategyFactory;
import org.springframework.data.jdbc.core.convert.DefaultDataAccessStrategy;
import org.springframework.data.jdbc.core.convert.DelegatingDataAccessStrategy;
import org.springframework.data.jdbc.core.convert.Identifier;
import org.springframework.data.jdbc.core.convert.InsertSubject;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.QueryMappingConfiguration;
import org.springframework.data.jdbc.core.dialect.DialectResolver;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.relational.core.conversion.IdValueSource;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.Assert;

/**
 * {@link DataAccessStrategy} implementation based on MyBatis. Each method gets mapped to a statement. The name of the
 * statement gets constructed as follows: By default, the namespace is based on the class of the entity plus the suffix
 * "Mapper". This is then followed by the method name separated by a dot. For methods taking a {@link PropertyPath} as
 * argument, the relevant entity is that of the root of the path, and the path itself gets as dot separated String
 * appended to the statement name. Each statement gets an instance of {@link MyBatisContext}, which at least has the
 * entityType set. For methods taking a {@link PropertyPath} the entityType if the context is set to the class of the
 * leaf type.
 *
 * @author Jens Schauder
 * @author Kazuki Shimizu
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Tyler Van Gorder
 * @author Milan Milanov
 * @author Myeonghyeon Lee
 * @author Chirag Tailor
 * @author Christopher Klein
 * @author Mikhail Polivakha
 * @author Sergey Korotaev
 * @author Jaeyeon Kim
 */
public class MyBatisDataAccessStrategy implements DataAccessStrategy {

	private static final String VERSION_SQL_PARAMETER_NAME_OLD = "___oldOptimisticLockingVersion";

	private final NamedParameterJdbcOperations jdbcOperations;
	private final Dialect dialect;
	private final SqlSession sqlSession;
	private NamespaceStrategy namespaceStrategy = NamespaceStrategy.DEFAULT_INSTANCE;

	/**
	 * Create a {@link DataAccessStrategy} that first checks for queries defined by MyBatis and if it doesn't find one
	 * uses a {@link DefaultDataAccessStrategy}
	 */
	public static DataAccessStrategy createCombinedAccessStrategy(RelationalMappingContext context,
			JdbcConverter converter, NamedParameterJdbcOperations operations, SqlSession sqlSession, Dialect dialect,
			QueryMappingConfiguration queryMappingConfiguration) {
		return createCombinedAccessStrategy(context, converter, operations, sqlSession, NamespaceStrategy.DEFAULT_INSTANCE,
				dialect, queryMappingConfiguration);
	}

	/**
	 * Create a {@link DataAccessStrategy} that first checks for queries defined by MyBatis and if it doesn't find one
	 * uses a {@link DefaultDataAccessStrategy}
	 */
	public static DataAccessStrategy createCombinedAccessStrategy(RelationalMappingContext context,
			JdbcConverter converter, NamedParameterJdbcOperations operations, SqlSession sqlSession,
			NamespaceStrategy namespaceStrategy, Dialect dialect, QueryMappingConfiguration queryMappingConfiguration) {

		DataAccessStrategy defaultDataAccessStrategy = new DataAccessStrategyFactory(converter, operations, dialect,
				queryMappingConfiguration).create();

		// the DefaultDataAccessStrategy needs a reference to the returned DataAccessStrategy. This creates a dependency
		// cycle. In order to create it, we need something that allows to defer closing the cycle until all the elements are
		// created. That is the purpose of the DelegatingAccessStrategy.
		MyBatisDataAccessStrategy myBatisDataAccessStrategy = new MyBatisDataAccessStrategy(operations, dialect,
				sqlSession);
		myBatisDataAccessStrategy.setNamespaceStrategy(namespaceStrategy);

		return new CascadingDataAccessStrategy(
				asList(myBatisDataAccessStrategy, new DelegatingDataAccessStrategy(defaultDataAccessStrategy)));
	}

	/**
	 * Constructs a {@link DataAccessStrategy} based on MyBatis.
	 * <p>
	 * Use a {@link SqlSessionTemplate} for {@link SqlSession} or a similar implementation tying the session to the proper
	 * transaction. Note that the resulting {@link DataAccessStrategy} only handles MyBatis. It does not include the
	 * functionality of the {@link DefaultDataAccessStrategy} which one normally still wants. Use
	 * {@link #createCombinedAccessStrategy(RelationalMappingContext, JdbcConverter, NamedParameterJdbcOperations, SqlSession, NamespaceStrategy, Dialect, QueryMappingConfiguration)}
	 * to create such a {@link DataAccessStrategy}.
	 *
	 * @param sqlSession Must be non {@literal null}.
	 * @since 3.1
	 */
	public MyBatisDataAccessStrategy(SqlSession sqlSession) {
		this.sqlSession = sqlSession;
		this.jdbcOperations = new NamedParameterJdbcTemplate(
				sqlSession.getConfiguration().getEnvironment().getDataSource());
		this.dialect = DialectResolver.getDialect(jdbcOperations.getJdbcOperations());
	}

	MyBatisDataAccessStrategy(NamedParameterJdbcOperations jdbcOperations, Dialect dialect, SqlSession sqlSession) {
		this.jdbcOperations = jdbcOperations;
		this.dialect = dialect;
		this.sqlSession = sqlSession;
	}

	@Override
	public Dialect getDialect() {
		return dialect;
	}

	@Override
	public NamedParameterJdbcOperations getJdbcOperations() {
		return jdbcOperations;
	}

	/**
	 * Set a NamespaceStrategy to be used.
	 *
	 * @param namespaceStrategy Must be non {@literal null}
	 */
	public void setNamespaceStrategy(NamespaceStrategy namespaceStrategy) {

		Assert.notNull(namespaceStrategy, "The NamespaceStrategy must not be null");

		this.namespaceStrategy = namespaceStrategy;
	}

	@Override
	public <T> @Nullable Object insert(T instance, Class<T> domainType, Identifier identifier,
			IdValueSource idValueSource) {

		MyBatisContext myBatisContext = new MyBatisContext(identifier, instance, domainType);
		sqlSession().insert(namespace(domainType) + ".insert", myBatisContext);

		return myBatisContext.getId();
	}

	@Override
	public <T> @Nullable Object[] insert(List<InsertSubject<T>> insertSubjects, Class<T> domainType,
			IdValueSource idValueSource) {

		return insertSubjects.stream().map(
				insertSubject -> insert(insertSubject.getInstance(), domainType, insertSubject.getIdentifier(), idValueSource))
				.toArray();
	}

	@Override
	public <S> boolean update(S instance, Class<S> domainType) {

		return sqlSession().update(namespace(domainType) + ".update",
				new MyBatisContext(null, instance, domainType, Collections.emptyMap())) != 0;
	}

	@Override
	public <S> boolean updateWithVersion(S instance, Class<S> domainType, Number previousVersion) {

		String statement = namespace(domainType) + ".updateWithVersion";
		MyBatisContext parameter = new MyBatisContext(null, instance, domainType,
				Collections.singletonMap(VERSION_SQL_PARAMETER_NAME_OLD, previousVersion));
		return sqlSession().update(statement, parameter) != 0;
	}

	@Override
	public void delete(Object id, Class<?> domainType) {

		String statement = namespace(domainType) + ".delete";
		MyBatisContext parameter = new MyBatisContext(id, null, domainType, Collections.emptyMap());
		sqlSession().delete(statement, parameter);
	}

	@Override
	public void delete(Iterable<Object> ids, Class<?> domainType) {
		ids.forEach(id -> delete(id, domainType));
	}

	@Override
	public <T> void deleteWithVersion(Object id, Class<T> domainType, Number previousVersion) {

		String statement = namespace(domainType) + ".deleteWithVersion";
		MyBatisContext parameter = new MyBatisContext(id, null, domainType,
				Collections.singletonMap(VERSION_SQL_PARAMETER_NAME_OLD, previousVersion));
		sqlSession().delete(statement, parameter);
	}

	@Override
	public void delete(Object rootId, PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {

		Class<?> ownerType = getOwnerTyp(propertyPath);
		String statement = namespace(ownerType) + ".delete-" + toDashPath(propertyPath);
		Class<?> leafType = propertyPath.getLeafProperty().getTypeInformation().getType();
		MyBatisContext parameter = new MyBatisContext(rootId, null, leafType, Collections.emptyMap());

		sqlSession().delete(statement, parameter);
	}

	@Override
	public void delete(Iterable<Object> rootIds, PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {
		rootIds.forEach(rootId -> delete(rootId, propertyPath));
	}

	@Override
	public <T> void deleteAll(Class<T> domainType) {

		String statement = namespace(domainType) + ".deleteAll";
		MyBatisContext parameter = new MyBatisContext(null, null, domainType, Collections.emptyMap());
		sqlSession().delete(statement, parameter);
	}

	@Override
	public void deleteAll(PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {

		Class<?> leafType = propertyPath.getLeafProperty().getTypeInformation().getType();

		String statement = namespace(getOwnerTyp(propertyPath)) + ".deleteAll-" + toDashPath(propertyPath);
		MyBatisContext parameter = new MyBatisContext(null, null, leafType, Collections.emptyMap());
		sqlSession().delete(statement, parameter);
	}

	@Override
	public void deleteByQuery(Query query, Class<?> domainType) {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public void deleteByQuery(Query query, PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public <T> void acquireLockById(Object id, LockMode lockMode, Class<T> domainType) {

		String statement = namespace(domainType) + ".acquireLockById";
		MyBatisContext parameter = new MyBatisContext(id, null, domainType, Collections.emptyMap());

		long result = sqlSession().selectOne(statement, parameter);
		if (result < 1) {

			String message = String.format("The lock target does not exist; id: %s, statement: %s", id, statement);
			throw new EmptyResultDataAccessException(message, 1);
		}
	}

	@Override
	public <T> void acquireLockAll(LockMode lockMode, Class<T> domainType) {

		String statement = namespace(domainType) + ".acquireLockAll";
		MyBatisContext parameter = new MyBatisContext(null, null, domainType, Collections.emptyMap());

		sqlSession().selectOne(statement, parameter);
	}

	@Override
	public <T> void acquireLockByQuery(Query query, LockMode lockMode, Class<T> domainType) {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public <T extends @Nullable Object> T findById(Object id, Class<T> domainType) {

		String statement = namespace(domainType) + ".findById";
		MyBatisContext parameter = new MyBatisContext(id, null, domainType, Collections.emptyMap());
		return sqlSession().selectOne(statement, parameter);
	}

	@Override
	public <T> List<T> findAll(Class<T> domainType) {

		String statement = namespace(domainType) + ".findAll";
		MyBatisContext parameter = new MyBatisContext(null, null, domainType, Collections.emptyMap());
		return sqlSession().selectList(statement, parameter);
	}

	@Override
	public <T> Stream<T> streamAll(Class<T> domainType) {

		String statement = namespace(domainType) + ".streamAll";
		MyBatisContext parameter = new MyBatisContext(null, null, domainType, Collections.emptyMap());
		Cursor<T> cursor = sqlSession().selectCursor(statement, parameter);
		return StreamSupport.stream(cursor.spliterator(), false);
	}

	@Override
	public <T> List<T> findAllById(Iterable<?> ids, Class<T> domainType) {

		return sqlSession().selectList(namespace(domainType) + ".findAllById",
				new MyBatisContext(ids, null, domainType, Collections.emptyMap()));
	}

	@Override
	public <T> Stream<T> streamAllByIds(Iterable<?> ids, Class<T> domainType) {

		String statement = namespace(domainType) + ".streamAllByIds";
		MyBatisContext parameter = new MyBatisContext(ids, null, domainType, Collections.emptyMap());
		Cursor<T> cursor = sqlSession().selectCursor(statement, parameter);
		return StreamSupport.stream(cursor.spliterator(), false);
	}

	@Override
	public List<Object> findAllByPath(Identifier identifier,
			PersistentPropertyPath<? extends RelationalPersistentProperty> path) {

		String statementName = namespace(getOwnerTyp(path)) + ".findAllByPath-" + path.toDotPath();

		return sqlSession().selectList(statementName,
				new MyBatisContext(identifier, null, path.getLeafProperty().getType()));
	}

	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {

		String statement = namespace(domainType) + ".existsById";
		MyBatisContext parameter = new MyBatisContext(id, null, domainType, Collections.emptyMap());
		return sqlSession().selectOne(statement, parameter);
	}

	@Override
	public <T> List<T> findAll(Class<T> domainType, Sort sort) {

		Map<String, Object> additionalContext = new HashMap<>();
		additionalContext.put("sort", sort);
		return sqlSession().selectList(namespace(domainType) + ".findAllSorted",
				new MyBatisContext(null, null, domainType, additionalContext));
	}

	@Override
	public <T> Stream<T> streamAll(Class<T> domainType, Sort sort) {

		Map<String, Object> additionalContext = new HashMap<>();
		additionalContext.put("sort", sort);

		String statement = namespace(domainType) + ".streamAllSorted";
		MyBatisContext parameter = new MyBatisContext(null, null, domainType, additionalContext);

		Cursor<T> cursor = sqlSession().selectCursor(statement, parameter);
		return StreamSupport.stream(cursor.spliterator(), false);
	}

	@Override
	public <T> List<T> findAll(Class<T> domainType, Pageable pageable) {

		Map<String, Object> additionalContext = new HashMap<>();
		additionalContext.put("pageable", pageable);
		return sqlSession().selectList(namespace(domainType) + ".findAllPaged",
				new MyBatisContext(null, null, domainType, additionalContext));
	}

	@Override
	public <T> Optional<T> findOne(Query query, Class<T> probeType) {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public <T> List<T> findAll(Query query, Class<T> probeType) {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public <T> Stream<T> streamAll(Query query, Class<T> probeType) {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public <T> List<T> findAll(Query query, Class<T> probeType, Pageable pageable) {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public <T> boolean exists(Query query, Class<T> probeType) {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public <T> long count(Query query, Class<T> probeType) {
		throw new UnsupportedOperationException("Not implemented");
	}

	@Override
	public long count(Class<?> domainType) {

		String statement = namespace(domainType) + ".count";
		MyBatisContext parameter = new MyBatisContext(null, null, domainType, Collections.emptyMap());
		return sqlSession().selectOne(statement, parameter);
	}

	private String namespace(Class<?> domainType) {
		return this.namespaceStrategy.getNamespace(domainType);
	}

	private SqlSession sqlSession() {
		return this.sqlSession;
	}

	private static String toDashPath(PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {
		return propertyPath.toDotPath().replaceAll("\\.", "-");
	}

	private Class<?> getOwnerTyp(PersistentPropertyPath<? extends RelationalPersistentProperty> propertyPath) {

		RelationalPersistentProperty baseProperty = propertyPath.getBaseProperty();

		Assert.notNull(baseProperty, "BaseProperty must not be null");

		return baseProperty.getOwner().getType();
	}
}
