/*
 * Copyright 2017-2021 the original author or authors.
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
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.ibatis.session.SqlSession;
import org.mybatis.spring.SqlSessionTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.CascadingDataAccessStrategy;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.DefaultDataAccessStrategy;
import org.springframework.data.jdbc.core.convert.DelegatingDataAccessStrategy;
import org.springframework.data.jdbc.core.convert.Identifier;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.SqlGeneratorSource;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
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
 */
public class MyBatisDataAccessStrategy implements DataAccessStrategy {

	private static final Logger LOG = LoggerFactory.getLogger(MyBatisDataAccessStrategy.class);
	private static final String VERSION_SQL_PARAMETER_NAME_OLD = "___oldOptimisticLockingVersion";

	private final SqlSession sqlSession;
	private final IdentifierProcessing identifierProcessing;
	private NamespaceStrategy namespaceStrategy = NamespaceStrategy.DEFAULT_INSTANCE;

	/**
	 * Create a {@link DataAccessStrategy} that first checks for queries defined by MyBatis and if it doesn't find one
	 * uses a {@link DefaultDataAccessStrategy}
	 */
	public static DataAccessStrategy createCombinedAccessStrategy(RelationalMappingContext context,
			JdbcConverter converter, NamedParameterJdbcOperations operations, SqlSession sqlSession, Dialect dialect) {
		return createCombinedAccessStrategy(context, converter, operations, sqlSession, NamespaceStrategy.DEFAULT_INSTANCE,
				dialect);
	}

	/**
	 * Create a {@link DataAccessStrategy} that first checks for queries defined by MyBatis and if it doesn't find one
	 * uses a {@link DefaultDataAccessStrategy}
	 */
	public static DataAccessStrategy createCombinedAccessStrategy(RelationalMappingContext context,
			JdbcConverter converter, NamedParameterJdbcOperations operations, SqlSession sqlSession,
			NamespaceStrategy namespaceStrategy, Dialect dialect) {

		// the DefaultDataAccessStrategy needs a reference to the returned DataAccessStrategy. This creates a dependency
		// cycle. In order to create it, we need something that allows to defer closing the cycle until all the elements are
		// created. That is the purpose of the DelegatingAccessStrategy.
		DelegatingDataAccessStrategy delegatingDataAccessStrategy = new DelegatingDataAccessStrategy();
		MyBatisDataAccessStrategy myBatisDataAccessStrategy = new MyBatisDataAccessStrategy(sqlSession,
				dialect.getIdentifierProcessing());
		myBatisDataAccessStrategy.setNamespaceStrategy(namespaceStrategy);

		CascadingDataAccessStrategy cascadingDataAccessStrategy = new CascadingDataAccessStrategy(
				asList(myBatisDataAccessStrategy, delegatingDataAccessStrategy));

		SqlGeneratorSource sqlGeneratorSource = new SqlGeneratorSource(context, converter, dialect);
		DefaultDataAccessStrategy defaultDataAccessStrategy = new DefaultDataAccessStrategy( //
				sqlGeneratorSource, //
				context, //
				converter, //
				operations //
		);

		delegatingDataAccessStrategy.setDelegate(defaultDataAccessStrategy);

		return cascadingDataAccessStrategy;
	}

	/**
	 * Constructs a {@link DataAccessStrategy} based on MyBatis.
	 * <p>
	 * Use a {@link SqlSessionTemplate} for {@link SqlSession} or a similar implementation tying the session to the proper
	 * transaction. Note that the resulting {@link DataAccessStrategy} only handles MyBatis. It does not include the
	 * functionality of the {@link DefaultDataAccessStrategy} which one normally still wants. Use
	 * {@link #createCombinedAccessStrategy(RelationalMappingContext, JdbcConverter, NamedParameterJdbcOperations, SqlSession, NamespaceStrategy, Dialect)}
	 * to create such a {@link DataAccessStrategy}.
	 *
	 * @param sqlSession Must be non {@literal null}.
	 * @param identifierProcessing the {@link IdentifierProcessing} applied to {@link SqlIdentifier} instances in order to
	 *          turn them into {@link String}
	 */
	public MyBatisDataAccessStrategy(SqlSession sqlSession, IdentifierProcessing identifierProcessing) {

		this.sqlSession = sqlSession;
		this.identifierProcessing = identifierProcessing;
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#insert(java.lang.Object, java.lang.Class, ParentKeys)
	 */
	@Override
	public <T> Object insert(T instance, Class<T> domainType, Identifier identifier) {

		MyBatisContext myBatisContext = new MyBatisContext(identifier, instance, domainType);
		sqlSession().insert(namespace(domainType) + ".insert", myBatisContext);

		return myBatisContext.getId();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#update(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <S> boolean update(S instance, Class<S> domainType) {

		return sqlSession().update(namespace(domainType) + ".update",
				new MyBatisContext(null, instance, domainType, Collections.emptyMap())) != 0;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#updateWithVersion(java.lang.Object, java.lang.Class, java.lang.Number)
	 */
	@Override
	public <S> boolean updateWithVersion(S instance, Class<S> domainType, Number previousVersion) {

		String statement = namespace(domainType) + ".updateWithVersion";
		MyBatisContext parameter = new MyBatisContext(null, instance, domainType,
				Collections.singletonMap(VERSION_SQL_PARAMETER_NAME_OLD, previousVersion));
		return sqlSession().update(statement, parameter) != 0;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#delete(java.lang.Object, java.lang.Class)
	 */
	@Override
	public void delete(Object id, Class<?> domainType) {

		String statement = namespace(domainType) + ".delete";
		MyBatisContext parameter = new MyBatisContext(id, null, domainType, Collections.emptyMap());
		sqlSession().delete(statement, parameter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#deleteInstance(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> void deleteWithVersion(Object id, Class<T> domainType, Number previousVersion) {

		String statement = namespace(domainType) + ".deleteWithVersion";
		MyBatisContext parameter = new MyBatisContext(id, null, domainType,
				Collections.singletonMap(VERSION_SQL_PARAMETER_NAME_OLD, previousVersion));
		sqlSession().delete(statement, parameter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#delete(java.lang.Object, org.springframework.data.mapping.PersistentPropertyPath)
	 */
	@Override
	public void delete(Object rootId, PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {

		Class<?> ownerType = propertyPath.getBaseProperty().getOwner().getType();
		String statement = namespace(ownerType) + ".delete-" + toDashPath(propertyPath);
		Class<?> leafType = propertyPath.getRequiredLeafProperty().getTypeInformation().getType();
		MyBatisContext parameter = new MyBatisContext(rootId, null, leafType, Collections.emptyMap());

		sqlSession().delete(statement, parameter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#deleteAll(java.lang.Class)
	 */
	@Override
	public <T> void deleteAll(Class<T> domainType) {

		String statement = namespace(domainType) + ".deleteAll";
		MyBatisContext parameter = new MyBatisContext(null, null, domainType, Collections.emptyMap());
		sqlSession().delete(statement, parameter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#deleteAll(org.springframework.data.mapping.PersistentPropertyPath)
	 */
	@Override
	public void deleteAll(PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {

		Class<?> baseType = propertyPath.getBaseProperty().getOwner().getType();
		Class<?> leafType = propertyPath.getRequiredLeafProperty().getTypeInformation().getType();

		String statement = namespace(baseType) + ".deleteAll-" + toDashPath(propertyPath);
		MyBatisContext parameter = new MyBatisContext(null, null, leafType, Collections.emptyMap());
		sqlSession().delete(statement, parameter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#acquireLockById(java.lang.Object, org.springframework.data.relational.core.sql.LockMode, java.lang.Class)
	 */
	@Override
	public <T> void acquireLockById(Object id, LockMode lockMode, Class<T> domainType) {

		String statement = namespace(domainType) + ".acquireLockById";
		MyBatisContext parameter = new MyBatisContext(id, null, domainType, Collections.emptyMap());

		long result = sqlSession().selectOne(statement, parameter);
		if (result < 1) {

			String message = String.format("The lock target does not exist. id: %s, statement: %s", id, statement);
			throw new EmptyResultDataAccessException(message, 1);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#acquireLockAll(org.springframework.data.relational.core.sql.LockMode, java.lang.Class)
	 */
	@Override
	public <T> void acquireLockAll(LockMode lockMode, Class<T> domainType) {

		String statement = namespace(domainType) + ".acquireLockAll";
		MyBatisContext parameter = new MyBatisContext(null, null, domainType, Collections.emptyMap());

		sqlSession().selectOne(statement, parameter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#findById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> T findById(Object id, Class<T> domainType) {

		String statement = namespace(domainType) + ".findById";
		MyBatisContext parameter = new MyBatisContext(id, null, domainType, Collections.emptyMap());
		return sqlSession().selectOne(statement, parameter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#findAll(java.lang.Class)
	 */
	@Override
	public <T> Iterable<T> findAll(Class<T> domainType) {

		String statement = namespace(domainType) + ".findAll";
		MyBatisContext parameter = new MyBatisContext(null, null, domainType, Collections.emptyMap());
		return sqlSession().selectList(statement, parameter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#findAllById(java.lang.Iterable, java.lang.Class)
	 */
	@Override
	public <T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType) {
		return sqlSession().selectList(namespace(domainType) + ".findAllById",
				new MyBatisContext(ids, null, domainType, Collections.emptyMap()));
	}

	@Override
	public Iterable<Object> findAllByPath(Identifier identifier,
			PersistentPropertyPath<? extends RelationalPersistentProperty> path) {

		String statementName = namespace(path.getBaseProperty().getOwner().getType()) + ".findAllByPath-"
				+ path.toDotPath();

		return sqlSession().selectList(statementName,
				new MyBatisContext(identifier, null, path.getRequiredLeafProperty().getType()));

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#existsById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {

		String statement = namespace(domainType) + ".existsById";
		MyBatisContext parameter = new MyBatisContext(id, null, domainType, Collections.emptyMap());
		return sqlSession().selectOne(statement, parameter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#findAll(java.lang.Class, org.springframework.data.domain.Sort)
	 */
	@Override
	public <T> Iterable<T> findAll(Class<T> domainType, Sort sort) {

		Map<String, Object> additionalContext = new HashMap<>();
		additionalContext.put("sort", sort);
		return sqlSession().selectList(namespace(domainType) + ".findAllSorted",
				new MyBatisContext(null, null, domainType, additionalContext));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#findAll(java.lang.Class, org.springframework.data.domain.Pageable)
	 */
	@Override
	public <T> Iterable<T> findAll(Class<T> domainType, Pageable pageable) {

		Map<String, Object> additionalContext = new HashMap<>();
		additionalContext.put("pageable", pageable);
		return sqlSession().selectList(namespace(domainType) + ".findAllPaged",
				new MyBatisContext(null, null, domainType, additionalContext));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#count(java.lang.Class)
	 */
	@Override
	public long count(Class<?> domainType) {

		String statement = namespace(domainType) + ".count";
		MyBatisContext parameter = new MyBatisContext(null, null, domainType, Collections.emptyMap());
		return sqlSession().selectOne(statement, parameter);
	}

	private Map<String, Object> convertToParameterMap(Map<SqlIdentifier, Object> additionalParameters) {

		return additionalParameters.entrySet().stream() //
				.collect(Collectors.toMap(e -> e.getKey().toSql(identifierProcessing), Map.Entry::getValue));
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
}
