/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Map;

import org.apache.ibatis.session.SqlSession;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.data.jdbc.core.CascadingDataAccessStrategy;
import org.springframework.data.jdbc.core.DataAccessStrategy;
import org.springframework.data.jdbc.core.DefaultDataAccessStrategy;
import org.springframework.data.jdbc.core.DelegatingDataAccessStrategy;
import org.springframework.data.jdbc.core.SqlGeneratorSource;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
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
 */
public class MyBatisDataAccessStrategy implements DataAccessStrategy {

	private final SqlSession sqlSession;
	private NamespaceStrategy namespaceStrategy = NamespaceStrategy.DEFAULT_INSTANCE;

	/**
	 * Create a {@link DataAccessStrategy} that first checks for queries defined by MyBatis and if it doesn't find one
	 * uses a {@link DefaultDataAccessStrategy}
	 */
	public static DataAccessStrategy createCombinedAccessStrategy(RelationalMappingContext context,
			RelationalConverter converter, NamedParameterJdbcOperations operations, SqlSession sqlSession) {
		return createCombinedAccessStrategy(context, converter, operations, sqlSession, NamespaceStrategy.DEFAULT_INSTANCE);
	}

	/**
	 * Create a {@link DataAccessStrategy} that first checks for queries defined by MyBatis and if it doesn't find one
	 * uses a {@link DefaultDataAccessStrategy}
	 */
	public static DataAccessStrategy createCombinedAccessStrategy(RelationalMappingContext context,
			RelationalConverter converter, NamedParameterJdbcOperations operations, SqlSession sqlSession,
			NamespaceStrategy namespaceStrategy) {

		// the DefaultDataAccessStrategy needs a reference to the returned DataAccessStrategy. This creates a dependency
		// cycle. In order to create it, we need something that allows to defer closing the cycle until all the elements are
		// created. That is the purpose of the DelegatingAccessStrategy.
		DelegatingDataAccessStrategy delegatingDataAccessStrategy = new DelegatingDataAccessStrategy();
		MyBatisDataAccessStrategy myBatisDataAccessStrategy = new MyBatisDataAccessStrategy(sqlSession);
		myBatisDataAccessStrategy.setNamespaceStrategy(namespaceStrategy);

		CascadingDataAccessStrategy cascadingDataAccessStrategy = new CascadingDataAccessStrategy(
				asList(myBatisDataAccessStrategy, delegatingDataAccessStrategy));

		SqlGeneratorSource sqlGeneratorSource = new SqlGeneratorSource(context);
		DefaultDataAccessStrategy defaultDataAccessStrategy = new DefaultDataAccessStrategy( //
				sqlGeneratorSource, //
				context, //
				converter, //
				operations, //
				cascadingDataAccessStrategy //
		);

		delegatingDataAccessStrategy.setDelegate(defaultDataAccessStrategy);

		return cascadingDataAccessStrategy;
	}

	/**
	 * Constructs a {@link DataAccessStrategy} based on MyBatis.
	 * <p>
	 * Use a {@link SqlSessionTemplate} for {@link SqlSession} or a similar implementation tying the session to the proper
	 * transaction. Note that the resulting {@link DataAccessStrategy} only handles MyBatis. It does not include the
	 * functionality of the {@link org.springframework.data.jdbc.core.DefaultDataAccessStrategy} which one normally still
	 * wants. Use
	 * {@link #createCombinedAccessStrategy(RelationalMappingContext, RelationalConverter, NamedParameterJdbcOperations, SqlSession, NamespaceStrategy)}
	 * to create such a {@link DataAccessStrategy}.
	 *
	 * @param sqlSession Must be non {@literal null}.
	 */
	public MyBatisDataAccessStrategy(SqlSession sqlSession) {
		this.sqlSession = sqlSession;
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
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#insert(java.lang.Object, java.lang.Class, java.util.Map)
	 */
	@Override
	public <T> Object insert(T instance, Class<T> domainType, Map<String, Object> additionalParameters) {

		MyBatisContext myBatisContext = new MyBatisContext(null, instance, domainType, additionalParameters);
		sqlSession().insert(namespace(domainType) + ".insert",
				myBatisContext);

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
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#delete(java.lang.Object, java.lang.Class)
	 */
	@Override
	public void delete(Object id, Class<?> domainType) {

		sqlSession().delete(namespace(domainType) + ".delete",
				new MyBatisContext(id, null, domainType, Collections.emptyMap()));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#delete(java.lang.Object, org.springframework.data.mapping.PersistentPropertyPath)
	 */
	@Override
	public void delete(Object rootId, PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {

		sqlSession().delete(
				namespace(propertyPath.getBaseProperty().getOwner().getType()) + ".delete-" + toDashPath(propertyPath),
				new MyBatisContext(rootId, null, propertyPath.getRequiredLeafProperty().getTypeInformation().getType(),
						Collections.emptyMap()));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#deleteAll(java.lang.Class)
	 */
	@Override
	public <T> void deleteAll(Class<T> domainType) {

		sqlSession().delete( //
				namespace(domainType) + ".deleteAll", //
				new MyBatisContext(null, null, domainType, Collections.emptyMap()) //
		);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#deleteAll(org.springframework.data.mapping.PersistentPropertyPath)
	 */
	@Override
	public void deleteAll(PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {

		Class<?> baseType = propertyPath.getBaseProperty().getOwner().getType();
		Class<?> leafType = propertyPath.getRequiredLeafProperty().getTypeInformation().getType();

		sqlSession().delete( //
				namespace(baseType) + ".deleteAll-" + toDashPath(propertyPath), //
				new MyBatisContext(null, null, leafType, Collections.emptyMap()) //
		);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#findById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> T findById(Object id, Class<T> domainType) {
		return sqlSession().selectOne(namespace(domainType) + ".findById",
				new MyBatisContext(id, null, domainType, Collections.emptyMap()));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#findAll(java.lang.Class)
	 */
	@Override
	public <T> Iterable<T> findAll(Class<T> domainType) {
		return sqlSession().selectList(namespace(domainType) + ".findAll",
				new MyBatisContext(null, null, domainType, Collections.emptyMap()));
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

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#findAllByProperty(java.lang.Object, org.springframework.data.relational.core.mapping.RelationalPersistentProperty)
	 */
	@Override
	public <T> Iterable<T> findAllByProperty(Object rootId, RelationalPersistentProperty property) {
		return sqlSession().selectList(
				namespace(property.getOwner().getType()) + ".findAllByProperty-" + property.getName(),
				new MyBatisContext(rootId, null, property.getType(), Collections.emptyMap()));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#existsById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {
		return sqlSession().selectOne(namespace(domainType) + ".existsById",
				new MyBatisContext(id, null, domainType, Collections.emptyMap()));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#count(java.lang.Class)
	 */
	@Override
	public long count(Class<?> domainType) {
		return sqlSession().selectOne(namespace(domainType) + ".count",
				new MyBatisContext(null, null, domainType, Collections.emptyMap()));
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
