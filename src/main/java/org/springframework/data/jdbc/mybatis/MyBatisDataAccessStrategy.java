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
import org.springframework.data.jdbc.mapping.model.JdbcMappingContext;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentProperty;
import org.springframework.data.mapping.PropertyPath;

/**
 * {@link DataAccessStrategy} implementation based on MyBatis. Each method gets mapped to a statement. The name of the
 * statement gets constructed as follows: The namespace is based on the class of the entity plus the suffix "Mapper".
 * This is then followed by the method name separated by a dot. For methods taking a {@link PropertyPath} as argument,
 * the relevant entity is that of the root of the path, and the path itself gets as dot separated String appended to the
 * statement name. Each statement gets an instance of {@link MyBatisContext}, which at least has the entityType set. For
 * methods taking a {@link PropertyPath} the entityTyoe if the context is set to the class of the leaf type.
 *
 * @author Jens Schauder
 * @author Kazuki Shimizu
 */
public class MyBatisDataAccessStrategy implements DataAccessStrategy {

	private static final String MAPPER_SUFFIX = "Mapper";

	private final SqlSession sqlSession;

	/**
	 * Create a {@link DataAccessStrategy} that first checks for queries defined by MyBatis and if it doesn't find one
	 * used a {@link DefaultDataAccessStrategy}
	 * 
	 * @param context
	 * @param sqlSession
	 * @return
	 */
	public static DataAccessStrategy createCombinedAccessStrategy(JdbcMappingContext context, SqlSession sqlSession) {

		// the DefaultDataAccessStrategy needs a reference to the returned DataAccessStrategy. This creates a dependency
		// cycle. In order to create it, we need something that allows to defer closing the cycle until all the elements are
		// created. That is the purpose of the DelegatingAccessStrategy.
		DelegatingDataAccessStrategy delegatingDataAccessStrategy = new DelegatingDataAccessStrategy();
		MyBatisDataAccessStrategy myBatisDataAccessStrategy = new MyBatisDataAccessStrategy(sqlSession);

		CascadingDataAccessStrategy cascadingDataAccessStrategy = new CascadingDataAccessStrategy(
				asList(myBatisDataAccessStrategy, delegatingDataAccessStrategy));

		DefaultDataAccessStrategy defaultDataAccessStrategy = new DefaultDataAccessStrategy( //
				new SqlGeneratorSource(context), //
				context, //
				cascadingDataAccessStrategy);

		delegatingDataAccessStrategy.setDelegate(defaultDataAccessStrategy);

		return cascadingDataAccessStrategy;
	}

	/**
	 * Constructs a {@link DataAccessStrategy} based on MyBatis.
	 * <p>
	 * Use a {@link SqlSessionTemplate} for {@link SqlSession} or a similar implementation tying the session to the proper
	 * transaction. Note that the resulting {@link DataAccessStrategy} only handles MyBatis. It does not include the
	 * functionality of the {@link org.springframework.data.jdbc.core.DefaultDataAccessStrategy} which one normally still
	 * wants. Use {@link #createCombinedAccessStrategy(JdbcMappingContext, SqlSession)} to create such a
	 * {@link DataAccessStrategy}.
	 *
	 * @param sqlSession Must be non {@literal null}.
	 */
	public MyBatisDataAccessStrategy(SqlSession sqlSession) {
		this.sqlSession = sqlSession;
	}

	@Override
	public <T> void insert(T instance, Class<T> domainType, Map<String, Object> additionalParameters) {
		sqlSession().insert(mapper(domainType) + ".insert",
				new MyBatisContext(null, instance, domainType, additionalParameters));
	}

	@Override
	public <S> void update(S instance, Class<S> domainType) {

		sqlSession().update(mapper(domainType) + ".update",
				new MyBatisContext(null, instance, domainType, Collections.emptyMap()));
	}

	@Override
	public void delete(Object id, Class<?> domainType) {

		sqlSession().delete(mapper(domainType) + ".delete",
				new MyBatisContext(id, null, domainType, Collections.emptyMap()));
	}

	@Override
	public void delete(Object rootId, PropertyPath propertyPath) {

		sqlSession().delete(mapper(propertyPath.getOwningType().getType()) + ".delete-" + toDashPath(propertyPath),
				new MyBatisContext(rootId, null, propertyPath.getLeafProperty().getTypeInformation().getType(),
						Collections.emptyMap()));
	}

	@Override
	public <T> void deleteAll(Class<T> domainType) {

		sqlSession().delete( //
				mapper(domainType) + ".deleteAll", //
				new MyBatisContext(null, null, domainType, Collections.emptyMap()) //
		);
	}

	@Override
	public <T> void deleteAll(PropertyPath propertyPath) {

		Class baseType = propertyPath.getOwningType().getType();
		Class leaveType = propertyPath.getLeafProperty().getTypeInformation().getType();

		sqlSession().delete( //
				mapper(baseType) + ".deleteAll-" + toDashPath(propertyPath), //
				new MyBatisContext(null, null, leaveType, Collections.emptyMap()) //
		);
	}

	@Override
	public <T> T findById(Object id, Class<T> domainType) {
		return sqlSession().selectOne(mapper(domainType) + ".findById",
				new MyBatisContext(id, null, domainType, Collections.emptyMap()));
	}

	@Override
	public <T> Iterable<T> findAll(Class<T> domainType) {
		return sqlSession().selectList(mapper(domainType) + ".findAll",
				new MyBatisContext(null, null, domainType, Collections.emptyMap()));
	}

	@Override
	public <T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType) {
		return sqlSession().selectList(mapper(domainType) + ".findAllById",
				new MyBatisContext(ids, null, domainType, Collections.emptyMap()));
	}

	@Override
	public <T> Iterable<T> findAllByProperty(Object rootId, JdbcPersistentProperty property) {
		return sqlSession().selectList(mapper(property.getOwner().getType()) + ".findAllByProperty-" + property.getName(),
				new MyBatisContext(rootId, null, property.getType(), Collections.emptyMap()));
	}

	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {
		return sqlSession().selectOne(mapper(domainType) + ".existsById",
				new MyBatisContext(id, null, domainType, Collections.emptyMap()));
	}

	@Override
	public long count(Class<?> domainType) {
		return sqlSession().selectOne(mapper(domainType) + ".count",
				new MyBatisContext(null, null, domainType, Collections.emptyMap()));
	}

	private String mapper(Class<?> domainType) {
		return domainType.getName() + MAPPER_SUFFIX;
	}

	private SqlSession sqlSession() {
		return this.sqlSession;
	}

	private String toDashPath(PropertyPath propertyPath) {
		return propertyPath.toDotPath().replaceAll("\\.", "-");
	}
}
