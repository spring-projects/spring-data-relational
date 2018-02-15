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

import org.apache.ibatis.session.SqlSession;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.data.jdbc.core.DataAccessStrategy;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentProperty;
import org.springframework.data.mapping.PropertyPath;

import java.util.Collections;
import java.util.Map;

/**
 * {@link DataAccessStrategy} implementation based on MyBatis. Each method gets mapped to a statement. The name of the
 * statement gets constructed as follows: By default, the namespace is based on the class of the entity plus the suffix "Mapper".
 * This is then followed by the method name separated by a dot. For methods taking a {@link PropertyPath} as argument,
 * the relevant entity is that of the root of the path, and the path itself gets as dot separated String appended to the
 * statement name. Each statement gets an instance of {@link MyBatisContext}, which at least has the entityType set. For
 * methods taking a {@link PropertyPath} the entityTyoe if the context is set to the class of the leaf type.
 *
 * @author Jens Schauder
 * @author Kazuki Shimizu
 */
public class MyBatisDataAccessStrategy implements DataAccessStrategy {

	private final SqlSession sqlSession;
	private MyBatisNamingStrategy namingStrategy = new MyBatisNamingStrategy() {};

	/**
	 * Constructs a {@link DataAccessStrategy} based on MyBatis.
	 * <p>
	 * Use a {@link SqlSessionTemplate} for {@link SqlSession} or a similar implementation tying the session to the
	 * proper transaction.
	 *
	 * @param sqlSession Must be non {@literal null}.
	 */
	public MyBatisDataAccessStrategy(SqlSession sqlSession) {
		this.sqlSession = sqlSession;
	}

	/**
	 * Set a naming strategy for MyBatis objects.
	 * @param namingStrategy Must be non {@literal null}
	 */
	public void setNamingStrategy(MyBatisNamingStrategy namingStrategy) {
		this.namingStrategy = namingStrategy;
	}

	@Override
	public <T> void insert(T instance, Class<T> domainType, Map<String, Object> additionalParameters) {
		sqlSession().insert(namespace(domainType) + ".insert",
				new MyBatisContext(null, instance, domainType, additionalParameters));
	}

	@Override
	public <S> void update(S instance, Class<S> domainType) {

		sqlSession().update(namespace(domainType) + ".update",
				new MyBatisContext(null, instance, domainType, Collections.emptyMap()));
	}

	@Override
	public void delete(Object id, Class<?> domainType) {

		sqlSession().delete(namespace(domainType) + ".delete",
				new MyBatisContext(id, null, domainType, Collections.emptyMap()));
	}

	@Override
	public void delete(Object rootId, PropertyPath propertyPath) {

		sqlSession().delete(namespace(propertyPath.getOwningType().getType()) + ".delete-" + toDashPath(propertyPath),
				new MyBatisContext(rootId, null, propertyPath.getLeafProperty().getTypeInformation().getType(),
						Collections.emptyMap()));
	}

	@Override
	public <T> void deleteAll(Class<T> domainType) {

		sqlSession().delete( //
				namespace(domainType) + ".deleteAll", //
				new MyBatisContext(null, null, domainType, Collections.emptyMap()) //
		);
	}

	@Override
	public <T> void deleteAll(PropertyPath propertyPath) {

		Class baseType = propertyPath.getOwningType().getType();
		Class leaveType = propertyPath.getLeafProperty().getTypeInformation().getType();

		sqlSession().delete( //
				namespace(baseType) + ".deleteAll-" + toDashPath(propertyPath), //
				new MyBatisContext(null, null, leaveType, Collections.emptyMap()) //
		);
	}

	@Override
	public <T> T findById(Object id, Class<T> domainType) {
		return sqlSession().selectOne(namespace(domainType) + ".findById",
				new MyBatisContext(id, null, domainType, Collections.emptyMap()));
	}

	@Override
	public <T> Iterable<T> findAll(Class<T> domainType) {
		return sqlSession().selectList(namespace(domainType) + ".findAll",
				new MyBatisContext(null, null, domainType, Collections.emptyMap()));
	}

	@Override
	public <T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType) {
		return sqlSession().selectList(namespace(domainType) + ".findAllById",
				new MyBatisContext(ids, null, domainType, Collections.emptyMap()));
	}

	@Override
	public <T> Iterable<T> findAllByProperty(Object rootId, JdbcPersistentProperty property) {
		return sqlSession().selectList(namespace(property.getOwner().getType()) + ".findAllByProperty-" + property.getName(),
				new MyBatisContext(rootId, null, property.getType(), Collections.emptyMap()));
	}

	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {
		return sqlSession().selectOne(namespace(domainType) + ".existsById",
				new MyBatisContext(id, null, domainType, Collections.emptyMap()));
	}

	@Override
	public long count(Class<?> domainType) {
		return sqlSession().selectOne(namespace(domainType) + ".count",
				new MyBatisContext(null, null, domainType, Collections.emptyMap()));
	}

	private String namespace(Class<?> domainType) {
		return this.namingStrategy.getNamespace(domainType);
	}

	private SqlSession sqlSession() {
		return this.sqlSession;
	}

	private String toDashPath(PropertyPath propertyPath) {
		return propertyPath.toDotPath().replaceAll("\\.", "-");
	}
}
