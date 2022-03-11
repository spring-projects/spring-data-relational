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
package org.springframework.data.jdbc.core.convert;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.sql.LockMode;

/**
 * Delegates each methods to the {@link DataAccessStrategy}s passed to the constructor in turn until the first that does
 * not throw an exception.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Tyler Van Gorder
 * @author Milan Milanov
 * @author Myeonghyeon Lee
 * @author Diego Krupitza
 * @since 1.1
 */
public class CascadingDataAccessStrategy implements DataAccessStrategy {

	private final List<DataAccessStrategy> strategies;

	public CascadingDataAccessStrategy(List<DataAccessStrategy> strategies) {
		this.strategies = new ArrayList<>(strategies);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#insert(java.lang.Object, java.lang.Class, org.springframework.data.jdbc.core.ParentKeys)
	 */
	@Override
	public <T> Object insert(T instance, Class<T> domainType, Identifier identifier) {
		return collect(das -> das.insert(instance, domainType, identifier));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#update(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <S> boolean update(S instance, Class<S> domainType) {
		return collect(das -> das.update(instance, domainType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#updateWithVersion(java.lang.Object, java.lang.Class, java.lang.Number)
	 */
	@Override
	public <S> boolean updateWithVersion(S instance, Class<S> domainType, Number previousVersion) {
		return collect(das -> das.updateWithVersion(instance, domainType, previousVersion));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#delete(java.lang.Object, java.lang.Class)
	 */
	@Override
	public void delete(Object id, Class<?> domainType) {
		collectVoid(das -> das.delete(id, domainType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#deleteInstance(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> void deleteWithVersion(Object id, Class<T> domainType, Number previousVersion) {
		collectVoid(das -> das.deleteWithVersion(id, domainType, previousVersion));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#delete(java.lang.Object, org.springframework.data.mapping.PersistentPropertyPath)
	 */
	@Override
	public void delete(Object rootId, PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {
		collectVoid(das -> das.delete(rootId, propertyPath));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#deleteAll(java.lang.Class)
	 */
	@Override
	public <T> void deleteAll(Class<T> domainType) {
		collectVoid(das -> das.deleteAll(domainType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#deleteAll(org.springframework.data.mapping.PersistentPropertyPath)
	 */
	@Override
	public void deleteAll(PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {
		collectVoid(das -> das.deleteAll(propertyPath));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#acquireLockById(java.lang.Object, org.springframework.data.relational.core.sql.LockMode, java.lang.Class)
	 */
	@Override
	public <T> void acquireLockById(Object id, LockMode lockMode, Class<T> domainType) {
		collectVoid(das -> das.acquireLockById(id, lockMode, domainType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#acquireLockAll(org.springframework.data.relational.core.sql.LockMode, java.lang.Class)
	 */
	@Override
	public <T> void acquireLockAll(LockMode lockMode, Class<T> domainType) {
		collectVoid(das -> das.acquireLockAll(lockMode, domainType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#count(java.lang.Class)
	 */
	@Override
	public long count(Class<?> domainType) {
		return collect(das -> das.count(domainType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#findById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> T findById(Object id, Class<T> domainType) {
		return collect(das -> das.findById(id, domainType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#findAll(java.lang.Class)
	 */
	@Override
	public <T> Iterable<T> findAll(Class<T> domainType) {
		return collect(das -> das.findAll(domainType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#findAllById(java.lang.Iterable, java.lang.Class)
	 */
	@Override
	public <T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType) {
		return collect(das -> das.findAllById(ids, domainType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.RelationResolver#findAllByPath(org.springframework.data.jdbc.support.Identifier, org.springframework.data.mapping.PersistentPropertyPath)
	 */
	@Override
	public Iterable<Object> findAllByPath(Identifier identifier,
			PersistentPropertyPath<? extends RelationalPersistentProperty> path) {
		return collect(das -> das.findAllByPath(identifier, path));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#existsById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {
		return collect(das -> das.existsById(id, domainType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#findAll(java.lang.Class, org.springframework.data.domain.Sort)
	 */
	@Override
	public <T> Iterable<T> findAll(Class<T> domainType, Sort sort) {
		return collect(das -> das.findAll(domainType, sort));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#findAll(java.lang.Class, org.springframework.data.domain.Pageable)
	 */
	@Override
	public <T> Iterable<T> findAll(Class<T> domainType, Pageable pageable) {
		return collect(das -> das.findAll(domainType, pageable));
	}

	@Override
	public <T> Optional<T> selectOne(Query query, Class<T> probeType) {
		return collect(das -> das.selectOne(query, probeType));
	}

	private <T> T collect(Function<DataAccessStrategy, T> function) {

		// Keep <T> as Eclipse fails to compile if <> is used.
		return strategies.stream().collect(new FunctionCollector<>(function));
	}

	private void collectVoid(Consumer<DataAccessStrategy> consumer) {

		collect(das -> {
			consumer.accept(das);
			return null;
		});
	}
}
