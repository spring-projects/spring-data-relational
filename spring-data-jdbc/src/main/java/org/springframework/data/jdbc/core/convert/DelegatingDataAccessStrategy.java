/*
 * Copyright 2017-2024 the original author or authors.
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

import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.conversion.IdValueSource;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.util.Assert;

/**
 * Delegates all method calls to an instance set after construction. This is useful for {@link DataAccessStrategy}s with
 * cyclic dependencies.
 *
 * @author Jens Schauder
 * @author Tyler Van Gorder
 * @author Milan Milanov
 * @author Myeonghyeon Lee
 * @author Chirag Tailor
 * @author Diego Krupitza
 * @since 1.1
 */
public class DelegatingDataAccessStrategy implements DataAccessStrategy {

	private DataAccessStrategy delegate;

	public DelegatingDataAccessStrategy() {}

	public DelegatingDataAccessStrategy(DataAccessStrategy delegate) {

		Assert.notNull(delegate, "DataAccessStrategy must not be null");
		this.delegate = delegate;
	}

	@Override
	public <T> Object insert(T instance, Class<T> domainType, Identifier identifier, IdValueSource idValueSource) {
		return delegate.insert(instance, domainType, identifier, idValueSource);
	}

	@Override
	public <T> Object[] insert(List<InsertSubject<T>> insertSubjects, Class<T> domainType, IdValueSource idValueSource) {
		return delegate.insert(insertSubjects, domainType, idValueSource);
	}

	@Override
	public <S> boolean update(S instance, Class<S> domainType) {
		return delegate.update(instance, domainType);
	}

	@Override
	public <S> boolean updateWithVersion(S instance, Class<S> domainType, Number nextVersion) {
		return delegate.updateWithVersion(instance, domainType, nextVersion);

	}

	@Override
	public void delete(Object rootId, PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {
		delegate.delete(rootId, propertyPath);
	}

	@Override
	public void delete(Iterable<Object> rootIds, PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {
		delegate.delete(rootIds, propertyPath);
	}

	@Override
	public void delete(Object id, Class<?> domainType) {
		delegate.delete(id, domainType);
	}

	@Override
	public void delete(Iterable<Object> ids, Class<?> domainType) {
		delegate.delete(ids, domainType);
	}

	@Override
	public <T> void deleteWithVersion(Object id, Class<T> domainType, Number previousVersion) {
		delegate.deleteWithVersion(id, domainType, previousVersion);
	}

	@Override
	public <T> void deleteAll(Class<T> domainType) {
		delegate.deleteAll(domainType);
	}

	@Override
	public void deleteAll(PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {
		delegate.deleteAll(propertyPath);
	}

	@Override
	public <T> void acquireLockById(Object id, LockMode lockMode, Class<T> domainType) {
		delegate.acquireLockById(id, lockMode, domainType);
	}

	@Override
	public <T> void acquireLockAll(LockMode lockMode, Class<T> domainType) {
		delegate.acquireLockAll(lockMode, domainType);
	}

	@Override
	public long count(Class<?> domainType) {
		return delegate.count(domainType);
	}

	@Override
	public <T> T findById(Object id, Class<T> domainType) {

		Assert.notNull(delegate, "Delegate is null");

		return delegate.findById(id, domainType);
	}

	@Override
	public <T> Iterable<T> findAll(Class<T> domainType) {
		return delegate.findAll(domainType);
	}

	@Override
	public <T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType) {
		return delegate.findAllById(ids, domainType);
	}

	@Override
	public Iterable<Object> findAllByPath(Identifier identifier,
			PersistentPropertyPath<? extends RelationalPersistentProperty> path) {
		return delegate.findAllByPath(identifier, path);
	}

	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {
		return delegate.existsById(id, domainType);
	}

	@Override
	public <T> Iterable<T> findAll(Class<T> domainType, Sort sort) {
		return delegate.findAll(domainType, sort);
	}

	@Override
	public <T> Iterable<T> findAll(Class<T> domainType, Pageable pageable) {
		return delegate.findAll(domainType, pageable);
	}

	@Override
	public <T> Optional<T> findOne(Query query, Class<T> domainType) {
		return delegate.findOne(query, domainType);
	}

	@Override
	public <T> Iterable<T> findAll(Query query, Class<T> domainType) {
		return delegate.findAll(query, domainType);
	}

	@Override
	public <T> Iterable<T> findAll(Query query, Class<T> domainType, Pageable pageable) {
		return delegate.findAll(query, domainType, pageable);
	}

	@Override
	public <T> boolean exists(Query query, Class<T> domainType) {
		return delegate.exists(query, domainType);
	}

	@Override
	public <T> long count(Query query, Class<T> domainType) {
		return delegate.count(query, domainType);
	}

	/**
	 * Must be called exactly once before calling any of the other methods.
	 *
	 * @param delegate Must not be {@literal null}
	 * @deprecated since 3.0, use {@link #DelegatingDataAccessStrategy(DataAccessStrategy)} to avoid mutable state.
	 */
	@Deprecated(since = "3.0", forRemoval = true)
	public void setDelegate(DataAccessStrategy delegate) {

		Assert.isNull(this.delegate, "The delegate must be set exactly once");
		Assert.notNull(delegate, "The delegate must not be set to null");

		this.delegate = delegate;
	}
}
