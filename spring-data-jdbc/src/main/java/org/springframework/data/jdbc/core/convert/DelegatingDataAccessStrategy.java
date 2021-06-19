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

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
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
 * @since 1.1
 */
public class DelegatingDataAccessStrategy implements DataAccessStrategy {

	private DataAccessStrategy delegate;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#insert(java.lang.Object, java.lang.Class, org.springframework.data.jdbc.core.ParentKeys)
	 */
	@Override
	public <T> Object insert(T instance, Class<T> domainType, Identifier identifier) {
		return delegate.insert(instance, domainType, identifier);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#update(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <S> boolean update(S instance, Class<S> domainType) {
		return delegate.update(instance, domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#updateWithVersion(java.lang.Object, java.lang.Class, java.lang.Number)
	 */
	@Override
	public <S> boolean updateWithVersion(S instance, Class<S> domainType, Number nextVersion) {
		return delegate.updateWithVersion(instance, domainType, nextVersion);

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#delete(java.lang.Object, org.springframework.data.mapping.PersistentPropertyPath)
	 */
	@Override
	public void delete(Object rootId, PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {
		delegate.delete(rootId, propertyPath);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#delete(java.lang.Object, java.lang.Class)
	 */
	@Override
	public void delete(Object id, Class<?> domainType) {
		delegate.delete(id, domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#deleteWithVersion(java.lang.Object, java.lang.Class, Number)
	 */
	@Override
	public <T> void deleteWithVersion(Object id, Class<T> domainType, Number previousVersion) {
		delegate.deleteWithVersion(id, domainType, previousVersion);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#deleteAll(java.lang.Class)
	 */
	@Override
	public <T> void deleteAll(Class<T> domainType) {
		delegate.deleteAll(domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#deleteAll(org.springframework.data.mapping.PersistentPropertyPath)
	 */
	@Override
	public void deleteAll(PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {
		delegate.deleteAll(propertyPath);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#acquireLockById(java.lang.Object, org.springframework.data.relational.core.sql.LockMode, java.lang.Class)
	 */
	@Override
	public <T> void acquireLockById(Object id, LockMode lockMode, Class<T> domainType) {
		delegate.acquireLockById(id, lockMode, domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#acquireLockAll(org.springframework.data.relational.core.sql.LockMode, java.lang.Class)
	 */
	@Override
	public <T> void acquireLockAll(LockMode lockMode, Class<T> domainType) {
		delegate.acquireLockAll(lockMode, domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#count(java.lang.Class)
	 */
	@Override
	public long count(Class<?> domainType) {
		return delegate.count(domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#findById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> T findById(Object id, Class<T> domainType) {

		Assert.notNull(delegate, "Delegate is null");

		return delegate.findById(id, domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#findAll(java.lang.Class)
	 */
	@Override
	public <T> Iterable<T> findAll(Class<T> domainType) {
		return delegate.findAll(domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#findAllById(java.lang.Iterable, java.lang.Class)
	 */
	@Override
	public <T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType) {
		return delegate.findAllById(ids, domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.RelationResolver#findAllByPath(org.springframework.data.jdbc.support.Identifier, org.springframework.data.mapping.PersistentPropertyPath)
	 */
	@Override
	public Iterable<Object> findAllByPath(Identifier identifier,
			PersistentPropertyPath<? extends RelationalPersistentProperty> path) {
		return delegate.findAllByPath(identifier, path);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#existsById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {
		return delegate.existsById(id, domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#findAll(java.lang.Class, org.springframework.data.domain.Sort)
	 */
	@Override
	public <T> Iterable<T> findAll(Class<T> domainType, Sort sort) {
		return delegate.findAll(domainType, sort);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#findAll(java.lang.Class, org.springframework.data.domain.Pageable)
	 */
	@Override
	public <T> Iterable<T> findAll(Class<T> domainType, Pageable pageable) {
		return delegate.findAll(domainType, pageable);
	}

	/**
	 * Must be called exactly once before calling any of the other methods.
	 *
	 * @param delegate Must not be {@literal null}
	 */
	public void setDelegate(DataAccessStrategy delegate) {

		Assert.isNull(this.delegate, "The delegate must be set exactly once");
		Assert.notNull(delegate, "The delegate must not be set to null");

		this.delegate = delegate;
	}
}
