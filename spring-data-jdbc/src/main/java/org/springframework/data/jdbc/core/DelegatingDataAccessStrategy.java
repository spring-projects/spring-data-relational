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
package org.springframework.data.jdbc.core;

import java.util.Map;

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.util.Assert;

/**
 * Delegates all method calls to an instance set after construction. This is useful for {@link DataAccessStrategy}s with
 * cyclic dependencies.
 *
 * @author Jens Schauder
 */
public class DelegatingDataAccessStrategy implements DataAccessStrategy {

	private DataAccessStrategy delegate;

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#insert(java.lang.Object, java.lang.Class, java.util.Map)
	 */
	@Override
	public <T> Object insert(T instance, Class<T> domainType, Map<String, Object> additionalParameters) {
		return delegate.insert(instance, domainType, additionalParameters);
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
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#findAllByProperty(java.lang.Object, org.springframework.data.relational.core.mapping.RelationalPersistentProperty)
	 */
	@Override
	public <T> Iterable<T> findAllByProperty(Object rootId, RelationalPersistentProperty property) {

		Assert.notNull(delegate, "Delegate is null");

		return delegate.findAllByProperty(rootId, property);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#existsById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {
		return delegate.existsById(id, domainType);
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
