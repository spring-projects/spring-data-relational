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

import org.springframework.data.jdbc.core.mapping.JdbcPersistentProperty;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.util.Assert;

/**
 * delegates all method calls to an instance set after construction. This is useful for {@link DataAccessStrategy}s with
 * cyclical dependencies.
 *
 * @author Jens Schauder
 * @since 1.0
 */
public class DelegatingDataAccessStrategy implements DataAccessStrategy {

	private DataAccessStrategy delegate;

	@Override
	public <T> void insert(T instance, Class<T> domainType, Map<String, Object> additionalParameters) {
		delegate.insert(instance, domainType, additionalParameters);
	}

	@Override
	public <S> void update(S instance, Class<S> domainType) {
		delegate.update(instance, domainType);
	}

	@Override
	public void delete(Object rootId, PropertyPath propertyPath) {
		delegate.delete(rootId, propertyPath);
	}

	@Override
	public void delete(Object id, Class<?> domainType) {
		delegate.delete(id, domainType);
	}

	@Override
	public <T> void deleteAll(Class<T> domainType) {
		delegate.deleteAll(domainType);
	}

	@Override
	public <T> void deleteAll(PropertyPath propertyPath) {
		delegate.deleteAll(propertyPath);
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
	public <T> Iterable<T> findAllByProperty(Object rootId, JdbcPersistentProperty property) {

		Assert.notNull(delegate, "Delegate is null");

		return delegate.findAllByProperty(rootId, property);
	}

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
