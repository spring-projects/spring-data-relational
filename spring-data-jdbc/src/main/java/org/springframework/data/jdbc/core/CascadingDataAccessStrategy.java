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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * Delegates each methods to the {@link DataAccessStrategy}s passed to the constructor in turn until the first that does
 * not throw an exception.
 *
 * @author Jens Schauder
 */
public class CascadingDataAccessStrategy implements DataAccessStrategy {

	private final List<DataAccessStrategy> strategies;

	public CascadingDataAccessStrategy(List<DataAccessStrategy> strategies) {
		this.strategies = new ArrayList<>(strategies);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#insert(java.lang.Object, java.lang.Class, java.util.Map)
	 */
	@Override
	public <T> Object insert(T instance, Class<T> domainType, Map<String, Object> additionalParameters) {
		return collect(das -> das.insert(instance, domainType, additionalParameters));
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
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#delete(java.lang.Object, java.lang.Class)
	 */
	@Override
	public void delete(Object id, Class<?> domainType) {
		collectVoid(das -> das.delete(id, domainType));
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
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#findAllByProperty(java.lang.Object, org.springframework.data.relational.core.mapping.RelationalPersistentProperty)
	 */
	@Override
	public <T> Iterable<T> findAllByProperty(Object rootId, RelationalPersistentProperty property) {
		return collect(das -> das.findAllByProperty(rootId, property));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#existsById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {
		return collect(das -> das.existsById(id, domainType));
	}

	private <T> T collect(Function<DataAccessStrategy, T> function) {

		// Keep <T> as Eclipse fails to compile if <> is used.
		return strategies.stream().collect(new FunctionCollector<T>(function));
	}

	private void collectVoid(Consumer<DataAccessStrategy> consumer) {

		collect(das -> {
			consumer.accept(das);
			return null;
		});
	}
}
