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

import org.springframework.data.jdbc.core.mapping.JdbcPersistentProperty;
import org.springframework.data.mapping.PropertyPath;

/**
 * Delegates each methods to the {@link DataAccessStrategy}s passed to the constructor in turn until the first that does
 * not throw an exception.
 *
 * @author Jens Schauder
 * @since 1.0
 */
public class CascadingDataAccessStrategy implements DataAccessStrategy {

	private final List<DataAccessStrategy> strategies;

	public CascadingDataAccessStrategy(List<DataAccessStrategy> strategies) {
		this.strategies = new ArrayList<>(strategies);
	}

	@Override
	public <T> void insert(T instance, Class<T> domainType, Map<String, Object> additionalParameters) {
		collectVoid(das -> das.insert(instance, domainType, additionalParameters));
	}

	@Override
	public <S> void update(S instance, Class<S> domainType) {
		collectVoid(das -> das.update(instance, domainType));
	}

	@Override
	public void delete(Object id, Class<?> domainType) {
		collectVoid(das -> das.delete(id, domainType));
	}

	@Override
	public void delete(Object rootId, PropertyPath propertyPath) {
		collectVoid(das -> das.delete(rootId, propertyPath));
	}

	@Override
	public <T> void deleteAll(Class<T> domainType) {
		collectVoid(das -> das.deleteAll(domainType));
	}

	@Override
	public <T> void deleteAll(PropertyPath propertyPath) {
		collectVoid(das -> das.deleteAll(propertyPath));
	}

	@Override
	public long count(Class<?> domainType) {
		return collect(das -> das.count(domainType));
	}

	@Override
	public <T> T findById(Object id, Class<T> domainType) {
		return collect(das -> das.findById(id, domainType));
	}

	@Override
	public <T> Iterable<T> findAll(Class<T> domainType) {
		return collect(das -> das.findAll(domainType));
	}

	@Override
	public <T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType) {
		return collect(das -> das.findAllById(ids, domainType));
	}

	@Override
	public <T> Iterable<T> findAllByProperty(Object rootId, JdbcPersistentProperty property) {
		return collect(das -> das.findAllByProperty(rootId, property));
	}

	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {
		return collect(das -> das.existsById(id, domainType));
	}

	private <T> T collect(Function<DataAccessStrategy, T> function) {
		return strategies.stream().collect(new FunctionCollector<T>(function));
	}

	private void collectVoid(Consumer<DataAccessStrategy> consumer) {

		collect(das -> {
			consumer.accept(das);
			return null;
		});
	}

}
