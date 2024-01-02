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

import static java.lang.Boolean.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.conversion.IdValueSource;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.sql.LockMode;

/**
 * Delegates each method to the {@link DataAccessStrategy}s passed to the constructor in turn until the first that does
 * not throw an exception.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Tyler Van Gorder
 * @author Milan Milanov
 * @author Myeonghyeon Lee
 * @author Chirag Tailor
 * @author Diego Krupitza
 * @since 1.1
 */
public class CascadingDataAccessStrategy implements DataAccessStrategy {

	private final List<DataAccessStrategy> strategies;

	public CascadingDataAccessStrategy(List<DataAccessStrategy> strategies) {
		this.strategies = new ArrayList<>(strategies);
	}

	@Override
	public <T> Object insert(T instance, Class<T> domainType, Identifier identifier, IdValueSource idValueSource) {
		return collect(das -> das.insert(instance, domainType, identifier, idValueSource));
	}

	@Override
	public <T> Object[] insert(List<InsertSubject<T>> insertSubjects, Class<T> domainType, IdValueSource idValueSource) {
		return collect(das -> das.insert(insertSubjects, domainType, idValueSource));
	}

	@Override
	public <S> boolean update(S instance, Class<S> domainType) {
		return collect(das -> das.update(instance, domainType));
	}

	@Override
	public <S> boolean updateWithVersion(S instance, Class<S> domainType, Number previousVersion) {
		return collect(das -> das.updateWithVersion(instance, domainType, previousVersion));
	}

	@Override
	public void delete(Object id, Class<?> domainType) {
		collectVoid(das -> das.delete(id, domainType));
	}

	@Override
	public void delete(Iterable<Object> ids, Class<?> domainType) {
		collectVoid(das -> das.delete(ids, domainType));
	}

	@Override
	public <T> void deleteWithVersion(Object id, Class<T> domainType, Number previousVersion) {
		collectVoid(das -> das.deleteWithVersion(id, domainType, previousVersion));
	}

	@Override
	public void delete(Object rootId, PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {
		collectVoid(das -> das.delete(rootId, propertyPath));
	}

	@Override
	public void delete(Iterable<Object> rootIds, PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {
		collectVoid(das -> das.delete(rootIds, propertyPath));
	}

	@Override
	public <T> void deleteAll(Class<T> domainType) {
		collectVoid(das -> das.deleteAll(domainType));
	}

	@Override
	public void deleteAll(PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {
		collectVoid(das -> das.deleteAll(propertyPath));
	}

	@Override
	public <T> void acquireLockById(Object id, LockMode lockMode, Class<T> domainType) {
		collectVoid(das -> das.acquireLockById(id, lockMode, domainType));
	}

	@Override
	public <T> void acquireLockAll(LockMode lockMode, Class<T> domainType) {
		collectVoid(das -> das.acquireLockAll(lockMode, domainType));
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
	public Iterable<Object> findAllByPath(Identifier identifier,
			PersistentPropertyPath<? extends RelationalPersistentProperty> path) {
		return collect(das -> das.findAllByPath(identifier, path));
	}

	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {
		return collect(das -> das.existsById(id, domainType));
	}

	@Override
	public <T> Iterable<T> findAll(Class<T> domainType, Sort sort) {
		return collect(das -> das.findAll(domainType, sort));
	}

	@Override
	public <T> Iterable<T> findAll(Class<T> domainType, Pageable pageable) {
		return collect(das -> das.findAll(domainType, pageable));
	}

	@Override
	public <T> Optional<T> findOne(Query query, Class<T> domainType) {
		return collect(das -> das.findOne(query, domainType));
	}

	@Override
	public <T> Iterable<T> findAll(Query query, Class<T> domainType) {
		return collect(das -> das.findAll(query, domainType));
	}

	@Override
	public <T> Iterable<T> findAll(Query query, Class<T> domainType, Pageable pageable) {
		return collect(das -> das.findAll(query, domainType, pageable));
	}

	@Override
	public <T> boolean exists(Query query, Class<T> domainType) {
		return collect(das -> das.exists(query, domainType));
	}

	@Override
	public <T> long count(Query query, Class<T> domainType) {
		return collect(das -> das.count(query, domainType));
	}

	private <T> T collect(Function<DataAccessStrategy, T> function) {

		return strategies.stream().collect(new FunctionCollector<>(function));
	}

	private void collectVoid(Consumer<DataAccessStrategy> consumer) {

		collect(das -> {
			consumer.accept(das);
			return TRUE;
		});
	}
}
