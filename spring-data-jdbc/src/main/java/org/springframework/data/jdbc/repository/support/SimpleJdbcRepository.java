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
package org.springframework.data.jdbc.repository.support;

import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.FluentQuery;
import org.springframework.data.repository.query.QueryByExampleExecutor;
import org.springframework.data.util.Streamable;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

/**
 * Default implementation of the {@link org.springframework.data.repository.CrudRepository} interface.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Milan Milanov
 * @author Diego Krupitza
 */
@Transactional(readOnly = true)
public class SimpleJdbcRepository<T, ID> implements PagingAndSortingRepository<T, ID>, QueryByExampleExecutor<T> {

	private final JdbcAggregateOperations entityOperations;
	private final PersistentEntity<T, ?> entity;

	public SimpleJdbcRepository(JdbcAggregateOperations entityOperations, PersistentEntity<T, ?> entity) {

		Assert.notNull(entityOperations, "EntityOperations must not be null.");
		Assert.notNull(entity, "Entity must not be null.");

		this.entityOperations = entityOperations;
		this.entity = entity;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#save(S)
	 */
	@Transactional
	@Override
	public <S extends T> S save(S instance) {
		return entityOperations.save(instance);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#save(java.lang.Iterable)
	 */
	@Transactional
	@Override
	public <S extends T> Iterable<S> saveAll(Iterable<S> entities) {

		return Streamable.of(entities).stream() //
				.map(this::save) //
				.collect(Collectors.toList());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findOne(java.io.Serializable)
	 */
	@Override
	public Optional<T> findById(ID id) {
		return Optional.ofNullable(entityOperations.findById(id, entity.getType()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#exists(java.io.Serializable)
	 */
	@Override
	public boolean existsById(ID id) {
		return entityOperations.existsById(id, entity.getType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findAll()
	 */
	@Override
	public Iterable<T> findAll() {
		return entityOperations.findAll(entity.getType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findAll(java.lang.Iterable)
	 */
	@Override
	public Iterable<T> findAllById(Iterable<ID> ids) {
		return entityOperations.findAllById(ids, entity.getType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#count()
	 */
	@Override
	public long count() {
		return entityOperations.count(entity.getType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.io.Serializable)
	 */
	@Transactional
	@Override
	public void deleteById(ID id) {
		entityOperations.deleteById(id, entity.getType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.lang.Object)
	 */
	@Transactional
	@Override
	public void delete(T instance) {
		entityOperations.delete(instance, entity.getType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.deleteAll#delete(java.lang.Iterable)
	 */
	@Override
	public void deleteAllById(Iterable<? extends ID> ids) {
		ids.forEach(it -> entityOperations.deleteById(it, entity.getType()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.lang.Iterable)
	 */
	@Transactional
	@Override
	@SuppressWarnings("unchecked")
	public void deleteAll(Iterable<? extends T> entities) {
		entities.forEach(it -> entityOperations.delete(it, (Class<T>) it.getClass()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#deleteAll()
	 */
	@Transactional
	@Override
	public void deleteAll() {
		entityOperations.deleteAll(entity.getType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.PagingAndSortingRepository#findAll(org.springframework.data.domain.Sort sort)
	 */
	@Override
	public Iterable<T> findAll(Sort sort) {
		return entityOperations.findAll(entity.getType(), sort);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.PagingAndSortingRepository#findAll(org.springframework.data.domain.Pageable pageable)
	 */
	@Override
	public Page<T> findAll(Pageable pageable) {
		return entityOperations.findAll(entity.getType(), pageable);
	}

	@Override
	public <S extends T> Optional<S> findOne(Example<S> example) {
		Assert.notNull(example, "Example must not be null!");
		return this.entityOperations.selectOne(example);
	}

	@Override
	public <S extends T> Iterable<S> findAll(Example<S> example) {
		Assert.notNull(example, "Example must not be null!");

		return findAll(example, Sort.unsorted());
	}

	@Override
	public <S extends T> Iterable<S> findAll(Example<S> example, Sort sort) {
		Assert.notNull(example, "Example must not be null!");
		Assert.notNull(sort, "Sort must not be null!");

		return this.entityOperations.select(example, sort);
	}

	@Override
	public <S extends T> Page<S> findAll(Example<S> example, Pageable pageable) {
		Assert.notNull(example, "Example must not be null!");

		return this.entityOperations.select(example, pageable);
	}

	@Override
	public <S extends T> long count(Example<S> example) {
		Assert.notNull(example, "Example must not be null!");

		return this.entityOperations.count(example);
	}

	@Override
	public <S extends T> boolean exists(Example<S> example) {
		Assert.notNull(example, "Example must not be null!");

		return this.entityOperations.exists(example);
	}

	@Override
	public <S extends T, R> R findBy(Example<S> example, Function<FluentQuery.FetchableFluentQuery<S>, R> queryFunction) {
		throw new UnsupportedOperationException("Not implemented");
	}
}
