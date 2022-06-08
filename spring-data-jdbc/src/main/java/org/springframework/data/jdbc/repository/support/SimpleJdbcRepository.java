/*
 * Copyright 2017-2022 the original author or authors.
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

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

/**
 * Default implementation of the {@link org.springframework.data.repository.CrudRepository} interface.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Milan Milanov
 * @author Chirag Tailor
 */
@Transactional(readOnly = true)
public class SimpleJdbcRepository<T, ID> implements CrudRepository<T,ID>, PagingAndSortingRepository<T, ID> {

	private final JdbcAggregateOperations entityOperations;
	private final PersistentEntity<T, ?> entity;

	public SimpleJdbcRepository(JdbcAggregateOperations entityOperations,PersistentEntity<T, ?> entity) {

		Assert.notNull(entityOperations, "EntityOperations must not be null");
		Assert.notNull(entity, "Entity must not be null");

		this.entityOperations = entityOperations;
		this.entity = entity;
	}

	@Transactional
	@Override
	public <S extends T> S save(S instance) {
		return entityOperations.save(instance);
	}

	@Transactional
	@Override
	public <S extends T> Iterable<S> saveAll(Iterable<S> entities) {
		return entityOperations.saveAll(entities);
	}

	@Override
	public Optional<T> findById(ID id) {
		return Optional.ofNullable(entityOperations.findById(id, entity.getType()));
	}

	@Override
	public boolean existsById(ID id) {
		return entityOperations.existsById(id, entity.getType());
	}

	@Override
	public Iterable<T> findAll() {
		return entityOperations.findAll(entity.getType());
	}

	@Override
	public Iterable<T> findAllById(Iterable<ID> ids) {
		return entityOperations.findAllById(ids, entity.getType());
	}

	@Override
	public long count() {
		return entityOperations.count(entity.getType());
	}

	@Transactional
	@Override
	public void deleteById(ID id) {
		entityOperations.deleteById(id, entity.getType());
	}

	@Transactional
	@Override
	public void delete(T instance) {
		entityOperations.delete(instance, entity.getType());
	}

	@Override
	public void deleteAllById(Iterable<? extends ID> ids) {
		entityOperations.deleteAllById(ids, entity.getType());
	}

	@Transactional
	@Override
	public void deleteAll(Iterable<? extends T> entities) {
		entityOperations.deleteAll(entities, entity.getType());
	}

	@Transactional
	@Override
	public void deleteAll() {
		entityOperations.deleteAll(entity.getType());
	}

	@Override
	public Iterable<T> findAll(Sort sort) {
		return entityOperations.findAll(entity.getType(), sort);
	}

	@Override
	public Page<T> findAll(Pageable pageable) {
		return entityOperations.findAll(entity.getType(), pageable);
	}

}
