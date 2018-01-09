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
package org.springframework.data.jdbc.repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jdbc.core.JdbcEntityOperations;
import org.springframework.data.jdbc.core.JdbcEntityTemplate;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntityInformation;
import org.springframework.data.repository.CrudRepository;

/**
 * @author Jens Schauder
 * @since 2.0
 */
public class SimpleJdbcRepository<T, ID> implements CrudRepository<T, ID> {

	private final JdbcPersistentEntityInformation<T, ID> entityInformation;

	private final JdbcEntityOperations entityOperations;

	/**
	 * Creates a new {@link SimpleJdbcRepository}.
	 */
	public SimpleJdbcRepository(JdbcEntityTemplate entityOperations,
			JdbcPersistentEntityInformation<T, ID> entityInformation) {

		this.entityOperations = entityOperations;
		this.entityInformation = entityInformation;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#save(S)
	 */
	@Override
	public <S extends T> S save(S instance) {

		entityOperations.save(instance, entityInformation.getJavaType());

		return instance;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#save(java.lang.Iterable)
	 */
	@Override
	public <S extends T> Iterable<S> saveAll(Iterable<S> entities) {

		List<S> savedEntities = new ArrayList<>();
		entities.forEach(e -> savedEntities.add(save(e)));
		return savedEntities;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findOne(java.io.Serializable)
	 */
	@Override
	public Optional<T> findById(ID id) {
		return Optional.ofNullable(entityOperations.findById(id, entityInformation.getJavaType()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#exists(java.io.Serializable)
	 */
	@Override
	public boolean existsById(ID id) {
		return entityOperations.existsById(id, entityInformation.getJavaType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findAll()
	 */
	@Override
	public Iterable<T> findAll() {
		return entityOperations.findAll(entityInformation.getJavaType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findAll(java.lang.Iterable)
	 */
	@Override
	public Iterable<T> findAllById(Iterable<ID> ids) {
		return entityOperations.findAllById(ids, entityInformation.getJavaType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#count()
	 */
	@Override
	public long count() {
		return entityOperations.count(entityInformation.getJavaType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.io.Serializable)
	 */
	@Override
	public void deleteById(ID id) {
		entityOperations.deleteById(id, entityInformation.getJavaType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.lang.Object)
	 */
	@Override
	public void delete(T instance) {
		entityOperations.delete(instance, entityInformation.getJavaType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.lang.Iterable)
	 */
	@Override
	public void deleteAll(Iterable<? extends T> entities) {

		for (T entity : entities) {
			entityOperations.delete(entity, (Class<T>) entity.getClass());

		}
	}

	@Override
	public void deleteAll() {
		entityOperations.deleteAll(entityInformation.getJavaType());
	}
}
