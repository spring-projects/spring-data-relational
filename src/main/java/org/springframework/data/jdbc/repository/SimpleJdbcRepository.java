/*
 * Copyright 2017 the original author or authors.
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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * @author Jens Schauder
 */
public class SimpleJdbcRepository<T, ID extends Serializable> implements CrudRepository<T, ID> {

	private final EntityInformation entityInformation;
	private final NamedParameterJdbcOperations template;

	public SimpleJdbcRepository(EntityInformation entityInformation, DataSource dataSource) {
		this.entityInformation = entityInformation;
		this.template = new NamedParameterJdbcTemplate(dataSource);
	}

	@Override
	public <S extends T> S save(S entity) {
		Map<String, Object> parameters = new HashMap<>();
		parameters.put("id", entityInformation.getId(entity));
		parameters.put("name", "blah blah");

		template.update(
				"insert into dummyentity (id, name) values (:id, :name)",
				parameters);

		return entity;
	}

	@Override
	public <S extends T> Iterable<S> save(Iterable<S> entities) {
		return null;
	}

	@Override
	public T findOne(ID id) {
		return null;
	}

	@Override
	public boolean exists(ID id) {
		return false;
	}

	@Override
	public Iterable<T> findAll() {
		return null;
	}

	@Override
	public Iterable<T> findAll(Iterable<ID> ids) {
		return null;
	}

	@Override
	public long count() {
		return 0;
	}

	@Override
	public void delete(ID id) {

	}

	@Override
	public void delete(T entity) {

	}

	@Override
	public void delete(Iterable<? extends T> entities) {

	}

	@Override
	public void deleteAll() {

	}
}
