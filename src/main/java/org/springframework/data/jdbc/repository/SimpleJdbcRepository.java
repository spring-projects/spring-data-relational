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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.sql.DataSource;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntity;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentProperty;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

/**
 * @author Jens Schauder
 */
public class SimpleJdbcRepository<T, ID extends Serializable> implements CrudRepository<T, ID> {

	private final JdbcPersistentEntity<T> entity;
	private final NamedParameterJdbcOperations template;
	private final SqlGenerator sql;

	private final EntityRowMapper<T> entityRowMapper;

	public SimpleJdbcRepository(JdbcPersistentEntity<T> entity, DataSource dataSource) {

		this.entity = entity;
		this.template = new NamedParameterJdbcTemplate(dataSource);

		entityRowMapper = new EntityRowMapper<T>(entity);
		sql = new SqlGenerator(entity);
	}

	@Override
	public <S extends T> S save(S instance) {

		KeyHolder holder = new GeneratedKeyHolder();

		template.update(
				sql.getInsert(),
				new MapSqlParameterSource(getPropertyMap(instance)),
				holder);

		entity.setId(instance, holder.getKey());

		return instance;
	}

	@Override
	public <S extends T> Iterable<S> save(Iterable<S> entities) {

		entities.forEach(this::save);

		return entities;
	}

	@Override
	public T findOne(ID id) {

		return template.queryForObject(
				sql.getFindOne(),
				new MapSqlParameterSource("id", id),
				entityRowMapper
		);
	}

	@Override
	public boolean exists(ID id) {

		return template.queryForObject(
				sql.getExists(),
				new MapSqlParameterSource("id", id),
				Boolean.class
		);
	}

	@Override
	public Iterable<T> findAll() {
		return template.query(sql.getFindAll(), entityRowMapper);
	}

	@Override
	public Iterable<T> findAll(Iterable<ID> ids) {
		return template.query(sql.getFindAllInList(), new MapSqlParameterSource("ids", ids), entityRowMapper);
	}

	@Override
	public long count() {
		return template.getJdbcOperations().queryForObject(sql.getCount(), Long.class);
	}

	@Override
	public void delete(ID id) {
		template.update(sql.getDeleteById(), new MapSqlParameterSource("id", id));
	}

	@Override
	public void delete(T instance) {

		template.update(
				sql.getDeleteById(),
				new MapSqlParameterSource("id",
						entity.getIdValue(instance)));
	}

	@Override
	public void delete(Iterable<? extends T> entities) {

		template.update(
				sql.getDeleteByList(),
				new MapSqlParameterSource("ids",
						StreamSupport
								.stream(entities.spliterator(), false)
								.map(entity::getIdValue)
								.collect(Collectors.toList())
				)
		);
	}

	@Override
	public void deleteAll() {
		template.getJdbcOperations().update(sql.getDeleteAll());
	}

	private <S extends T> Map<String, Object> getPropertyMap(final S instance) {

		Map<String, Object> parameters = new HashMap<>();

		this.entity.doWithProperties(new PropertyHandler<JdbcPersistentProperty>() {
			@Override
			public void doWithPersistentProperty(JdbcPersistentProperty persistentProperty) {
				parameters.put(persistentProperty.getColumnName(), entity.getPropertyAccessor(instance).getProperty(persistentProperty));
			}
		});

		return parameters;
	}
}
