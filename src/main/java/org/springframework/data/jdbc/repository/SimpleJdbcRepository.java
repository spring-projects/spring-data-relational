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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * @author Jens Schauder
 */
public class SimpleJdbcRepository<T, ID extends Serializable> implements CrudRepository<T, ID> {

	private final JdbcPersistentEntity<T> entity;
	private final NamedParameterJdbcOperations template;

	private final String findOneSql;
	private final String insertSql;

	public SimpleJdbcRepository(JdbcPersistentEntity<T> entity, DataSource dataSource) {

		this.entity = entity;
		this.template = new NamedParameterJdbcTemplate(dataSource);

		findOneSql = createFindOneSelectSql();
		insertSql = createInsertSql();
	}

	@Override
	public <S extends T> S save(S entity) {

		template.update(insertSql, getPropertyMap(entity));

		return entity;
	}

	@Override
	public <S extends T> Iterable<S> save(Iterable<S> entities) {
		return null;
	}

	@Override
	public T findOne(ID id) {

		return template.queryForObject(
				findOneSql,
				new MapSqlParameterSource("id", id),
				new EntityRowMapper<T>(entity)
		);
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

	private String createFindOneSelectSql() {

		String tableName = entity.getType().getSimpleName();
		String idColumn = entity.getIdProperty().getName();

		return String.format("select * from %s where %s = :id", tableName, idColumn);
	}

	private String createInsertSql() {

		List<String> propertyNames = new ArrayList<>();
		entity.doWithProperties((PropertyHandler) persistentProperty -> propertyNames.add(persistentProperty.getName()));

		String insertTemplate = "insert into %s (%s) values (%s)";

		String tableName = entity.getType().getSimpleName();

		String tableColumns = propertyNames.stream().collect(Collectors.joining(", "));
		String parameterNames = propertyNames.stream().collect(Collectors.joining(", :", ":", ""));

		return String.format(insertTemplate, tableName, tableColumns, parameterNames);
	}

	private <S extends T> Map<String, Object> getPropertyMap(final S entity) {

		Map<String, Object> parameters = new HashMap<>();

		this.entity.doWithProperties(new PropertyHandler() {
			@Override
			public void doWithPersistentProperty(PersistentProperty persistentProperty) {
				try {
					parameters.put(persistentProperty.getName(), persistentProperty.getGetter().invoke(entity));
				} catch (Exception e) {
					throw new RuntimeException(String.format("Couldn't get value of property %s", persistentProperty.getName()));
				}
			}
		});

		return parameters;
	}
}
