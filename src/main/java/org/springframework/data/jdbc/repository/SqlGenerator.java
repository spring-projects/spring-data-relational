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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntity;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentProperty;
import org.springframework.data.mapping.PropertyHandler;

/**
 * Generates SQL statements to be used by {@link SimpleJdbcRepository}
 *
 * @author Jens Schauder
 * @since 2.0
 */
class SqlGenerator {

	private final String findOneSql;
	private final String findAllSql;
	private final String findAllInListSql;

	private final String existsSql;
	private final String countSql;

	private final String updateSql;

	private final String deleteByIdSql;
	private final String deleteAllSql;
	private final String deleteByListSql;

	private final JdbcPersistentEntity<?> entity;
	private final List<String> propertyNames = new ArrayList<>();
	private final List<String> nonIdPropertyNames = new ArrayList<>();

	SqlGenerator(JdbcPersistentEntity<?> entity) {

		this.entity = entity;

		initPropertyNames();

		findOneSql = createFindOneSelectSql();
		findAllSql = createFindAllSql();
		findAllInListSql = createFindAllInListSql();

		existsSql = createExistsSql();
		countSql = createCountSql();

		updateSql = createUpdateSql();

		deleteByIdSql = createDeleteSql();
		deleteAllSql = createDeleteAllSql();
		deleteByListSql = createDeleteByListSql();
	}

	private void initPropertyNames() {

		entity.doWithProperties((PropertyHandler<JdbcPersistentProperty>) p -> {
			propertyNames.add(p.getName());
			if (!entity.isIdProperty(p)) {
				nonIdPropertyNames.add(p.getName());
			}
		});
	}

	String getFindAllInList() {
		return findAllInListSql;
	}

	String getFindAll() {
		return findAllSql;
	}

	String getExists() {
		return existsSql;
	}

	String getFindOne() {
		return findOneSql;
	}

	String getInsert(boolean excludeId) {
		return createInsertSql(excludeId);
	}

	String getUpdate() {
		return updateSql;
	}

	String getCount() {
		return countSql;
	}

	String getDeleteById() {
		return deleteByIdSql;
	}

	String getDeleteAll() {
		return deleteAllSql;
	}

	String getDeleteByList() {
		return deleteByListSql;
	}

	private String createFindOneSelectSql() {
		return String.format("select * from %s where %s = :id", entity.getTableName(), entity.getIdColumn());
	}

	private String createFindAllSql() {
		return String.format("select * from %s", entity.getTableName());
	}

	private String createFindAllInListSql() {
		return String.format("select * from %s where %s in (:ids)", entity.getTableName(), entity.getIdColumn());
	}

	private String createExistsSql() {
		return String.format("select count(*) from %s where %s = :id", entity.getTableName(), entity.getIdColumn());
	}

	private String createCountSql() {
		return String.format("select count(*) from %s", entity.getTableName());
	}

	private String createInsertSql(boolean excludeId) {

		String insertTemplate = "insert into %s (%s) values (%s)";
		List<String> propertyNamesForInsert = excludeId ? nonIdPropertyNames : propertyNames;
		String tableColumns = String.join(", ", propertyNamesForInsert);
		String parameterNames = propertyNamesForInsert.stream().collect(Collectors.joining(", :", ":", ""));

		return String.format(insertTemplate, entity.getTableName(), tableColumns, parameterNames);
	}

	private String createUpdateSql() {

		String updateTemplate = "update %s set %s where %s = :%s";

		String setClause = propertyNames.stream()//
				.map(n -> String.format("%s = :%s", n, n))//
				.collect(Collectors.joining(", "));

		return String.format(updateTemplate, entity.getTableName(), setClause, entity.getIdColumn(), entity.getIdColumn());
	}

	private String createDeleteSql() {
		return String.format("delete from %s where %s = :id", entity.getTableName(), entity.getIdColumn());
	}

	private String createDeleteAllSql() {
		return String.format("delete from %s", entity.getTableName());
	}

	private String createDeleteByListSql() {
		return String.format("delete from %s where %s in (:ids)", entity.getTableName(), entity.getIdColumn());
	}
}
