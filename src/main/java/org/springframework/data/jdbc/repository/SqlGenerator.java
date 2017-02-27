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
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntity;
import org.springframework.data.mapping.PropertyHandler;

/**
 * @author Jens Schauder
 */
class SqlGenerator {

	private final String findOneSql;
	private final String findAllSql;
	private final String findAllInListSql;

	private final String existsSql;
	private final String countSql;

	private final String insertSql;
	private final String deleteByIdSql;
	private final String deleteAllSql;
	private final String deleteByListSql;
	private final String updateSql;
	private final List<String> propertyNames = new ArrayList<>();

	<T> SqlGenerator(JdbcPersistentEntity<T> entity) {

		entity.doWithProperties((PropertyHandler) persistentProperty -> propertyNames.add(persistentProperty.getName()));

		findOneSql = createFindOneSelectSql(entity);
		findAllSql = createFindAllSql(entity);
		findAllInListSql = createFindAllInListSql(entity);

		existsSql = createExistsSql(entity);
		countSql = createCountSql(entity);

		insertSql = createInsertSql(entity);
		updateSql = createUpdateSql(entity);

		deleteByIdSql = createDeleteSql(entity);
		deleteAllSql = createDeleteAllSql(entity);
		deleteByListSql = createDeleteByListSql(entity);
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

	String getInsert() {
		return insertSql;
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
	private String createFindOneSelectSql(JdbcPersistentEntity<?> entity) {
		return String.format("select * from %s where %s = :id", entity.getTableName(), entity.getIdColumn());
	}

	private String createFindAllSql(JdbcPersistentEntity<?> entity) {
		return String.format("select * from %s", entity.getTableName());
	}

	private String createFindAllInListSql(JdbcPersistentEntity<?> entity) {
		return String.format(String.format("select * from %s where %s in (:ids)", entity.getTableName(), entity.getIdColumn()), entity.getTableName());
	}

	private String createExistsSql(JdbcPersistentEntity<?> entity) {
		return String.format("select count(*) from %s where %s = :id", entity.getTableName(), entity.getIdColumn());
	}

	private <T> String createCountSql(JdbcPersistentEntity<T> entity) {
		return String.format("select count(*) from %s", entity.getTableName());
	}

	private String createInsertSql(JdbcPersistentEntity<?> entity) {

		String insertTemplate = "insert into %s (%s) values (%s)";

		String tableColumns = propertyNames.stream().collect(Collectors.joining(", "));
		String parameterNames = propertyNames.stream().collect(Collectors.joining(", :", ":", ""));

		return String.format(insertTemplate, entity.getTableName(), tableColumns, parameterNames);
	}

	private <T> String createUpdateSql(JdbcPersistentEntity<T> entity) {

		String updateTemplate = "update %s set %s where %s = :%s";

		String setClause = propertyNames.stream().map(n -> String.format("%s = :%s", n, n)).collect(Collectors.joining(", "));

		return String.format(updateTemplate, entity.getTableName(), setClause, entity.getIdColumn(), entity.getIdColumn());
	}

	private String createDeleteSql(JdbcPersistentEntity entity) {
		return String.format("delete from %s where %s = :id", entity.getTableName(), entity.getIdColumn());
	}

	private String createDeleteAllSql(JdbcPersistentEntity entity) {
		return String.format("delete from %s", entity.getTableName());
	}

	private String createDeleteByListSql(JdbcPersistentEntity entity) {
		return String.format("delete from %s where %s in (:ids)", entity.getTableName(), entity.getIdColumn());
	}

}
