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

import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.core.mapping.JdbcPersistentEntity;
import org.springframework.data.jdbc.core.mapping.JdbcPersistentProperty;
import org.springframework.data.jdbc.repository.support.SimpleJdbcRepository;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.StreamUtils;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Generates SQL statements to be used by {@link SimpleJdbcRepository}
 *
 * @author Jens Schauder
 * @since 1.0
 */
class SqlGenerator {

	private final JdbcPersistentEntity<?> entity;
	private final JdbcMappingContext context;
	private final List<String> columnNames = new ArrayList<>();
	private final List<String> nonIdColumnNames = new ArrayList<>();

	private final Lazy<String> findOneSql = Lazy.of(this::createFindOneSelectSql);
	private final Lazy<String> findAllSql = Lazy.of(this::createFindAllSql);
	private final Lazy<String> findAllInListSql = Lazy.of(this::createFindAllInListSql);

	private final Lazy<String> existsSql = Lazy.of(this::createExistsSql);
	private final Lazy<String> countSql = Lazy.of(this::createCountSql);

	private final Lazy<String> updateSql = Lazy.of(this::createUpdateSql);

	private final Lazy<String> deleteByIdSql = Lazy.of(this::createDeleteSql);
	private final Lazy<String> deleteByListSql = Lazy.of(this::createDeleteByListSql);
	private final SqlGeneratorSource sqlGeneratorSource;

	SqlGenerator(JdbcMappingContext context, JdbcPersistentEntity<?> entity, SqlGeneratorSource sqlGeneratorSource) {

		this.context = context;
		this.entity = entity;
		this.sqlGeneratorSource = sqlGeneratorSource;
		initColumnNames();
	}

	private void initColumnNames() {

		entity.doWithProperties((PropertyHandler<JdbcPersistentProperty>) p -> {
			// the referencing column of referenced entity is expected to be on the other side of the relation
			if (!p.isEntity()) {
				columnNames.add(p.getColumnName());
				if (!entity.isIdProperty(p)) {
					nonIdColumnNames.add(p.getColumnName());
				}
			}
		});
	}

	String getFindAllInList() {
		return findAllInListSql.get();
	}

	String getFindAll() {
		return findAllSql.get();
	}

	/**
	 * Returns a query for selecting all simple properties of an entity, including those for one-to-one relationships.
	 * Results are limited to those rows referencing some other entity using the column specified by
	 * {@literal columnName}. This is used to select values for a complex property ({@link Set}, {@link Map} ...) based on
	 * a referencing entity.
	 *
	 * @param columnName name of the column of the FK back to the referencing entity.
	 * @param keyColumn  if the property is of type {@link Map} this column contains the map key.
	 * @param ordered    whether the SQL statement should include an ORDER BY for the keyColumn. If this is {@literal true}, the keyColumn must not be {@literal null}.
	 * @return a SQL String.
	 */
	String getFindAllByProperty(String columnName, String keyColumn, boolean ordered) {

		Assert.isTrue(keyColumn != null || !ordered, "If the SQL statement should be ordered a keyColumn to order by must be provided.");

		String baseSelect = (keyColumn != null) //
				? createSelectBuilder().column(cb -> cb.tableAlias(entity.getTableName()).column(keyColumn).as(keyColumn))
				.build()
				: getFindAll();

		String orderBy = ordered ? " ORDER BY " + keyColumn : "";

		return String.format("%s WHERE %s = :%s%s", baseSelect, columnName, columnName, orderBy);
	}

	String getExists() {
		return existsSql.get();
	}

	String getFindOne() {
		return findOneSql.get();
	}

	String getInsert(Set<String> additionalColumns) {
		return createInsertSql(additionalColumns);
	}

	String getUpdate() {
		return updateSql.get();
	}

	String getCount() {
		return countSql.get();
	}

	String getDeleteById() {
		return deleteByIdSql.get();
	}

	String getDeleteByList() {
		return deleteByListSql.get();
	}

	private String createFindOneSelectSql() {

		return createSelectBuilder() //
				.where(wb -> wb.tableAlias(entity.getTableName()).column(entity.getIdColumn()).eq().variable("id")) //
				.build();
	}

	private SelectBuilder createSelectBuilder() {

		SelectBuilder builder = new SelectBuilder(entity.getTableName());
		addColumnsForSimpleProperties(builder);
		addColumnsAndJoinsForOneToOneReferences(builder);

		return builder;
	}

	/**
	 * Adds the columns to the provided {@link SelectBuilder} representing simplem properties, including those from
	 * one-to-one relationships.
	 *
	 * @param builder The {@link SelectBuilder} to be modified.
	 */
	private void addColumnsAndJoinsForOneToOneReferences(SelectBuilder builder) {

		for (JdbcPersistentProperty property : entity) {
			if (!property.isEntity() //
					|| Collection.class.isAssignableFrom(property.getType()) //
					|| Map.class.isAssignableFrom(property.getType()) //
					) {
				continue;
			}

			JdbcPersistentEntity<?> refEntity = context.getRequiredPersistentEntity(property.getActualType());
			String joinAlias = property.getName();
			builder.join(jb -> jb.leftOuter().table(refEntity.getTableName()).as(joinAlias) //
					.where(property.getReverseColumnName()).eq().column(entity.getTableName(), entity.getIdColumn()));

			for (JdbcPersistentProperty refProperty : refEntity) {
				builder.column( //
						cb -> cb.tableAlias(joinAlias) //
								.column(refProperty.getColumnName()) //
								.as(joinAlias + "_" + refProperty.getColumnName()) //
				);
			}
		}
	}

	private void addColumnsForSimpleProperties(SelectBuilder builder) {

		for (JdbcPersistentProperty property : entity) {

			if (property.isEntity()) {
				continue;
			}

			builder.column(cb -> cb //
					.tableAlias(entity.getTableName()) //
					.column(property.getColumnName()) //
					.as(property.getColumnName()));
		}
	}

	private Stream<String> getColumnNameStream(String prefix) {

		return StreamUtils.createStreamFromIterator(entity.iterator()) //
				.flatMap(p -> getColumnNameStream(p, prefix));
	}

	private Stream<String> getColumnNameStream(JdbcPersistentProperty p, String prefix) {

		if (p.isEntity()) {
			return sqlGeneratorSource.getSqlGenerator(p.getType()).getColumnNameStream(prefix + p.getColumnName() + "_");
		} else {
			return Stream.of(prefix + p.getColumnName());
		}
	}

	private String createFindAllSql() {
		return createSelectBuilder().build();
	}

	private String createFindAllInListSql() {

		return createSelectBuilder() //
				.where(wb -> wb.tableAlias(entity.getTableName()).column(entity.getIdColumn()).in().variable("ids")) //
				.build();
	}

	private String createExistsSql() {
		return String.format("SELECT COUNT(*) FROM %s WHERE %s = :id", entity.getTableName(), entity.getIdColumn());
	}

	private String createCountSql() {
		return String.format("select count(*) from %s", entity.getTableName());
	}

	private String createInsertSql(Set<String> additionalColumns) {

		String insertTemplate = "INSERT INTO %s (%s) VALUES (%s)";

		LinkedHashSet<String> columnNamesForInsert = new LinkedHashSet<>(nonIdColumnNames);
		columnNamesForInsert.addAll(additionalColumns);

		String tableColumns = String.join(", ", columnNamesForInsert);
		String parameterNames = columnNamesForInsert.stream().collect(Collectors.joining(", :", ":", ""));

		return String.format(insertTemplate, entity.getTableName(), tableColumns, parameterNames);
	}

	private String createUpdateSql() {

		String updateTemplate = "UPDATE %s SET %s WHERE %s = :%s";

		String setClause = columnNames.stream()//
				.map(n -> String.format("%s = :%s", n, n))//
				.collect(Collectors.joining(", "));

		return String.format(updateTemplate, entity.getTableName(), setClause, entity.getIdColumn(), entity.getIdColumn());
	}

	private String createDeleteSql() {
		return String.format("DELETE FROM %s WHERE %s = :id", entity.getTableName(), entity.getIdColumn());
	}

	String createDeleteAllSql(PropertyPath path) {

		if (path == null) {
			return String.format("DELETE FROM %s", entity.getTableName());
		}

		JdbcPersistentEntity<?> entityToDelete = context.getRequiredPersistentEntity(path.getLeafType());

		JdbcPersistentEntity<?> owningEntity = context.getRequiredPersistentEntity(path.getOwningType());
		JdbcPersistentProperty property = owningEntity.getRequiredPersistentProperty(path.getSegment());

		String innerMostCondition = String.format("%s IS NOT NULL", property.getReverseColumnName());

		String condition = cascadeConditions(innerMostCondition, path.next());

		return String.format("DELETE FROM %s WHERE %s", entityToDelete.getTableName(), condition);
	}

	private String createDeleteByListSql() {
		return String.format("DELETE FROM %s WHERE %s IN (:ids)", entity.getTableName(), entity.getIdColumn());
	}

	String createDeleteByPath(PropertyPath path) {

		JdbcPersistentEntity<?> entityToDelete = context.getRequiredPersistentEntity(path.getLeafType());
		JdbcPersistentEntity<?> owningEntity = context.getRequiredPersistentEntity(path.getOwningType());
		JdbcPersistentProperty property = owningEntity.getRequiredPersistentProperty(path.getSegment());

		String innerMostCondition = String.format("%s = :rootId", property.getReverseColumnName());

		String condition = cascadeConditions(innerMostCondition, path.next());

		return String.format("DELETE FROM %s WHERE %s", entityToDelete.getTableName(), condition);
	}

	private String cascadeConditions(String innerCondition, PropertyPath path) {

		if (path == null) {
			return innerCondition;
		}

		JdbcPersistentEntity<?> entity = context.getRequiredPersistentEntity(path.getOwningType());
		JdbcPersistentProperty property = entity.getPersistentProperty(path.getSegment());

		Assert.notNull(property, "could not find property for path " + path.getSegment() + " in " + entity);

		return String.format("%s IN (SELECT %s FROM %s WHERE %s)", //
				property.getReverseColumnName(), //
				entity.getIdColumn(), //
				entity.getTableName(), innerCondition //
		);
	}
}
