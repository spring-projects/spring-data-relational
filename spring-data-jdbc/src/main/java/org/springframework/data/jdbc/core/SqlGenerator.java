/*
 * Copyright 2017-2019 the original author or authors.
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
package org.springframework.data.jdbc.core;

import lombok.Value;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.data.annotation.ReadOnlyProperty;
import org.springframework.data.jdbc.repository.support.SimpleJdbcRepository;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.*;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.StreamUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Generates SQL statements to be used by {@link SimpleJdbcRepository}
 *
 * @author Jens Schauder
 * @author Yoichi Imai
 * @author Bastian Wilhelm
 * @author Oleksandr Kucher
 */
class SqlGenerator {

	private final RelationalPersistentEntity<?> entity;
	private final RelationalMappingContext mappingContext;
	private final List<String> columnNames = new ArrayList<>();
	private final List<String> nonIdColumnNames = new ArrayList<>();
	private final Set<String> readOnlyColumnNames = new HashSet<>();

	private final Lazy<String> findOneSql = Lazy.of(this::createFindOneSql);
	private final Lazy<String> findAllSql = Lazy.of(this::createFindAllSql);
	private final Lazy<String> findAllInListSql = Lazy.of(this::createFindAllInListSql);

	private final Lazy<String> existsSql = Lazy.of(this::createExistsSql);
	private final Lazy<String> countSql = Lazy.of(this::createCountSql);

	private final Lazy<String> updateSql = Lazy.of(this::createUpdateSql);

	private final Lazy<String> deleteByIdSql = Lazy.of(this::createDeleteSql);
	private final Lazy<String> deleteByListSql = Lazy.of(this::createDeleteByListSql);
	private final SqlGeneratorSource sqlGeneratorSource;

	private final Pattern parameterPattern = Pattern.compile("\\W");
	private final SqlContext sqlContext;

	SqlGenerator(RelationalMappingContext mappingContext, RelationalPersistentEntity<?> entity,
			SqlGeneratorSource sqlGeneratorSource) {

		this.mappingContext = mappingContext;
		this.entity = entity;
		this.sqlGeneratorSource = sqlGeneratorSource;
		this.sqlContext = new SqlContext(entity);
		initColumnNames(entity, "");
	}

	private void initColumnNames(RelationalPersistentEntity<?> entity, String prefix) {

		entity.doWithProperties((PropertyHandler<RelationalPersistentProperty>) property -> {

			// the referencing column of referenced entity is expected to be on the other side of the relation
			if (!property.isEntity()) {
				initSimpleColumnName(property, prefix);
			} else if (property.isEmbedded()) {
				initEmbeddedColumnNames(property, prefix);
			}
		});
	}

	private void initSimpleColumnName(RelationalPersistentProperty property, String prefix) {

		String columnName = prefix + property.getColumnName();

		columnNames.add(columnName);

		if (!entity.isIdProperty(property)) {
			nonIdColumnNames.add(columnName);
		}
		if (property.isAnnotationPresent(ReadOnlyProperty.class)) {
			readOnlyColumnNames.add(columnName);
		}
	}

	private void initEmbeddedColumnNames(RelationalPersistentProperty property, String prefix) {

		final String embeddedPrefix = property.getEmbeddedPrefix();

		final RelationalPersistentEntity<?> embeddedEntity = mappingContext
				.getRequiredPersistentEntity(property.getColumnType());

		initColumnNames(embeddedEntity, prefix + embeddedPrefix);
	}

	/**
	 * Returns a query for selecting all simple properties of an entitty, including those for one-to-one relationhships.
	 * Results are filtered using an {@code IN}-clause on the id column.
	 *
	 * @return a SQL statement. Guaranteed to be not {@code null}.
	 */
	String getFindAllInList() {
		return findAllInListSql.get();
	}

	/**
	 * Returns a query for selecting all simple properties of an entitty, including those for one-to-one relationhships.
	 *
	 * @return a SQL statement. Guaranteed to be not {@code null}.
	 */
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
	 * @param keyColumn if the property is of type {@link Map} this column contains the map key.
	 * @param ordered whether the SQL statement should include an ORDER BY for the keyColumn. If this is {@code true}, the
	 *          keyColumn must not be {@code null}.
	 * @return a SQL String.
	 */
	String getFindAllByProperty(String columnName, @Nullable String keyColumn, boolean ordered) {

		Assert.isTrue(keyColumn != null || !ordered,
				"If the SQL statement should be ordered a keyColumn to order by must be provided.");

		SelectBuilder.SelectWhere baseSelect = createBaseSelect(keyColumn);

		Table table = Table.create(entity.getTableName());
		SelectBuilder.SelectWhereAndOr withWhereClause = baseSelect
				.where(table.column(columnName).isEqualTo(SQL.bindMarker(":" + columnName)));

		SelectBuilder.BuildSelect select;
		if (ordered) {
			select = withWhereClause.orderBy(table.column(keyColumn).as(keyColumn));
		} else {
			select = withWhereClause;
		}

		return render(select);
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

	private String createFindOneSql() {

		SelectBuilder.SelectWhereAndOr withCondition = createBaseSelect()
				.where(sqlContext.getIdColumn().isEqualTo(SQL.bindMarker(":id")));

		return render(withCondition);
	}

	private Stream<String> getColumnNameStream(String prefix) {

		return StreamUtils.createStreamFromIterator(entity.iterator()) //
				.flatMap(p -> getColumnNameStream(p, prefix));
	}

	private Stream<String> getColumnNameStream(RelationalPersistentProperty p, String prefix) {

		if (p.isEntity()) {
			return sqlGeneratorSource.getSqlGenerator(p.getType()).getColumnNameStream(prefix + p.getColumnName() + "_");
		} else {
			return Stream.of(prefix + p.getColumnName());
		}
	}

	private String createFindAllSql() {
		return render(createBaseSelect());
	}

	private SelectBuilder.SelectWhere createBaseSelect() {

		return createBaseSelect(null);
	}

	private SelectBuilder.SelectWhere createBaseSelect(@Nullable String keyColumn) {

		Table table = SQL.table(entity.getTableName());

		List<Expression> columnExpressions = new ArrayList<>();

		List<Join> joinTables = new ArrayList<>();
		for (PersistentPropertyPath<RelationalPersistentProperty> path : mappingContext
				.findPersistentPropertyPaths(entity.getType(), p -> true)) {

			PersistentPropertyPathExtension extPath = new PersistentPropertyPathExtension(mappingContext, path);

			// add a join if necessary
			Join join = getJoin(extPath);
			if (join != null) {
				joinTables.add(join);
			}

			Column column = getColumn(extPath);
			if (column != null) {
				columnExpressions.add(column);
			}
		}

		if (keyColumn != null) {
			columnExpressions.add(table.column(keyColumn).as(keyColumn));
		}

		SelectBuilder.SelectAndFrom selectBuilder = StatementBuilder.select(columnExpressions);

		SelectBuilder.SelectJoin baseSelect = selectBuilder.from(table);

		for (Join join : joinTables) {
			baseSelect = baseSelect.leftOuterJoin(join.joinTable).on(join.joinColumn).equals(join.parentId);
		}

		return (SelectBuilder.SelectWhere) baseSelect;
	}

	@Nullable
	Column getColumn(PersistentPropertyPathExtension path) {

		// an embedded itself doesn't give an column, its members will though.
		// if there is a collection or map on the path it won't get selected at all, but it will get loaded with a separate
		// select
		// only the parent path is considered in order to handle arrays that get stored as BINARY properly
		if (path.isEmbedded() || path.getParentPath().isMultiValued()) {
			return null;
		}

		if (path.isEntity()) {

			// Simple entities without id include there backreference as an synthetic id in order to distinguish null entities
			// from entities with only null values.

			if (path.isQualified() //
					|| path.isCollectionLike() //
					|| path.hasIdProperty() //
			) {
				return null;
			}

			return sqlContext.getReverseColumn(path);

		}

		return sqlContext.getColumn(path);

	}

	@Nullable
	Join getJoin(PersistentPropertyPathExtension path) {

		if (!path.isEntity() || path.isEmbedded() || path.isMultiValued()) {
			return null;
		}

		Table currentTable = sqlContext.getTable(path);

		PersistentPropertyPathExtension idDefiningParentPath = path.getIdDefiningParentPath();
		Table parentTable = sqlContext.getTable(idDefiningParentPath);

		return new Join( //
				currentTable, //
				currentTable.column(path.getReverseColumnName()), //
				parentTable.column(idDefiningParentPath.getIdColumnName()) //
		);
	}

	private String createFindAllInListSql() {

		SelectBuilder.SelectWhereAndOr withCondition = createBaseSelect()
				.where(sqlContext.getIdColumn().in(SQL.bindMarker(":ids")));

		return render(withCondition);
	}

	private String render(SelectBuilder.BuildSelect select) {
		return SqlRenderer.create().render(select.build());
	}

	private String render(InsertBuilder.BuildInsert insert) {
		return SqlRenderer.create().render(insert.build());
	}

	private String render(DeleteBuilder.BuildDelete delete) {
		return SqlRenderer.create().render(delete.build());
	}

	private String render(UpdateBuilder.BuildUpdate update) {
		return SqlRenderer.create().render(update.build());
	}

	private String createExistsSql() {

		Table table = sqlContext.getTable();
		Column idColumn = table.column(entity.getIdColumn());

		SelectBuilder.BuildSelect select = StatementBuilder //
				.select(Functions.count(idColumn)) //
				.from(table) //
				.where(idColumn.isEqualTo(SQL.bindMarker(":id")));

		return render(select);
	}

	private String createCountSql() {

		Table table = SQL.table(entity.getTableName());

		SelectBuilder.BuildSelect select = StatementBuilder //
				.select(Functions.count(Expressions.asterisk())) //
				.from(table);

		return render(select);
	}

	private String createInsertSql(Set<String> additionalColumns) {

		Table table = SQL.table(entity.getTableName());

		LinkedHashSet<String> columnNamesForInsert = new LinkedHashSet<>(nonIdColumnNames);
		columnNamesForInsert.addAll(additionalColumns);
		columnNamesForInsert.removeIf(readOnlyColumnNames::contains);

		InsertBuilder.InsertIntoColumnsAndValuesWithBuild insert = Insert.builder().into(table);

		for (String cn : columnNamesForInsert) {
			insert = insert.column(table.column(cn));
		}

		InsertBuilder.InsertValuesWithBuild insertWithValues = null;
		for (String cn : columnNamesForInsert) {
			insertWithValues = (insertWithValues == null ? insert : insertWithValues)
					.values(SQL.bindMarker(":" + columnNameToParameterName(cn)));
		}

		return render(insertWithValues == null ? insert : insertWithValues);
	}

	private String createUpdateSql() {

		Table table = SQL.table(entity.getTableName());

		List<AssignValue> assignments = columnNames.stream() //
				.filter(s -> !s.equals(entity.getIdColumn())) //
				.filter(s -> !readOnlyColumnNames.contains(s)) //
				.map(columnName -> Assignments.value( //
						table.column(columnName), //
						SQL.bindMarker(":" + columnNameToParameterName(columnName)))) //
				.collect(Collectors.toList());

		UpdateBuilder.UpdateWhereAndOr update = Update.builder() //
				.table(table) //
				.set(assignments) //
				.where(table.column(entity.getIdColumn())
						.isEqualTo(SQL.bindMarker(":" + columnNameToParameterName(entity.getIdColumn())))) //
		;

		return render(update);
	}

	private String createDeleteSql() {

		Table table = SQL.table(entity.getTableName());

		DeleteBuilder.DeleteWhereAndOr delete = Delete.builder().from(table)
				.where(table.column(entity.getIdColumn()).isEqualTo(SQL.bindMarker(":id")));

		return render(delete);
	}

	String createDeleteAllSql(@Nullable PersistentPropertyPath<RelationalPersistentProperty> path) {

		Table table = SQL.table(entity.getTableName());

		DeleteBuilder.DeleteWhere deleteAll = Delete.builder().from(table);

		if (path == null) {
			return render(deleteAll);
		}
		return createDeleteByPathAndCriteria(new PersistentPropertyPathExtension(mappingContext, path), Column::isNotNull);
	}

	private String createDeleteByListSql() {

		Table table = SQL.table(entity.getTableName());

		DeleteBuilder.DeleteWhereAndOr delete = Delete.builder() //
				.from(table) //
				.where(table.column(entity.getIdColumn()).in(SQL.bindMarker(":ids")));

		return render(delete);
	}

	String createDeleteByPath(PersistentPropertyPath<RelationalPersistentProperty> path) {
		return createDeleteByPathAndCriteria(new PersistentPropertyPathExtension(mappingContext, path),
				filterColumn -> filterColumn.isEqualTo(SQL.bindMarker(":rootId")));
	}

	private String createDeleteByPathAndCriteria(PersistentPropertyPathExtension path,
			Function<Column, Condition> rootCondition) {

		Table table = SQL.table(path.getTableName());

		DeleteBuilder.DeleteWhere delete = Delete.builder() //
				.from(table);

		DeleteBuilder.DeleteWhereAndOr deleteWithWhere;

		Column filterColumn = table.column(path.getReverseColumnName());

		if (path.getLength() == 1) {

			deleteWithWhere = delete //
					.where(rootCondition.apply(filterColumn));
		} else {

			Condition condition = getSubselectCondition(path, rootCondition, filterColumn);
			deleteWithWhere = delete.where(condition);
		}
		return render(deleteWithWhere);
	}

	private Condition getSubselectCondition(PersistentPropertyPathExtension path,
			Function<Column, Condition> rootCondition, Column filterColumn) {

		PersistentPropertyPathExtension parentPath = path.getParentPath();

		Table subSelectTable = SQL.table(parentPath.getTableName());
		Column idColumn = subSelectTable.column(parentPath.getIdColumnName());
		Column selectFilterColumn = subSelectTable.column(parentPath.getEffectiveIdColumnName());

		Condition innerCondition = parentPath.getLength() == 1 ? rootCondition.apply(selectFilterColumn)
				: getSubselectCondition(parentPath, rootCondition, selectFilterColumn);

		Select select = Select.builder() //
				.select(idColumn) //
				.from(subSelectTable) //
				.where(innerCondition).build();

		return filterColumn.in(select);
	}

	private String columnNameToParameterName(String columnName) {
		return parameterPattern.matcher(columnName).replaceAll("");
	}

	@Value
	class Join {
		Table joinTable;
		Column joinColumn;
		Column parentId;
	}

}
