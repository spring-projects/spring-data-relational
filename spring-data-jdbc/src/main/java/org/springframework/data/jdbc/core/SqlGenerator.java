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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.springframework.data.annotation.ReadOnlyProperty;
import org.springframework.data.jdbc.repository.support.SimpleJdbcRepository;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.*;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Generates SQL statements to be used by {@link SimpleJdbcRepository}
 *
 * @author Jens Schauder
 * @author Yoichi Imai
 * @author Bastian Wilhelm
 * @author Oleksandr Kucher
 * @author Mark Paluch
 */
class SqlGenerator {

	private static final Pattern parameterPattern = Pattern.compile("\\W");

	private final RelationalPersistentEntity<?> entity;
	private final MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty> mappingContext;

	private final SqlContext sqlContext;
	private final Columns columns;

	private final Lazy<String> findOneSql = Lazy.of(this::createFindOneSql);
	private final Lazy<String> findAllSql = Lazy.of(this::createFindAllSql);
	private final Lazy<String> findAllInListSql = Lazy.of(this::createFindAllInListSql);

	private final Lazy<String> existsSql = Lazy.of(this::createExistsSql);
	private final Lazy<String> countSql = Lazy.of(this::createCountSql);

	private final Lazy<String> updateSql = Lazy.of(this::createUpdateSql);

	private final Lazy<String> deleteByIdSql = Lazy.of(this::createDeleteSql);
	private final Lazy<String> deleteByListSql = Lazy.of(this::createDeleteByListSql);

	/**
	 * Create a new {@link SqlGenerator} given {@link RelationalMappingContext} and {@link RelationalPersistentEntity}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 */
	SqlGenerator(RelationalMappingContext mappingContext, RelationalPersistentEntity<?> entity) {

		this.mappingContext = mappingContext;
		this.entity = entity;
		this.sqlContext = new SqlContext(entity);
		this.columns = new Columns(entity, mappingContext);
	}

	/**
	 * Construct a IN-condition based on a {@link Select Sub-Select} which selects the ids (or stand ins for ids) of the
	 * given {@literal path} to those that reference the root entities specified by the {@literal rootCondition}.
	 *
	 * @param path specifies the table and id to select
	 * @param rootCondition the condition on the root of the path determining what to select
	 * @param filterColumn the column to apply the IN-condition to.
	 * @return the IN condition
	 */
	private static Condition getSubselectCondition(PersistentPropertyPathExtension path,
			Function<Column, Condition> rootCondition, Column filterColumn) {

		PersistentPropertyPathExtension parentPath = path.getParentPath();

		if (!parentPath.hasIdProperty()) {
			if (parentPath.getLength() > 1) {
				return getSubselectCondition(parentPath, rootCondition, filterColumn);
			}
			return rootCondition.apply(filterColumn);
		}

		Table subSelectTable = SQL.table(parentPath.getTableName());
		Column idColumn = subSelectTable.column(parentPath.getIdColumnName());
		Column selectFilterColumn = subSelectTable.column(parentPath.getEffectiveIdColumnName());

		Condition innerCondition = parentPath.getLength() == 1 // if the parent is the root of the path
				? rootCondition.apply(selectFilterColumn) // apply the rootCondition
				: getSubselectCondition(parentPath, rootCondition, selectFilterColumn); // otherwise we need another layer of
																																								// subselect

		Select select = Select.builder() //
				.select(idColumn) //
				.from(subSelectTable) //
				.where(innerCondition).build();

		return filterColumn.in(select);
	}

	private static BindMarker getBindMarker(String columnName) {
		return SQL.bindMarker(":" + parameterPattern.matcher(columnName).replaceAll(""));
	}

	/**
	 * Returns a query for selecting all simple properties of an entity, including those for one-to-one relationships.
	 * Results are filtered using an {@code IN}-clause on the id column.
	 *
	 * @return a SQL statement. Guaranteed to be not {@code null}.
	 */
	String getFindAllInList() {
		return findAllInListSql.get();
	}

	/**
	 * Returns a query for selecting all simple properties of an entity, including those for one-to-one relationships.
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

		SelectBuilder.SelectWhere builder = selectBuilder(
				keyColumn == null ? Collections.emptyList() : Collections.singleton(keyColumn));

		Table table = getTable();
		SelectBuilder.SelectWhereAndOr withWhereClause = builder
				.where(table.column(columnName).isEqualTo(getBindMarker(columnName)));

		Select select;
		if (ordered) {
			select = withWhereClause.orderBy(table.column(keyColumn).as(keyColumn)).build();
		} else {
			select = withWhereClause.build();
		}

		return render(select);
	}

	/**
	 * Create a {@code SELECT COUNT(id) FROM … WHERE :id = …} statement.
	 *
	 * @return
	 */
	String getExists() {
		return existsSql.get();
	}

	/**
	 * Create a {@code SELECT … FROM … WHERE :id = …} statement.
	 *
	 * @return
	 */
	String getFindOne() {
		return findOneSql.get();
	}

	/**
	 * Create a {@code INSERT INTO … (…) VALUES(…)} statement.
	 *
	 * @return
	 */
	String getInsert(Set<String> additionalColumns) {
		return createInsertSql(additionalColumns);
	}

	/**
	 * Create a {@code UPDATE … SET …} statement.
	 *
	 * @return
	 */
	String getUpdate() {
		return updateSql.get();
	}

	/**
	 * Create a {@code SELECT COUNT(*) FROM …} statement.
	 *
	 * @return
	 */
	String getCount() {
		return countSql.get();
	}

	/**
	 * Create a {@code DELETE FROM … WHERE :id = …} statement.
	 *
	 * @return
	 */
	String getDeleteById() {
		return deleteByIdSql.get();
	}

	/**
	 * Create a {@code DELETE FROM … WHERE :ids in (…)} statement.
	 *
	 * @return
	 */
	String getDeleteByList() {
		return deleteByListSql.get();
	}

	/**
	 * Create a {@code DELETE} query and optionally filter by {@link PersistentPropertyPath}.
	 *
	 * @param path can be {@literal null}.
	 * @return
	 */
	String createDeleteAllSql(@Nullable PersistentPropertyPath<RelationalPersistentProperty> path) {

		Table table = getTable();

		DeleteBuilder.DeleteWhere deleteAll = Delete.builder().from(table);

		if (path == null) {
			return render(deleteAll.build());
		}

		return createDeleteByPathAndCriteria(new PersistentPropertyPathExtension(mappingContext, path), Column::isNotNull);
	}

	/**
	 * Create a {@code DELETE} query and filter by {@link PersistentPropertyPath}.
	 *
	 * @param path must not be {@literal null}.
	 * @return
	 */
	String createDeleteByPath(PersistentPropertyPath<RelationalPersistentProperty> path) {
		return createDeleteByPathAndCriteria(new PersistentPropertyPathExtension(mappingContext, path),
				filterColumn -> filterColumn.isEqualTo(getBindMarker("rootId")));
	}

	private String createFindOneSql() {

		Select select = selectBuilder().where(getIdColumn().isEqualTo(getBindMarker("id"))) //
				.build();

		return render(select);
	}

	private String createFindAllSql() {
		return render(selectBuilder().build());
	}

	private SelectBuilder.SelectWhere selectBuilder() {
		return selectBuilder(Collections.emptyList());
	}

	private SelectBuilder.SelectWhere selectBuilder(Collection<String> keyColumns) {

		Table table = getTable();

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

		for (String keyColumn : keyColumns) {
			columnExpressions.add(table.column(keyColumn).as(keyColumn));
		}

		SelectBuilder.SelectAndFrom selectBuilder = StatementBuilder.select(columnExpressions);
		SelectBuilder.SelectJoin baseSelect = selectBuilder.from(table);

		for (Join join : joinTables) {
			baseSelect = baseSelect.leftOuterJoin(join.joinTable).on(join.joinColumn).equals(join.parentId);
		}

		return (SelectBuilder.SelectWhere) baseSelect;
	}

	/**
	 * Create a {@link Column} for {@link PersistentPropertyPathExtension}.
	 *
	 * @param path
	 * @return
	 */
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

		Select select = selectBuilder().where(getIdColumn().in(getBindMarker("ids"))).build();

		return render(select);
	}

	private String createExistsSql() {

		Table table = getTable();

		Select select = StatementBuilder //
				.select(Functions.count(getIdColumn())) //
				.from(table) //
				.where(getIdColumn().isEqualTo(getBindMarker("id"))) //
				.build();

		return render(select);
	}

	private String createCountSql() {

		Table table = getTable();

		Select select = StatementBuilder //
				.select(Functions.count(Expressions.asterisk())) //
				.from(table) //
				.build();

		return render(select);
	}

	private String createInsertSql(Set<String> additionalColumns) {

		Table table = getTable();

		Set<String> columnNamesForInsert = new LinkedHashSet<>(columns.getInsertableColumns());
		columnNamesForInsert.addAll(additionalColumns);

		InsertBuilder.InsertIntoColumnsAndValuesWithBuild insert = Insert.builder().into(table);

		for (String cn : columnNamesForInsert) {
			insert = insert.column(table.column(cn));
		}

		InsertBuilder.InsertValuesWithBuild insertWithValues = null;
		for (String cn : columnNamesForInsert) {
			insertWithValues = (insertWithValues == null ? insert : insertWithValues).values(getBindMarker(cn));
		}

		return render(insertWithValues == null ? insert.build() : insertWithValues.build());
	}

	private String createUpdateSql() {

		Table table = getTable();

		List<AssignValue> assignments = columns.getUpdateableColumns() //
				.stream() //
				.map(columnName -> Assignments.value( //
						table.column(columnName), //
						getBindMarker(columnName))) //
				.collect(Collectors.toList());

		Update update = Update.builder() //
				.table(table) //
				.set(assignments) //
				.where(getIdColumn().isEqualTo(getBindMarker(entity.getIdColumn()))) //
				.build();

		return render(update);
	}

	private String createDeleteSql() {

		Table table = getTable();

		Delete delete = Delete.builder().from(table).where(getIdColumn().isEqualTo(SQL.bindMarker(":id"))) //
				.build();

		return render(delete);
	}

	private String createDeleteByPathAndCriteria(PersistentPropertyPathExtension path,
			Function<Column, Condition> rootCondition) {

		Table table = SQL.table(path.getTableName());

		DeleteBuilder.DeleteWhere builder = Delete.builder() //
				.from(table);
		Delete delete;

		Column filterColumn = table.column(path.getReverseColumnName());

		if (path.getLength() == 1) {

			delete = builder //
					.where(rootCondition.apply(filterColumn)) //
					.build();
		} else {

			Condition condition = getSubselectCondition(path, rootCondition, filterColumn);
			delete = builder.where(condition).build();
		}

		return render(delete);
	}

	private String createDeleteByListSql() {

		Table table = getTable();

		Delete delete = Delete.builder() //
				.from(table) //
				.where(getIdColumn().in(getBindMarker("ids"))) //
				.build();

		return render(delete);
	}

	private String render(Select select) {
		return SqlRenderer.create().render(select);
	}

	private String render(Insert insert) {
		return SqlRenderer.create().render(insert);
	}

	private String render(Update update) {
		return SqlRenderer.create().render(update);
	}

	private String render(Delete delete) {
		return SqlRenderer.create().render(delete);
	}

	private Table getTable() {
		return sqlContext.getTable();
	}

	private Column getIdColumn() {
		return sqlContext.getIdColumn();
	}

	/**
	 * Value object representing a {@code JOIN} association.
	 */
	@Value
	static class Join {
		Table joinTable;
		Column joinColumn;
		Column parentId;
	}

	/**
	 * Value object encapsulating column name caches.
	 *
	 * @author Mark Paluch
	 */
	static class Columns {

		private final MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty> mappingContext;

		private final List<String> columnNames = new ArrayList<>();
		private final List<String> idColumnNames = new ArrayList<>();
		private final List<String> nonIdColumnNames = new ArrayList<>();
		private final Set<String> readOnlyColumnNames = new HashSet<>();
		private final Set<String> insertableColumns;
		private final Set<String> updateableColumns;

		Columns(RelationalPersistentEntity<?> entity,
				MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty> mappingContext) {

			this.mappingContext = mappingContext;

			populateColumnNameCache(entity, "");

			Set<String> insertable = new LinkedHashSet<>(nonIdColumnNames);
			insertable.removeAll(readOnlyColumnNames);

			this.insertableColumns = Collections.unmodifiableSet(insertable);

			Set<String> updateable = new LinkedHashSet<>(columnNames);

			updateable.removeAll(idColumnNames);
			updateable.removeAll(readOnlyColumnNames);

			this.updateableColumns = Collections.unmodifiableSet(updateable);
		}

		private void populateColumnNameCache(RelationalPersistentEntity<?> entity, String prefix) {

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

			if (!property.getOwner().isIdProperty(property)) {
				nonIdColumnNames.add(columnName);
			} else {
				idColumnNames.add(columnName);
			}

			if (!property.isWritable() || property.isAnnotationPresent(ReadOnlyProperty.class)) {
				readOnlyColumnNames.add(columnName);
			}
		}

		private void initEmbeddedColumnNames(RelationalPersistentProperty property, String prefix) {

			String embeddedPrefix = property.getEmbeddedPrefix();

			RelationalPersistentEntity<?> embeddedEntity = mappingContext
					.getRequiredPersistentEntity(property.getColumnType());

			populateColumnNameCache(embeddedEntity, prefix + embeddedPrefix);
		}

		/**
		 * @return Column names that can be used for {@code INSERT}.
		 */
		Set<String> getInsertableColumns() {
			return insertableColumns;
		}

		/**
		 * @return Column names that can be used for {@code UPDATE}.
		 */
		Set<String> getUpdateableColumns() {
			return updateableColumns;
		}
	}
}
