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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.data.jdbc.repository.support.SimpleJdbcRepository;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.StreamUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Generates SQL statements to be used by {@link SimpleJdbcRepository}
 *
 * @author Jens Schauder
 * @author Yoichi Imai
 */
class SqlGenerator {

	private final RelationalPersistentEntity<?> entity;
	private final RelationalMappingContext context;
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

	SqlGenerator(RelationalMappingContext context, RelationalPersistentEntity<?> entity,
			SqlGeneratorSource sqlGeneratorSource) {

		this.context = context;
		this.entity = entity;
		this.sqlGeneratorSource = sqlGeneratorSource;
		initColumnNames();
	}

	private void initColumnNames() {

		entity.doWithProperties((PropertyHandler<RelationalPersistentProperty>) p -> {
			// the referencing column of referenced entity is expected to be on the other side of the relation
			if (!p.isEntity()) {
				columnNames.add(p.getColumnName());
				if (!entity.isIdProperty(p)) {
					nonIdColumnNames.add(p.getColumnName());
				}
			}
		});
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

		String baseSelect = (keyColumn != null) //
				? createSelectBuilder().column(cb -> cb //
						.tableAlias(entity.getTableName()) //
						.column(keyColumn) //
						.as(keyColumn) //
				).build()
				: getFindAll();

		String orderBy = ordered ? " ORDER BY " + keyColumn : "";

		return String.format("%s WHERE %s = :%s%s", baseSelect, columnName, columnName, orderBy);
	}

	String getFindAllByProperty(PersistentPropertyPath<RelationalPersistentProperty> path) {
		return new Variant(path).getFindAllByProperty();
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

	/**
	 * Create a SelectBuilder that is appropriate if the entity in question is the aggregate root, i.e. it's id consists
	 * only of it's own id.
	 *
	 * @return a {@link SelectBuilder}. Guaranteed to be not {@code null}.
	 */
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

		for (RelationalPersistentProperty property : entity) {
			if (!isSimpleEntity(property) //
			) {
				continue;
			}

			RelationalPersistentEntity<?> refEntity = context.getRequiredPersistentEntity(property.getActualType());
			String joinAlias = property.getName();

			// TODO: joins by back reference and id of parent
			// but that id might not exist and be instead a list of backreferences and keys in a hierarchy of maps/lists and
			// references.
			builder.join(jb -> buildJoin(property, refEntity, joinAlias, jb));

			for (RelationalPersistentProperty refProperty : refEntity) {
				builder.column( //
						cb -> cb.tableAlias(joinAlias) //
								.column(refProperty.getColumnName()) //
								.as(joinAlias + "_" + refProperty.getColumnName()) //
				);
			}

			// if the referenced property doesn't have an id, include the back reference in the select list.
			// this enables determining if the referenced entity is present or null.
			if (!refEntity.hasIdProperty()) {

				builder.column( //
						cb -> cb.tableAlias(joinAlias) //
								.column(property.getReverseColumnName()) //
								.as(joinAlias + "_" + property.getReverseColumnName()) //
				);
			}
		}
	}

	private SelectBuilder.Join.JoinBuilder buildJoin(RelationalPersistentProperty property,
			RelationalPersistentEntity<?> referenced, String joinAlias, SelectBuilder.Join.JoinBuilder jb) {

		SelectBuilder.Join.JoinBuilder joinBuilder = jb.leftOuter().table(referenced.getTableName()).as(joinAlias);

		if (entity.hasIdProperty()) {
			return joinBuilder.where(property.getReverseColumnName()).eq().column(entity.getTableName(),
					entity.getIdColumn());
		} else {
			return joinBuilder.where("intermediate").eq().column(entity.getTableName(), "backref") //
					.and("intermediate_key").eq().column(entity.getTableName(), "backref_key");
		}

	}

	private void addColumnsForSimpleProperties(SelectBuilder builder) {

		for (RelationalPersistentProperty property : entity) {

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

	private Stream<String> getColumnNameStream(RelationalPersistentProperty p, String prefix) {

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

		String parameterNames = columnNamesForInsert.stream()//
				.map(n -> String.format(":%s", n))//
				.collect(Collectors.joining(", "));

		return String.format(insertTemplate, entity.getTableName(), tableColumns, parameterNames);
	}

	private String createUpdateSql() {

		String updateTemplate = "UPDATE %s SET %s WHERE %s = :%s";

		String setClause = columnNames.stream() //
				.filter(s -> !s.equals(entity.getIdColumn())) //
				.map(n -> String.format("%s = :%s", n, n)) //
				.collect(Collectors.joining(", "));

		return String.format(updateTemplate, entity.getTableName(), setClause, entity.getIdColumn(), entity.getIdColumn());
	}

	private String createDeleteSql() {
		return String.format("DELETE FROM %s WHERE %s = :id", entity.getTableName(), entity.getIdColumn());
	}

	String createDeleteAllSql(@Nullable PersistentPropertyPath<RelationalPersistentProperty> path) {

		if (path == null) {
			return String.format("DELETE FROM %s", entity.getTableName());
		}

		RelationalPersistentEntity<?> entityToDelete = context
				.getRequiredPersistentEntity(path.getRequiredLeafProperty().getActualType());

		RelationalPersistentProperty property = path.getBaseProperty();

		String innerMostCondition = String.format("%s IS NOT NULL", property.getReverseColumnName());

		String condition = cascadeConditions(innerMostCondition, getSubPath(path));

		return String.format("DELETE FROM %s WHERE %s", entityToDelete.getTableName(), condition);
	}

	private String createDeleteByListSql() {
		return String.format("DELETE FROM %s WHERE %s IN (:ids)", entity.getTableName(), entity.getIdColumn());
	}

	String createDeleteByPath(PersistentPropertyPath<RelationalPersistentProperty> path) {

		RelationalPersistentEntity<?> entityToDelete = context
				.getRequiredPersistentEntity(path.getRequiredLeafProperty().getActualType());
		RelationalPersistentProperty property = path.getBaseProperty();

		String innerMostCondition = String.format("%s = :rootId", property.getReverseColumnName());

		String condition = cascadeConditions(innerMostCondition, getSubPath(path));

		return String.format("DELETE FROM %s WHERE %s", entityToDelete.getTableName(), condition);
	}

	private PersistentPropertyPath<RelationalPersistentProperty> getSubPath(
			PersistentPropertyPath<RelationalPersistentProperty> path) {

		int pathLength = path.getLength();

		PersistentPropertyPath<RelationalPersistentProperty> ancestor = path;

		for (int i = pathLength - 1; i > 0; i--) {
			ancestor = path.getParentPath();
		}

		return path.getExtensionForBaseOf(ancestor);
	}

	private String cascadeConditions(String innerCondition, PersistentPropertyPath<RelationalPersistentProperty> path) {

		if (path.getLength() == 0) {
			return innerCondition;
		}

		RelationalPersistentEntity<?> entity = context
				.getRequiredPersistentEntity(path.getBaseProperty().getOwner().getTypeInformation());
		RelationalPersistentProperty property = path.getRequiredLeafProperty();

		return String.format("%s IN (SELECT %s FROM %s WHERE %s)", //
				property.getReverseColumnName(), //
				entity.getIdColumn(), //
				entity.getTableName(), innerCondition //
		);
	}

	// TODO: instances need cashing
	// TODO: shitty name
	private class Variant {

		private final PersistentPropertyPath<RelationalPersistentProperty> path;
		private final List<PersistentPropertyPath<RelationalPersistentProperty>> keyContributingPaths;

		public Variant(PersistentPropertyPath<RelationalPersistentProperty> path) {

			// collect keys that are essentially the map keys and list indexes accumulated along the path.
			List<PersistentPropertyPath<RelationalPersistentProperty>> keyContributingPaths = new ArrayList<>();
			PersistentPropertyPath<RelationalPersistentProperty> subPath = path;
			while (!subPath.isEmpty()) {

				if (subPath.getRequiredLeafProperty().isQualified()) {
					keyContributingPaths.add(subPath);
				}

				subPath = subPath.getParentPath();
			}

			Collections.reverse(keyContributingPaths);

			this.path = path;
			this.keyContributingPaths = keyContributingPaths;
		}

		public String getFindAllByProperty() {

			RelationalPersistentProperty leafProperty = path.getRequiredLeafProperty();

			PersistentPropertyPath<RelationalPersistentProperty> parentPath = path.getParentPath();

			StringJoiner whereClauseJoiner = new StringJoiner(" AND ");

			RelationalPersistentProperty baseProperty = path.getBaseProperty();
			String relativeRootId = baseProperty.getReverseColumnName();

			whereClauseJoiner.add(entity.getTableName() + "." + relativeRootId + " = :" + relativeRootId);

			while (!parentPath.isEmpty()) {

				String keyColumn = parentPath.getRequiredLeafProperty().getKeyColumn();
				whereClauseJoiner.add(keyColumn + " = :" + keyColumn);

				parentPath = parentPath.getParentPath();
			}

			String baseSelect = (leafProperty.isQualified()) //
					? createSelectBuilder().column(cb -> {
						String keyColumn = leafProperty.getKeyColumn();
						return cb.tableAlias(entity.getTableName()).column(keyColumn).as(keyColumn);
					}).build()
					: getFindAll();

			String orderBy = leafProperty.isOrdered() ? " ORDER BY " + leafProperty.getKeyColumn() : "";

			return String.format("%s WHERE %s%s", baseSelect, whereClauseJoiner.toString(), orderBy);
		}

		/**
		 * Create a SelectBuilder that is appropriate if the entity in question is the aggregate root, i.e. it's id consists
		 * only of it's own id.
		 *
		 * @return a {@link SelectBuilder}. Guaranteed to be not {@code null}.
		 */
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

			for (RelationalPersistentProperty property : entity) {

				if (isSimpleEntity(property)) {

					RelationalPersistentEntity<?> refEntity = context.getRequiredPersistentEntity(property.getActualType());
					String joinAlias = property.getName();

					addJoin(builder, property, refEntity, joinAlias);
					addColumns(builder, refEntity, joinAlias);
					addNullCheckingColumn(builder, property, refEntity, joinAlias);

				}
			}
		}

		private void addNullCheckingColumn(SelectBuilder builder, RelationalPersistentProperty property,
				RelationalPersistentEntity<?> refEntity, String joinAlias) {

			// if the referenced property doesn't have an id, include the back reference in the select list.
			// this enables determining if the referenced entity is present or null.
			if (!refEntity.hasIdProperty()) {

				builder.column( //
						cb -> cb.tableAlias(joinAlias) //
								.column(property.getReverseColumnName(path, null)) // todo: null is wrong, we need a path here
								.as(joinAlias + "_" + property.getReverseColumnName()) //
				);
			}
		}

		private void addColumns(SelectBuilder builder, RelationalPersistentEntity<?> refEntity, String joinAlias) {

			for (RelationalPersistentProperty refProperty : refEntity) {
				builder.column( //
						cb -> cb.tableAlias(joinAlias) //
								.column(refProperty.getColumnName()) //
								.as(joinAlias + "_" + refProperty.getColumnName()) //
				);
			}
		}

		private void addJoin(SelectBuilder builder, RelationalPersistentProperty property,
				RelationalPersistentEntity<?> refEntity, String joinAlias) {
			builder.join(jb -> buildJoin(property, refEntity, joinAlias, jb));
		}

		private SelectBuilder.Join.JoinBuilder buildJoin(RelationalPersistentProperty property,
				RelationalPersistentEntity<?> referenced, String joinAlias, SelectBuilder.Join.JoinBuilder jb) {

			// property: child
			// referenced NoIdChild
			// entity: NoIdIntermediate

			SelectBuilder.Join.JoinBuilder joinBuilder = jb.leftOuter().table(referenced.getTableName()).as(joinAlias);

			if (entity.hasIdProperty()) {
				return joinBuilder.where(property.getReverseColumnName()).eq().column(entity.getTableName(),
						entity.getIdColumn());
			} else {

				// todo: passing null in the next line is just a temporary workaround befor we turn this into using paths.
				joinBuilder = joinBuilder.where(property.getReverseColumnName(path, null)).eq().column(entity.getTableName(),
						path.getRequiredLeafProperty().getReverseColumnName());

				for (PersistentPropertyPath<RelationalPersistentProperty> keyContributingPath : keyContributingPaths) {

					String keyColumn = property.getKeyColumn(keyContributingPath);
					joinBuilder = joinBuilder.where(keyColumn).eq().column(entity.getTableName(),
							keyContributingPath.getRequiredLeafProperty().getKeyColumn(keyContributingPath));
				}

				return joinBuilder;
			}

		}
	}

	private boolean isSimpleEntity(RelationalPersistentProperty property) {

		return property.isEntity() //
				&& !Collection.class.isAssignableFrom(property.getType()) //
				&& !Map.class.isAssignableFrom(property.getType());
	}
}
