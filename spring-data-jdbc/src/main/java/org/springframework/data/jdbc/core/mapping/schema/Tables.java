/*
 * Copyright 2023-2024 the original author or authors.
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
package org.springframework.data.jdbc.core.mapping.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.mapping.MappedCollection;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Model class that contains Table/Column information that can be used to generate SQL for Schema generation.
 *
 * @author Kurt Niemi
 * @author Evgenii Koba
 * @since 3.2
 */
record Tables(List<Table> tables) {

	public static Tables from(RelationalMappingContext context) {
		return from(context.getPersistentEntities().stream(), new DefaultSqlTypeMapping(), null, context);
	}

	// TODO: Add support (i.e. create tickets) to support entities, embedded properties, and aggregate references.

	public static Tables from(Stream<? extends RelationalPersistentEntity<?>> persistentEntities,
			SqlTypeMapping sqlTypeMapping, @Nullable String defaultSchema,
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context) {

		List<ForeignKeyMetadata> foreignKeyMetadataList = new ArrayList<>();
		List<Table> tables = persistentEntities
				.filter(it -> it.isAnnotationPresent(org.springframework.data.relational.core.mapping.Table.class)) //
				.map(entity -> {

					Table table = new Table(defaultSchema, entity.getTableName().getReference());

					Set<RelationalPersistentProperty> identifierColumns = new LinkedHashSet<>();
					entity.getPersistentProperties(Id.class).forEach(identifierColumns::add);

					for (RelationalPersistentProperty property : entity) {

						if (property.isEntity() && !property.isEmbedded()) {
							foreignKeyMetadataList.add(createForeignKeyMetadata(entity, property, context, sqlTypeMapping));
							continue;
						}

						Column column = new Column(property.getColumnName().getReference(), sqlTypeMapping.getColumnType(property),
								sqlTypeMapping.isNullable(property), identifierColumns.contains(property));
						table.columns().add(column);
					}
					return table;
				}).collect(Collectors.toList());

		applyForeignKeyMetadata(tables, foreignKeyMetadataList);

		return new Tables(tables);
	}

	public static Tables empty() {
		return new Tables(Collections.emptyList());
	}

	/**
	 * Apply all information we know about foreign keys to correctly create foreign and primary keys
	 */
	private static void applyForeignKeyMetadata(List<Table> tables, List<ForeignKeyMetadata> foreignKeyMetadataList) {

		foreignKeyMetadataList.forEach(foreignKeyMetadata -> {

			Table table = tables.stream().filter(t -> t.name().equals(foreignKeyMetadata.tableName)).findAny().orElseThrow();

			List<Column> parentIdColumns = collectParentIdentityColumns(foreignKeyMetadata, foreignKeyMetadataList, tables);
			List<String> parentIdColumnNames = parentIdColumns.stream().map(Column::name).toList();

			String foreignKeyName = getForeignKeyName(foreignKeyMetadata.parentTableName, parentIdColumnNames);
			if (parentIdColumnNames.size() == 1) {

				addIfAbsent(table.columns(), new Column(foreignKeyMetadata.referencingColumnName(),
						parentIdColumns.get(0).type(), false, table.getIdColumns().isEmpty()));
				if (foreignKeyMetadata.keyColumnName() != null) {
					addIfAbsent(table.columns(),
							new Column(foreignKeyMetadata.keyColumnName(), foreignKeyMetadata.keyColumnType(), false, true));
				}
				addIfAbsent(table.foreignKeys(),
						new ForeignKey(foreignKeyName, foreignKeyMetadata.tableName(),
								List.of(foreignKeyMetadata.referencingColumnName()), foreignKeyMetadata.parentTableName(),
								parentIdColumnNames));
			} else {

				addIfAbsent(table.columns(), parentIdColumns.toArray(new Column[0]));
				addIfAbsent(table.columns(),
						new Column(foreignKeyMetadata.keyColumnName(), foreignKeyMetadata.keyColumnType(), false, true));
				addIfAbsent(table.foreignKeys(), new ForeignKey(foreignKeyName, foreignKeyMetadata.tableName(),
						parentIdColumnNames, foreignKeyMetadata.parentTableName(), parentIdColumnNames));
			}

		});
	}

	private static <E> void addIfAbsent(List<E> list, E... elements) {

		for (E element : elements) {
			if (!list.contains(element)) {
				list.add(element);
			}
		}
	}

	private static List<Column> collectParentIdentityColumns(ForeignKeyMetadata child,
			List<ForeignKeyMetadata> foreignKeyMetadataList, List<Table> tables) {
		return collectParentIdentityColumns(child, foreignKeyMetadataList, tables, new HashSet<>());
	}

	private static List<Column> collectParentIdentityColumns(ForeignKeyMetadata child,
			List<ForeignKeyMetadata> foreignKeyMetadataList, List<Table> tables, Set<String> excludeTables) {

		excludeTables.add(child.tableName());

		Table parentTable = findTableByName(tables, child.parentTableName());
		ForeignKeyMetadata parentMetadata = findMetadataByTableName(foreignKeyMetadataList, child.parentTableName(),
				excludeTables);
		List<Column> parentIdColumns = parentTable.getIdColumns();

		if (!parentIdColumns.isEmpty()) {
			return new ArrayList<>(parentIdColumns);
		}

		Assert.state(parentMetadata != null, "parentMetadata must not be null at this stage");

		List<Column> parentParentIdColumns = collectParentIdentityColumns(parentMetadata, foreignKeyMetadataList, tables);
		if (parentParentIdColumns.size() == 1) {
			Column parentParentIdColumn = parentParentIdColumns.get(0);
			Column withChangedName = new Column(parentMetadata.referencingColumnName, parentParentIdColumn.type(), false,
					true);
			parentParentIdColumns = new LinkedList<>(List.of(withChangedName));
		}
		if (parentMetadata.keyColumnName() != null) {
			parentParentIdColumns
					.add(new Column(parentMetadata.keyColumnName(), parentMetadata.keyColumnType(), false, true));
		}
		return parentParentIdColumns;
	}

	@Nullable
	private static Table findTableByName(List<Table> tables, String tableName) {
		return tables.stream().filter(table -> table.name().equals(tableName)).findAny().orElse(null);
	}

	@Nullable
	private static ForeignKeyMetadata findMetadataByTableName(List<ForeignKeyMetadata> metadata, String tableName,
			Set<String> excludeTables) {

		return metadata.stream()
				.filter(m -> m.tableName().equals(tableName) && !excludeTables.contains(m.parentTableName())).findAny()
				.orElse(null);
	}

	private static ForeignKeyMetadata createForeignKeyMetadata(RelationalPersistentEntity<?> entity,
			RelationalPersistentProperty property,
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			SqlTypeMapping sqlTypeMapping) {

		RelationalPersistentEntity childEntity = context.getRequiredPersistentEntity(property.getActualType());

		String referencedKeyColumnType = null;
		if (property.isAnnotationPresent(MappedCollection.class)) {
			if (property.getType() == List.class) {
				referencedKeyColumnType = sqlTypeMapping.getColumnType(Integer.class);
			} else if (property.getType() == Map.class) {
				referencedKeyColumnType = sqlTypeMapping.getColumnType(property.getComponentType());
			}
		}

		return new ForeignKeyMetadata(childEntity.getTableName().getReference(),
				property.getReverseColumnName(entity).getReference(),
				Optional.ofNullable(property.getKeyColumn()).map(SqlIdentifier::getReference).orElse(null),
				referencedKeyColumnType, entity.getTableName().getReference());
	}

	private static String getForeignKeyName(String referencedTableName, List<String> referencedColumnNames) {
		return String.format("%s_%s_fk", referencedTableName, String.join("_", referencedColumnNames));
	}

	private record ForeignKeyMetadata(String tableName, String referencingColumnName, @Nullable String keyColumnName,
			@Nullable String keyColumnType, String parentTableName) {

	}
}
