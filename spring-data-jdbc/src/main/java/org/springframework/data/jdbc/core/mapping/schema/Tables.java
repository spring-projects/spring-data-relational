/*
 * Copyright 2023 the original author or authors.
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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.mapping.MappedCollection;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;

/**
 * Model class that contains Table/Column information that can be used to generate SQL for Schema generation.
 *
 * @author Kurt Niemi
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

		Map<String, List<ColumnWithForeignKey>> colAndFKByTableName = new HashMap<>();
		List<Table> tables = persistentEntities
				.filter(it -> it.isAnnotationPresent(org.springframework.data.relational.core.mapping.Table.class)) //
				.map(entity -> {

					Table table = new Table(defaultSchema, entity.getTableName().getReference());

					Set<RelationalPersistentProperty> identifierColumns = new LinkedHashSet<>();
					entity.getPersistentProperties(Id.class).forEach(identifierColumns::add);
					collectForeignKeysInfo(entity, context, colAndFKByTableName, sqlTypeMapping);

					for (RelationalPersistentProperty property : entity) {

						if (property.isEntity() && !property.isEmbedded()) {
							continue;
						}

						Column column = new Column(property.getColumnName().getReference(), sqlTypeMapping.getColumnType(property),
												   sqlTypeMapping.isNullable(property), identifierColumns.contains(property));
						table.columns().add(column);
					}
					return table;
				}).collect(Collectors.toList());

		applyForeignKeys(tables, colAndFKByTableName);

		return new Tables(tables);
	}

	public static Tables empty() {
		return new Tables(Collections.emptyList());
	}

	private static void applyForeignKeys(List<Table> tables,
			Map<String, List<ColumnWithForeignKey>> colAndFKByTableName) {

		colAndFKByTableName.forEach(
				(tableName, colsAndFK) -> tables.stream().filter(table -> table.name().equals(tableName)).forEach(table -> {

					colsAndFK.forEach(colAndFK -> {
						if (!table.columns().contains(colAndFK.column())) {
							table.columns().add(colAndFK.column());
						}
					});

					colsAndFK.forEach(colAndFK -> table.foreignKeys().add(colAndFK.foreignKey()));
				}));
	}

	private static void collectForeignKeysInfo(RelationalPersistentEntity<?> entity,
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			Map<String, List<ColumnWithForeignKey>> keyColumnsByTableName, SqlTypeMapping sqlTypeMapping) {

		RelationalPersistentProperty identifierColumn = entity.getPersistentProperty(Id.class);

		entity.getPersistentProperties(MappedCollection.class).forEach(property -> {
			if (property.isEntity()) {
				property.getPersistentEntityTypeInformation().forEach(typeInformation -> {

					String tableName = context.getRequiredPersistentEntity(typeInformation).getTableName().getReference();
					String columnName = property.getReverseColumnName(entity).getReference();
					String referencedTableName = entity.getTableName().getReference();
					String referencedColumnName = identifierColumn.getColumnName().getReference();

					ForeignKey foreignKey = new ForeignKey(getForeignKeyName(referencedTableName, referencedColumnName),
							tableName, columnName, referencedTableName, referencedColumnName);
					Column column = new Column(columnName, sqlTypeMapping.getColumnType(identifierColumn), true, false);

					ColumnWithForeignKey columnWithForeignKey = new ColumnWithForeignKey(column, foreignKey);
					keyColumnsByTableName.compute(
							context.getRequiredPersistentEntity(typeInformation).getTableName().getReference(), (key, value) -> {
								if (value == null) {
									return new ArrayList<>(List.of(columnWithForeignKey));
								} else {
									value.add(columnWithForeignKey);
									return value;
								}
							});
				});
			}
		});
	}

	//TODO should we place it in BasicRelationalPersistentProperty/BasicRelationalPersistentEntity and generate using NamingStrategy?
	private static String getForeignKeyName(String referencedTableName, String referencedColumnName) {
		return String.format("%s_%s_fk", referencedTableName, referencedColumnName);
	}

	private record ColumnWithForeignKey(Column column, ForeignKey foreignKey) {
	}
}
