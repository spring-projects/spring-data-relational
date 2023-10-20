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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

		List<NestedEntityMetadata> nestedEntityMetadataList = new ArrayList<>();
		List<Table> tables = persistentEntities
				.filter(it -> it.isAnnotationPresent(org.springframework.data.relational.core.mapping.Table.class)) //
				.map(entity -> {

					Table table = new Table(defaultSchema, entity.getTableName().getReference());

					Set<RelationalPersistentProperty> identifierColumns = new LinkedHashSet<>();
					entity.getPersistentProperties(Id.class).forEach(identifierColumns::add);
					collectNestedEntityMetadata(entity, context, nestedEntityMetadataList, sqlTypeMapping);

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

		applyNestedEntityMetadata(tables, nestedEntityMetadataList);

		return new Tables(tables);
	}

	public static Tables empty() {
		return new Tables(Collections.emptyList());
	}

	/**
	 * Apply all information we know about nested entities to correctly create foreign and primary keys
	 */
	private static void applyNestedEntityMetadata(List<Table> tables, List<NestedEntityMetadata> nestedEntityMetadataList) {
		nestedEntityMetadataList.forEach(nestedEntityMetadata -> {
			while (nestedEntityMetadata.parentMetadata != null) {
				NestedEntityMetadata current = nestedEntityMetadata;
				NestedEntityMetadata parentMetadata = current.parentMetadata;

				Table table = tables.stream().filter(t -> t.name().equals(current.tableName)).findAny().get();

				List<Column> parentIdColumns = new ArrayList<>();
				collectIdentityColumns(parentMetadata, parentIdColumns);
				List<String> parentIdColumnNames = parentIdColumns.stream().map(Column::name).toList();

				String foreignKeyName = getForeignKeyName(parentMetadata.tableName, parentIdColumnNames);
				if(parentIdColumnNames.size() == 1) {
					addIfAbsent(table.columns(), new Column(parentMetadata.referencedIdColumnName, parentMetadata.idColumnType,
									false, current.idColumnName == null));
					if(parentMetadata.referencedKeyColumnName != null) {
						addIfAbsent(table.columns(), new Column(parentMetadata.referencedKeyColumnName, parentMetadata.referencedKeyColumnType,
								false, true));
					}
					table.foreignKeys().add(new ForeignKey(foreignKeyName, current.tableName,
							List.of(parentMetadata.referencedIdColumnName), parentMetadata.tableName, parentIdColumnNames));
				} else {
					addIfAbsent(table.columns(), parentIdColumns.toArray(new Column[0]));
					addIfAbsent(table.columns(), new Column(parentMetadata.referencedKeyColumnName, parentMetadata.referencedKeyColumnType,
							false, true));
					table.foreignKeys().add(new ForeignKey(foreignKeyName, current.tableName, parentIdColumnNames,
							parentMetadata.tableName, parentIdColumnNames));
				}

				nestedEntityMetadata = nestedEntityMetadata.parentMetadata;
			}
		});
	}

	private static  <E> void addIfAbsent(List<E> list, E... elements) {
		for(E element : elements) {
			if (!list.contains(element)) {
				list.add(element);
			}
		}
	}

	private static void collectIdentityColumns(NestedEntityMetadata nestedEntityMetadata, List<Column> identityColumns) {
		if(nestedEntityMetadata.idColumnName != null) {
			if(identityColumns.isEmpty()) {
				identityColumns.add(new Column(nestedEntityMetadata.idColumnName, nestedEntityMetadata.idColumnType,
						false, true));
			} else {
				identityColumns.add(new Column(nestedEntityMetadata.referencedIdColumnName, nestedEntityMetadata.idColumnType,
						false, true));
			}
			Collections.reverse(identityColumns);
		} else {
			NestedEntityMetadata parentMetadata = nestedEntityMetadata.parentMetadata;
			identityColumns.add(new Column(parentMetadata.referencedKeyColumnName, parentMetadata.referencedKeyColumnType,
					false, true));
			collectIdentityColumns(parentMetadata, identityColumns);
		}
	}

	private static void collectNestedEntityMetadata(RelationalPersistentEntity<?> entity,
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			List<NestedEntityMetadata> nestedEntityMetadataList, SqlTypeMapping sqlTypeMapping) {

		Optional<RelationalPersistentProperty> idColumn = Optional.ofNullable(entity.getPersistentProperty(Id.class));

		entity.getPersistentProperties(MappedCollection.class).forEach(property -> {
			if (property.isEntity()) {
				property.getPersistentEntityTypeInformation().forEach(typeInformation -> {

					String referencedKeyColumnType = null;
					if(property.getType() == List.class) {
						referencedKeyColumnType = sqlTypeMapping.getColumnTypeByClass(Integer.class);
					} else if (property.getType() == Map.class) {
						referencedKeyColumnType = sqlTypeMapping.getColumnTypeByClass(property.getComponentType());
					}

					NestedEntityMetadata parent = new NestedEntityMetadata(
							idColumn.map(column -> column.getColumnName().getReference()).orElse(null),
							idColumn.map(column -> sqlTypeMapping.getColumnType(column)).orElse(null),
							property.getReverseColumnName(entity).getReference(),
							Optional.ofNullable(property.getKeyColumn()).map(SqlIdentifier::getReference).orElse(null),
							referencedKeyColumnType,
							entity.getTableName().getReference(),
							null);

					RelationalPersistentEntity childEntity = context.getRequiredPersistentEntity(typeInformation);
					Optional<RelationalPersistentProperty> childIdColumn = Optional.ofNullable(entity.getPersistentProperty(Id.class));
					NestedEntityMetadata child = new NestedEntityMetadata(
							childIdColumn.map(column -> column.getColumnName().getReference()).orElse(null),
							childIdColumn.map(column -> sqlTypeMapping.getColumnType(column)).orElse(null),
							null,
							null,
							null,
							childEntity.getTableName().getReference(),
							parent);

					boolean added = attachNewChild(nestedEntityMetadataList, child);
					added = added || attachNewParent(nestedEntityMetadataList, child);
					if(!added) {
						nestedEntityMetadataList.add(child);
					}

				});
			}
		});
	}

	//TODO should we place it in BasicRelationalPersistentProperty/BasicRelationalPersistentEntity and generate using NamingStrategy?
	private static String getForeignKeyName(String referencedTableName, List<String> referencedColumnNames) {
		return String.format("%s_%s_fk", referencedTableName, String.join("_", referencedColumnNames));
	}

	private static boolean attachNewParent(List<NestedEntityMetadata> nestedEntityMetadataList, NestedEntityMetadata newParent) {

		Optional<NestedEntityMetadata> oldParent
				= nestedEntityMetadataList.stream().filter(elem -> elem.getLastParent().equals(newParent)).findAny();
		if(oldParent.isEmpty()) {
			return false;
		} else {
			oldParent.get().parentMetadata.parentMetadata = newParent.parentMetadata;
			return true;
		}
	}

	private static boolean attachNewChild(List<NestedEntityMetadata> nestedEntityMetadataList, NestedEntityMetadata newChild) {

		int index = nestedEntityMetadataList.indexOf(newChild.parentMetadata);
		if(index == -1) {
			return false;
		} else {
			NestedEntityMetadata oldChild = nestedEntityMetadataList.remove(index);
			newChild.parentMetadata.parentMetadata = oldChild.parentMetadata;
			nestedEntityMetadataList.add(newChild);
			return true;
		}
	}

	private static class NestedEntityMetadata {

		public NestedEntityMetadata(String idColumnName, String idColumnType, String referencedIdColumnName,
				String referencedKeyColumnName, String referencedKeyColumnType, String tableName,
				NestedEntityMetadata parentMetadata) {
			this.idColumnName = idColumnName;
			this.idColumnType = idColumnType;
			this.referencedIdColumnName = referencedIdColumnName;
			this.referencedKeyColumnName = referencedKeyColumnName;
			this.referencedKeyColumnType = referencedKeyColumnType;
			this.tableName = tableName;
			this.parentMetadata = parentMetadata;
		}

		private String idColumnName;
		private String idColumnType;
		//column name for nested entity set by 'idColumn' of MappedCollection
		private String referencedIdColumnName;
		//column name for nested entity set by 'keyColumn' of MappedCollection
		private String referencedKeyColumnName;
		private String referencedKeyColumnType;
		private String tableName;
		private NestedEntityMetadata parentMetadata;

		public NestedEntityMetadata getLastParent() {
			if(parentMetadata == null) {
				return null;
			}
			NestedEntityMetadata lastParent = parentMetadata;
			while (lastParent.parentMetadata != null) {
				lastParent = lastParent.parentMetadata;
			}
			return lastParent;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;
			NestedEntityMetadata that = (NestedEntityMetadata) o;
			return Objects.equals(tableName, that.tableName);
		}

		@Override
		public int hashCode() {
			return Objects.hash(tableName);
		}
	}
}
