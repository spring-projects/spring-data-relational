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
package org.springframework.data.relational.core.mapping.schema;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * Model class that contains Table/Column information that can be used to generate SQL for Schema generation.
 *
 * @author Kurt Niemi
 * @since 3.2
 */
record Tables(List<Table> tables) {

	public static Tables from(RelationalMappingContext context) {
		return from(context.getPersistentEntities().stream(), new DefaultSqlTypeMapping(), null);
	}

	// TODO: Add support (i.e. create tickets) to support mapped collections, entities, embedded properties, and aggregate
	// references.

	public static Tables from(Stream<? extends RelationalPersistentEntity<?>> persistentEntities,
			SqlTypeMapping sqlTypeMapping, String defaultSchema) {

		List<Table> tables = persistentEntities
				.filter(it -> it.isAnnotationPresent(org.springframework.data.relational.core.mapping.Table.class)) //
				.map(entity -> {

					Table table = new Table(defaultSchema, entity.getTableName().getReference());

					Set<RelationalPersistentProperty> identifierColumns = new LinkedHashSet<>();
					entity.getPersistentProperties(Id.class).forEach(identifierColumns::add);

					for (RelationalPersistentProperty property : entity) {

						if (property.isEntity() && !property.isEmbedded()) {
							continue;
						}

						Column column = new Column(property.getColumnName().getReference(), sqlTypeMapping.getColumnType(property),
								true, identifierColumns.contains(property));
						table.columns().add(column);
					}
					return table;
				}).collect(Collectors.toList());

		return new Tables(tables);
	}

	public static Tables empty() {
		return new Tables(Collections.emptyList());
	}
}
