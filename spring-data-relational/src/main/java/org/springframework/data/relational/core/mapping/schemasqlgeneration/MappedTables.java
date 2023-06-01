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
package org.springframework.data.relational.core.mapping.schemasqlgeneration;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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
public class MappedTables {

	private final List<TableModel> tableData = new ArrayList<>();
	public final SqlTypeMapping sqlTypeMapping;

	public MappedTables() {
		this.sqlTypeMapping = new DefaultSqlTypeMapping();
	}

	/**
	 * Create model from a RelationalMappingContext
	 */
	public MappedTables(RelationalMappingContext context) {
		this.sqlTypeMapping = createTypeMapping(context);
	}

	// TODO: Add support (i.e. create tickets) to support mapped collections, entities, embedded properties, and aggregate
	// references.
	private SqlTypeMapping createTypeMapping(RelationalMappingContext context) {

		SqlTypeMapping sqlTypeMapping = new DefaultSqlTypeMapping();

		for (RelationalPersistentEntity<?> entity : context.getPersistentEntities()) {

			TableModel tableModel = new TableModel(entity.getTableName().getReference());

			Set<RelationalPersistentProperty> identifierColumns = new LinkedHashSet<>();
			entity.getPersistentProperties(Id.class).forEach(identifierColumns::add);

			for (RelationalPersistentProperty property : entity) {

				if (property.isEntity() && !property.isEmbedded()) {
					continue;
				}

				ColumnModel columnModel = new ColumnModel(property.getColumnName().getReference(),
						sqlTypeMapping.getColumnType(property), true, identifierColumns.contains(property));
				tableModel.columns().add(columnModel);
			}

			tableData.add(tableModel);
		}

		return sqlTypeMapping;
	}

	List<TableModel> getTableData() {
		return tableData;
	}
}
