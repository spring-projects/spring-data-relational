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

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.*;

import java.util.*;

/**
 * Model class that contains Table/Column information that can be used
 * to generate SQL for Schema generation.
 *
 * @author Kurt Niemi
 * @since 3.2
 */
public class SchemaModel
{
    private final List<TableModel> tableData = new ArrayList<TableModel>();
    public DatabaseTypeMapping databaseTypeMapping;

    /**
     * Create empty model
     */
    public SchemaModel() {

    }

    /**
     * Create model from a RelationalMappingContext
     */
    public SchemaModel(RelationalMappingContext context) {

        if (databaseTypeMapping == null) {
            databaseTypeMapping = new DefaultDatabaseTypeMapping();
        }

        for (RelationalPersistentEntity entity : context.getPersistentEntities()) {
            TableModel tableModel = new TableModel(entity.getTableName());

            Iterator<BasicRelationalPersistentProperty> iter =
                    entity.getPersistentProperties(Id.class).iterator();
            Set<BasicRelationalPersistentProperty> setIdentifierColumns = new HashSet<BasicRelationalPersistentProperty>();
            while (iter.hasNext()) {
                BasicRelationalPersistentProperty p = iter.next();
                setIdentifierColumns.add(p);
            }

            iter =
                    entity.getPersistentProperties(Column.class).iterator();

            while (iter.hasNext()) {
                BasicRelationalPersistentProperty p = iter.next();
                ColumnModel columnModel = new ColumnModel(p.getColumnName(),
                        databaseTypeMapping.databaseTypeFromClass(p.getActualType()),
                        true, setIdentifierColumns.contains(p));
                tableModel.columns().add(columnModel);
            }

            tableData.add(tableModel);
        }
    }

    public List<TableModel> getTableData() {
        return tableData;
    }
}
