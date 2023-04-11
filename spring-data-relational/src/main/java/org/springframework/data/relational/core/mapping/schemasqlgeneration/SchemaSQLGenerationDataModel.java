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

import org.springframework.data.relational.core.mapping.BasicRelationalPersistentProperty;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Model class that contains Table/Column information that can be used
 * to generate SQL for Schema generation.
 *
 * @author Kurt Niemi
 */
public class SchemaSQLGenerationDataModel implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private final List<TableModel> tableData = new ArrayList<TableModel>();
    BaseTypeMapper typeMapper;

    /**
     * Default constructor so that we can deserialize a model
     */
    public SchemaSQLGenerationDataModel() {
    }

    /**
     * Create model from a RelationalMappingContext
     */
    public SchemaSQLGenerationDataModel(RelationalMappingContext context) {

        if (typeMapper == null) {
            typeMapper = new BaseTypeMapper();
        }

        for (RelationalPersistentEntity entity : context.getPersistentEntities()) {
            TableModel tableModel = new TableModel(entity.getTableName());

            Iterator<BasicRelationalPersistentProperty> iter =
                    entity.getPersistentProperties(Column.class).iterator();

            while (iter.hasNext()) {
                BasicRelationalPersistentProperty p = iter.next();
                ColumnModel columnModel = new ColumnModel(p.getColumnName(),
                        typeMapper.databaseTypeFromClass(p.getActualType()),
                        true);
                tableModel.getColumns().add(columnModel);
            }
            tableData.add(tableModel);
        }
    }

    public List<TableModel> getTableData() {
        return tableData;
    }
}
