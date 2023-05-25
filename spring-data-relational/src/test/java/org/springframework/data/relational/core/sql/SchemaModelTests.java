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
package org.springframework.data.relational.core.sql;

import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.mapping.schemasqlgeneration.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link SchemaModel}.
 *
 * @author Kurt Niemi
 */
public class SchemaModelTests {

    @Test
    void testDiffSchema() {

        RelationalMappingContext context = new RelationalMappingContext();
        context.getRequiredPersistentEntity(SchemaModelTests.Table1.class);
        context.getRequiredPersistentEntity(SchemaModelTests.Table2.class);

        SchemaModel model = new SchemaModel(context);

        SchemaModel newModel = new SchemaModel(context);

        // Add column to table
        SqlIdentifier newIdentifier = new DefaultSqlIdentifier("newcol", false);
        ColumnModel newColumn = new ColumnModel(newIdentifier, "VARCHAR(255)");
        newModel.getTableData().get(0).columns().add(newColumn);

        // Remove table
        newModel.getTableData().remove(1);

        // Add new table
        SqlIdentifier tableIdenfifier = new DefaultSqlIdentifier("newtable", false);
        TableModel newTable = new TableModel(null, tableIdenfifier);
        newTable.columns().add(newColumn);
        newModel.getTableData().add(newTable);

        SchemaDiff diff = new SchemaDiff(model, newModel); //, model);

        // Verify that newtable is an added table in the diff
        assertThat(diff.getTableAdditions().size() > 0);
        assertThat(diff.getTableAdditions().get(0).name().getReference().equals("newtable"));

        assertThat(diff.getTableDeletions().size() > 0);
        assertThat(diff.getTableDeletions().get(0).name().getReference().equals("vader"));

        assertThat(diff.getTableDiff().size() > 0);
        assertThat(diff.getTableDiff().get(0).addedColumns().size() > 0);
        assertThat(diff.getTableDiff().get(0).deletedColumns().size() > 0);
    }

    // Test table classes for performing schema diff
    @Table
    static class Table1 {
        @Column
        public String force;
        @Column
        public String be;
        @Column
        public String with;
        @Column
        public String you;
    }

    @Table
    static class Table2 {
        @Column
        public String lukeIAmYourFather;
        @Column
        public Boolean darkSide;
        @Column
        public Float floater;
        @Column
        public Double doubleClass;
        @Column
        public Integer integerClass;
    }


}
