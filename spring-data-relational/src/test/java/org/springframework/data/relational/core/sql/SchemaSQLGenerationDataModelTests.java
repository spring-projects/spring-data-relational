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

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.mapping.schemasqlgeneration.*;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link SchemaSQLGenerationDataModel}.
 *
 * @author Kurt Niemi
 */
public class SchemaSQLGenerationDataModelTests {

    @Test
    void testBasicSchemaSQLGeneration() {

        IdentifierProcessing.Quoting quoting = new IdentifierProcessing.Quoting("`");
        IdentifierProcessing identifierProcessing = IdentifierProcessing.create(quoting, IdentifierProcessing.LetterCasing.LOWER_CASE);
        SchemaSQLGenerator generator = new SchemaSQLGenerator(identifierProcessing);

        RelationalMappingContext context = new RelationalMappingContext();
        context.getRequiredPersistentEntity(SchemaSQLGenerationDataModelTests.Luke.class);

        SchemaSQLGenerationDataModel model = new SchemaSQLGenerationDataModel(context);
        String sql = generator.generateSQL(model);
        assertThat(sql).isEqualTo("CREATE TABLE `luke` (`force` VARCHAR(255),`be` VARCHAR(255),`with` VARCHAR(255),`you` VARCHAR(255) );\n");

        context = new RelationalMappingContext();
        context.getRequiredPersistentEntity(SchemaSQLGenerationDataModelTests.Vader.class);

        model = new SchemaSQLGenerationDataModel(context);
        sql = generator.generateSQL(model);
        assertThat(sql).isEqualTo("CREATE TABLE `vader` (`luke_i_am_your_father` VARCHAR(255),`dark_side` TINYINT,`floater` FLOAT,`double_class` DOUBLE,`integer_class` INT );\n");
    }

    @Test
    void testDiffSchema() {
        IdentifierProcessing.Quoting quoting = new IdentifierProcessing.Quoting("`");
        IdentifierProcessing identifierProcessing = IdentifierProcessing.create(quoting, IdentifierProcessing.LetterCasing.LOWER_CASE);
        SchemaSQLGenerator generator = new SchemaSQLGenerator(identifierProcessing);

        RelationalMappingContext context = new RelationalMappingContext();
        context.getRequiredPersistentEntity(SchemaSQLGenerationDataModelTests.Luke.class);
        context.getRequiredPersistentEntity(SchemaSQLGenerationDataModelTests.Vader.class);

        SchemaSQLGenerationDataModel model = new SchemaSQLGenerationDataModel(context);

        SchemaSQLGenerationDataModel newModel = new SchemaSQLGenerationDataModel(context);

        // Add column to table
        SqlIdentifier newIdentifier = new DefaultSqlIdentifier("newcol", false);
        ColumnModel newColumn = new ColumnModel(newIdentifier, "VARCHAR(255)");
        newModel.getTableData().get(0).getColumns().add(newColumn);

        // Remove table
        newModel.getTableData().remove(1);

        // Add new table
        SqlIdentifier tableIdenfifier = new DefaultSqlIdentifier("newtable", false);
        TableModel newTable = new TableModel(null, tableIdenfifier);
        newTable.getColumns().add(newColumn);
        newModel.getTableData().add(newTable);

        SchemaDiff diff = newModel.diffModel(model);

        // Verify that newtable is an added table in the diff
        assertThat(diff.getTableAdditions().size() > 0);
        assertThat(diff.getTableAdditions().get(0).getName().getReference().equals("newtable"));

        assertThat(diff.getTableDeletions().size() > 0);
        assertThat(diff.getTableDeletions().get(0).getName().getReference().equals("vader"));

        assertThat(diff.getTableDiff().size() > 0);
        assertThat(diff.getTableDiff().get(0).getAddedColumns().size() > 0);
        assertThat(diff.getTableDiff().get(0).getDeletedColumns().size() > 0);
        assertThat(diff.getTableDiff().get(0).getAddedColumns().get(0).getName().getReference().equals("A"));
        assertThat(diff.getTableDiff().get(0).getDeletedColumns().get(0).getName().getReference().equals("B"));
    }

    @Test
    void testSerialization() {
        IdentifierProcessing.Quoting quoting = new IdentifierProcessing.Quoting("`");
        IdentifierProcessing identifierProcessing = IdentifierProcessing.create(quoting, IdentifierProcessing.LetterCasing.LOWER_CASE);
        SchemaSQLGenerator generator = new SchemaSQLGenerator(identifierProcessing);

        RelationalMappingContext context = new RelationalMappingContext();
        context.getRequiredPersistentEntity(SchemaSQLGenerationDataModelTests.Luke.class);
        context.getRequiredPersistentEntity(SchemaSQLGenerationDataModelTests.Vader.class);

        SchemaSQLGenerationDataModel model = new SchemaSQLGenerationDataModel(context);

        try {
            model.persist("model.ser");

            SchemaSQLGenerationDataModel loadedModel = SchemaSQLGenerationDataModel.load("model.ser");

            assertThat(loadedModel.getTableData().size() == 2);
        } catch (Exception ex) {
            ex.printStackTrace();
            assertThat(false);
        }
    }


    @Table
    static class Luke {
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
    static class Vader {
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
