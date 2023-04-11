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
package org.springframework.data.relational.schemasqlgeneration;

import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.mapping.schemasqlgeneration.SchemaSQLGenerationDataModel;
import org.springframework.data.relational.core.mapping.schemasqlgeneration.SchemaSQLGenerator;
import org.springframework.data.relational.core.mapping.schemasqlgeneration.TableModel;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.util.StringUtils;

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
