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
import org.springframework.data.relational.core.mapping.schemasqlgeneration.TableModel;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link SchemaSQLGenerationDataModel}.
 *
 * @author Kurt Niemi
 */
public class SchemaSQLGenerationDataModelTests {

    @Test
    void testBasicModelGeneration() {
        RelationalMappingContext context = new RelationalMappingContext();
        context.getRequiredPersistentEntity(SchemaSQLGenerationDataModelTests.Luke.class);
        context.getRequiredPersistentEntity(SchemaSQLGenerationDataModelTests.Vader.class);

        SchemaSQLGenerationDataModel model = context.generateSchemaSQL();

        assertThat(model.getTableData().size()).isEqualTo(2);

        TableModel t1 = model.getTableData().get(0);
        assertThat(t1.getName()).isEqualTo("luke");
        assertThat(t1.getColumns().get(0).getName()).isEqualTo("force");
        assertThat(t1.getColumns().get(1).getName()).isEqualTo("be");
        assertThat(t1.getColumns().get(2).getName()).isEqualTo("with");
        assertThat(t1.getColumns().get(3).getName()).isEqualTo("you");

        TableModel t2 = model.getTableData().get(1);
        assertThat(t2.getName()).isEqualTo("vader");
        assertThat(t2.getColumns().get(0).getName()).isEqualTo("luke_i_am_your_father");
        assertThat(t2.getColumns().get(1).getName()).isEqualTo("dark_side");
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
    }


}
