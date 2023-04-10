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

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
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

    public List<TableModel> getTableData() {
        return tableData;
    }
}
