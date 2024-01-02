/*
 * Copyright 2023-2024 the original author or authors.
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
import java.util.List;

/**
 * Used to keep track of columns that should be added or deleted, when performing a difference between a source and
 * target {@link Tables}.
 *
 * @author Kurt Niemi
 * @author Evgenii Koba
 * @since 3.2
 */
record TableDiff(Table table, List<Column> columnsToAdd, List<Column> columnsToDrop, List<ForeignKey> fkToAdd,
								 List<ForeignKey> fkToDrop) {

	public TableDiff(Table table) {
		this(table, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
	}

}
