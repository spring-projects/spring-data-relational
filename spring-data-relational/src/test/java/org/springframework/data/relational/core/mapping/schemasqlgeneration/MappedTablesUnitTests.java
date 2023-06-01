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

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.Table;

/**
 * Unit tests for the {@link MappedTables}.
 *
 * @author Kurt Niemi
 */
class MappedTablesUnitTests {

	@Test
	void testDiffSchema() {

		RelationalMappingContext context = new RelationalMappingContext();
		context.getRequiredPersistentEntity(MappedTablesUnitTests.Table1.class);
		context.getRequiredPersistentEntity(MappedTablesUnitTests.Table2.class);

		MappedTables target = new MappedTables(context);
		MappedTables source = new MappedTables(context);

		// Add column to table
		ColumnModel newColumn = new ColumnModel("newcol", "VARCHAR(255)");
		source.getTableData().get(0).columns().add(newColumn);

		// Remove table
		source.getTableData().remove(1);

		// Add new table
		TableModel newTable = new TableModel(null, "newtable");
		newTable.columns().add(newColumn);
		source.getTableData().add(newTable);

		SchemaDiff diff = new SchemaDiff(target, source);

		// Verify that newtable is an added table in the diff
		assertThat(diff.getTableAdditions()).isNotEmpty();
		assertThat(diff.getTableAdditions().get(0).name()).isEqualTo("table1");

		assertThat(diff.getTableDeletions()).isNotEmpty();
		assertThat(diff.getTableDeletions().get(0).name()).isEqualTo("newtable");

		assertThat(diff.getTableDiff()).isNotEmpty();
		assertThat(diff.getTableDiff().get(0).columnsToAdd()).isEmpty();
		assertThat(diff.getTableDiff().get(0).columnsToDrop()).isNotEmpty();
	}

	// Test table classes for performing schema diff
	@Table
	static class Table1 {
		String force;
		String be;
		String with;
		String you;
	}

	@Table
	static class Table2 {
		String lukeIAmYourFather;
		Boolean darkSide;
		Float floater;
		Double doubleClass;
		Integer integerClass;
	}

}
