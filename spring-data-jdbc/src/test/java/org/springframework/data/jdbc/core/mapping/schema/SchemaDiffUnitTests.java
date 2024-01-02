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

import static org.assertj.core.api.Assertions.*;

import java.text.Collator;
import java.util.Locale;

import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

/**
 * Unit tests for the {@link Tables}.
 *
 * @author Kurt Niemi
 * @author Mark Paluch
 */
class SchemaDiffUnitTests {

	@Test
	void testDiffSchema() {

		RelationalMappingContext context = new RelationalMappingContext();
		context.getRequiredPersistentEntity(SchemaDiffUnitTests.Table1.class);
		context.getRequiredPersistentEntity(SchemaDiffUnitTests.Table2.class);

		Tables mappedEntities = Tables.from(context);
		Tables existingTables = Tables.from(context);

		// Table table1 does not exist on the database yet.
		existingTables.tables().remove(new Table("table1"));

		// Add column to table2
		Column newColumn = new Column("newcol", "VARCHAR(255)");
		Table table2 = mappedEntities.tables().get(mappedEntities.tables().indexOf(new Table("table2")));
		table2.columns().add(newColumn);

		// This should be deleted
		Table delete_me = new Table(null, "delete_me");
		delete_me.columns().add(newColumn);
		existingTables.tables().add(delete_me);

		SchemaDiff diff = SchemaDiff.diff(mappedEntities, existingTables, Collator.getInstance(Locale.ROOT)::compare);

		// Verify that newtable is an added table in the diff
		assertThat(diff.tableAdditions()).isNotEmpty();
		assertThat(diff.tableAdditions()).extracting(Table::name).containsOnly("table1");

		assertThat(diff.tableDeletions()).isNotEmpty();
		assertThat(diff.tableDeletions()).extracting(Table::name).containsOnly("delete_me");

		assertThat(diff.tableDiffs()).hasSize(1);
		assertThat(diff.tableDiffs()).extracting(it -> it.table().name()).containsOnly("table2");
		assertThat(diff.tableDiffs().get(0).columnsToAdd()).contains(newColumn);
		assertThat(diff.tableDiffs().get(0).columnsToDrop()).isEmpty();
	}

	// Test table classes for performing schema diff
	@org.springframework.data.relational.core.mapping.Table
	static class Table1 {
		String force;
		String be;
		String with;
		String you;
	}

	@org.springframework.data.relational.core.mapping.Table
	static class Table2 {
		String lukeIAmYourFather;
		Boolean darkSide;
		Float floater;
		Double doubleClass;
		Integer integerClass;
	}

}
