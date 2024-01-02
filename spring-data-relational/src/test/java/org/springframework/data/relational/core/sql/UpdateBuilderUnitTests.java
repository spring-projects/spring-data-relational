/*
 * Copyright 2019-2024 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link UpdateBuilder}.
 *
 * @author Mark Paluch
 */
public class UpdateBuilderUnitTests {

	@Test // DATAJDBC-335
	public void shouldCreateSimpleUpdate() {

		Table table = SQL.table("mytable");
		Column column = table.column("foo");

		Update update = StatementBuilder.update(table).set(column.set(SQL.bindMarker())).build();

		CapturingVisitor visitor = new CapturingVisitor();
		update.visit(visitor);

		assertThat(visitor.enter).containsSequence(update, table, Assignments.value(column, SQL.bindMarker()), column,
				table, SQL.bindMarker());

		assertThat(update.toString()).isEqualTo("UPDATE mytable SET mytable.foo = ?");
	}

	@Test // DATAJDBC-335
	public void shouldCreateUpdateWIthCondition() {

		Table table = SQL.table("mytable");
		Column column = table.column("foo");

		Update update = StatementBuilder.update(table).set(column.set(SQL.bindMarker())).where(column.isNull()).build();

		CapturingVisitor visitor = new CapturingVisitor();
		update.visit(visitor);

		assertThat(visitor.enter).containsSequence(update, table, Assignments.value(column, SQL.bindMarker()), column,
				table, SQL.bindMarker(), new Where(column.isNull()));

		assertThat(update.toString()).isEqualTo("UPDATE mytable SET mytable.foo = ? WHERE mytable.foo IS NULL");
	}
}
