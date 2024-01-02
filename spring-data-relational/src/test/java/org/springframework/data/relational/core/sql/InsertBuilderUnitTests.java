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
 * Unit tests for {@link InsertBuilder}.
 *
 * @author Mark Paluch
 */
public class InsertBuilderUnitTests {

	@Test // DATAJDBC-335
	public void shouldCreateSimpleInsert() {

		Table table = SQL.table("mytable");
		Column foo = table.column("foo");
		Column bar = table.column("bar");

		Insert insert = StatementBuilder.insert().into(table).column(foo).column(bar).value(SQL.bindMarker()).build();

		CapturingVisitor visitor = new CapturingVisitor();
		insert.visit(visitor);

		assertThat(visitor.enter).containsSequence(insert, new Into(table), table, foo, table, bar, table,
				new Values(SQL.bindMarker()));

		assertThat(insert.toString()).isEqualTo("INSERT INTO mytable (mytable.foo, mytable.bar) VALUES(?)");
	}
}
