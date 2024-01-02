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

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DeleteValidator}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public class DeleteValidatorUnitTests {

	@Test // DATAJDBC-335
	public void shouldReportMissingTableForDeleteViaWhere() {

		Column column = SQL.table("table").column("foo");
		Table bar = SQL.table("bar");

		assertThatThrownBy(() -> {
			StatementBuilder.delete() //
					.from(bar) //
					.where(column.isEqualTo(SQL.literalOf("foo"))) //
					.build();
		}).isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Required table [table] by a WHERE predicate not imported by FROM [bar]");
	}

	@Test // DATAJDBC-335
	public void shouldIgnoreImportsFromSubselectsInWhereClause() {

		Table foo = SQL.table("foo");
		Column bar = foo.column("bar");

		Table floo = SQL.table("floo");
		Column bah = floo.column("bah");

		Select subselect = Select.builder().select(bah).from(floo).build();

		assertThat(Delete.builder().from(foo).where(Conditions.in(bar, subselect)).build()).isNotNull();
	}
}
