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
 * Unit tests for {@link SelectValidator}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public class SelectValidatorUnitTests {

	@Test // DATAJDBC-309
	public void shouldReportMissingTableViaSelectlist() {

		Column column = SQL.table("table").column("foo");

		assertThatThrownBy(() -> {
			StatementBuilder.select(column).from(SQL.table("bar")).build();
		}).isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Required table [table] by a SELECT column not imported by FROM [bar] or JOIN []");
	}

	@Test // DATAJDBC-309
	public void shouldReportMissingTableViaSelectlistCount() {

		Column column = SQL.table("table").column("foo");

		assertThatThrownBy(() -> {
			StatementBuilder.select(Functions.count(column)).from(SQL.table("bar")).build();
		}).isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Required table [table] by a SELECT column not imported by FROM [bar] or JOIN []");
	}

	@Test // DATAJDBC-309
	public void shouldReportMissingTableViaSelectlistDistinct() {

		Column column = SQL.table("table").column("foo");

		assertThatThrownBy(() -> {
			StatementBuilder.select(column).distinct().from(SQL.table("bar")).build();
		}).isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Required table [table] by a SELECT column not imported by FROM [bar] or JOIN []");
	}

	@Test // DATAJDBC-309
	public void shouldReportMissingTableViaOrderBy() {

		Column foo = SQL.table("table").column("foo");
		Table bar = SQL.table("bar");

		assertThatThrownBy(() -> {
			StatementBuilder.select(bar.column("foo")) //
					.from(bar) //
					.orderBy(foo) //
					.build();
		}).isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Required table [table] by a ORDER BY column not imported by FROM [bar] or JOIN []");
	}

	@Test // DATAJDBC-309
	public void shouldReportMissingTableViaWhere() {

		Column column = SQL.table("table").column("foo");
		Table bar = SQL.table("bar");

		assertThatThrownBy(() -> {
			StatementBuilder.select(bar.column("foo")) //
					.from(bar) //
					.where(column.isEqualTo(SQL.literalOf("foo"))) //
					.build();
		}).isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Required table [table] by a WHERE predicate not imported by FROM [bar] or JOIN []");
	}

	@Test // DATAJDBC-309
	public void shouldIgnoreImportsFromSubselectsInWhereClause() {

		Table foo = SQL.table("foo");
		Column bar = foo.column("bar");

		Table floo = SQL.table("floo");
		Column bah = floo.column("bah");

		Select subselect = Select.builder().select(bah).from(floo).build();

		assertThatThrownBy(() -> {
			Select.builder().select(bah).from(foo).where(Conditions.in(bar, subselect)).build();
		}).isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Required table [floo] by a SELECT column not imported by FROM [foo] or JOIN []");
	}

}
