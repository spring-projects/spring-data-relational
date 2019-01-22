/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.relational.core.sql;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;

/**
 * Unit tests for {@link NaiveSqlRenderer}.
 *
 * @author Mark Paluch
 */
public class NaiveSqlRendererUnitTests {

	@Test // DATAJDBC-309
	public void shouldRenderSingleColumn() {

		Select select = Select.builder().select("foo").from("bar").build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT foo FROM bar");
	}

	@Test // DATAJDBC-309
	public void shouldRenderAliasedColumnAndFrom() {

		Table table = Table.create("bar").as("my_bar");

		Select select = Select.builder().select(table.column("foo").as("my_foo")).from(table).build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT my_bar.foo AS my_foo FROM bar AS my_bar");
	}

	@Test // DATAJDBC-309
	public void shouldRenderMultipleColumnsFromTables() {

		Table table1 = Table.create("table1");
		Table table2 = Table.create("table2");

		Select select = Select.builder().select(table1.column("col1")).select(table2.column("col2")).from(table1).from(table2).build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT table1.col1, table2.col2 FROM table1, table2");
	}

	@Test // DATAJDBC-309
	public void shouldRenderDistinct() {

		Select select = Select.builder().select(Functions.distinct("foo", "bar")).from("bar").build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT DISTINCT foo, bar FROM bar");
	}

	@Test // DATAJDBC-309
	public void shouldRenderCountFunction() {

		Select select = Select.builder().select(Functions.count("foo"), Column.create("bar")).from("bar").build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT COUNT(foo), bar FROM bar");
	}

	@Test // DATAJDBC-309
	public void shouldRenderSimpleJoin() {

		Table employee = SQL.table("employee");
		Table department = SQL.table("department");

		Select select = Select.builder().select(employee.column("id"), department.column("name")).from(employee) //
				.join(department).on(employee.column("department_id")).equals(department.column("id")) //
				.build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT employee.id, department.name FROM employee " +
				"JOIN department ON employee.department_id = department.id");
	}

	@Test // DATAJDBC-309
	public void shouldRenderSimpleJoinWithAnd() {

		Table employee = SQL.table("employee");
		Table department = SQL.table("department");

		Select select = Select.builder().select(employee.column("id"), department.column("name")).from(employee) //
				.join(department).on(employee.column("department_id")).equals(department.column("id")) //
				.and(employee.column("tenant")).equals(department.column("tenant")) //
				.build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT employee.id, department.name FROM employee " +
				"JOIN department ON employee.department_id = department.id " +
				"AND employee.tenant = department.tenant");
	}

	@Test // DATAJDBC-309
	public void shouldRenderMultipleJoinWithAnd() {

		Table employee = SQL.table("employee");
		Table department = SQL.table("department");
		Table tenant = SQL.table("tenant").as("tenant_base");

		Select select = Select.builder().select(employee.column("id"), department.column("name")).from(employee) //
				.join(department).on(employee.column("department_id")).equals(department.column("id")) //
				.and(employee.column("tenant")).equals(department.column("tenant")) //
				.join(tenant).on(tenant.column("tenant_id")).equals(department.column("tenant")) //
				.build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT employee.id, department.name FROM employee " +
				"JOIN department ON employee.department_id = department.id " +
				"AND employee.tenant = department.tenant " +
				"JOIN tenant AS tenant_base ON tenant_base.tenant_id = department.tenant");
	}

	@Test // DATAJDBC-309
	public void shouldRenderOrderByIndex() {

		Select select = Select.builder().select(Functions.count("foo"), Column.create("bar")).from("bar").orderBy(1, 2).build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT COUNT(foo), bar FROM bar ORDER BY 1, 2");
	}

	@Test // DATAJDBC-309
	public void shouldRenderOrderByName() {

		Table employee = SQL.table("employee").as("emp");
		Column column = employee.column("name").as("emp_name");

		Select select = Select.builder().select(column).from(employee).orderBy(OrderByField.from(column).asc()).build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT emp.name AS emp_name FROM employee AS emp ORDER BY emp_name ASC");
	}

	@Test // DATAJDBC-309
	public void shouldRenderOrderLimitOffset() {

		Select select = Select.builder().select(Column.create("bar")).from("foo").limitOffset(10, 20).build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT bar FROM foo LIMIT 10 OFFSET 20");
	}
}
