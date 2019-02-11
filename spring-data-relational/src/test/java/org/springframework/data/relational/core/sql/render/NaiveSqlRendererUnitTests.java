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
package org.springframework.data.relational.core.sql.render;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Conditions;
import org.springframework.data.relational.core.sql.Functions;
import org.springframework.data.relational.core.sql.OrderByField;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.util.StringUtils;

/**
 * Unit tests for {@link NaiveSqlRenderer}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public class NaiveSqlRendererUnitTests {

	@Test // DATAJDBC-309
	public void shouldRenderSingleColumn() {

		Table bar = SQL.table("bar");
		Column foo = bar.column("foo");

		Select select = Select.builder().select(foo).from(bar).build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT bar.foo FROM bar");
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

		Select select = Select.builder().select(table1.column("col1")).select(table2.column("col2")).from(table1)
				.from(table2).build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT table1.col1, table2.col2 FROM table1, table2");
	}

	@Test // DATAJDBC-309
	public void shouldRenderDistinct() {

		Table table = SQL.table("bar");
		Column foo = table.column("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().distinct().select(foo, bar).from(table).build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT DISTINCT bar.foo, bar.bar FROM bar");
	}

	@Test // DATAJDBC-309
	public void shouldRenderCountFunction() {

		Table table = SQL.table("bar");
		Column foo = table.column("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().select(Functions.count(foo), bar).from(table).build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT COUNT(bar.foo), bar.bar FROM bar");
	}

	@Test // DATAJDBC-309
	public void shouldRenderSimpleJoin() {

		Table employee = SQL.table("employee");
		Table department = SQL.table("department");

		Select select = Select.builder().select(employee.column("id"), department.column("name")).from(employee) //
				.join(department).on(employee.column("department_id")).equals(department.column("id")) //
				.build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT employee.id, department.name FROM employee "
				+ "JOIN department ON employee.department_id = department.id");
	}

	@Test // DATAJDBC-309
	public void shouldRenderSimpleJoinWithAnd() {

		Table employee = SQL.table("employee");
		Table department = SQL.table("department");

		Select select = Select.builder().select(employee.column("id"), department.column("name")).from(employee) //
				.join(department).on(employee.column("department_id")).equals(department.column("id")) //
				.and(employee.column("tenant")).equals(department.column("tenant")) //
				.build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT employee.id, department.name FROM employee "
				+ "JOIN department ON employee.department_id = department.id " + "AND employee.tenant = department.tenant");
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

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT employee.id, department.name FROM employee "
				+ "JOIN department ON employee.department_id = department.id " + "AND employee.tenant = department.tenant "
				+ "JOIN tenant AS tenant_base ON tenant_base.tenant_id = department.tenant");
	}

	@Test // DATAJDBC-309
	public void shouldRenderOrderByName() {

		Table employee = SQL.table("employee").as("emp");
		Column column = employee.column("name").as("emp_name");

		Select select = Select.builder().select(column).from(employee).orderBy(OrderByField.from(column).asc()).build();

		assertThat(NaiveSqlRenderer.render(select))
				.isEqualTo("SELECT emp.name AS emp_name FROM employee AS emp ORDER BY emp_name ASC");
	}

	@Test // DATAJDBC-309
	public void shouldRenderOrderLimitOffset() {

		Table table = SQL.table("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().select(bar).from("foo").limitOffset(10, 20).build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT foo.bar FROM foo LIMIT 10 OFFSET 20");
	}

	@Test // DATAJDBC-309
	public void shouldRenderIsNull() {

		Table table = SQL.table("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().select(bar).from(table).where(Conditions.isNull(bar)).build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar IS NULL");
	}

	@Test // DATAJDBC-309
	public void shouldRenderNotNull() {

		Table table = SQL.table("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().select(bar).from(table).where(Conditions.isNull(bar).not()).build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar IS NOT NULL");
	}

	@Test // DATAJDBC-309
	public void shouldRenderEqualityCondition() {

		Table table = SQL.table("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().select(bar).from(table).where(Conditions.isEqual(bar, SQL.bindMarker(":name")))
				.build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar = :name");
	}

	@Test // DATAJDBC-309
	public void shouldRendersAndOrConditionWithProperParentheses() {

		Table table = SQL.table("foo");
		Column bar = table.column("bar");
		Column baz = table.column("baz");

		Select select = Select.builder().select(bar).from(table).where(Conditions.isEqual(bar, SQL.bindMarker(":name"))
				.or(Conditions.isEqual(bar, SQL.bindMarker(":name2"))).and(Conditions.isNull(baz))).build();

		assertThat(NaiveSqlRenderer.render(select))
				.isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar = :name OR foo.bar = :name2 AND foo.baz IS NULL");
	}

	@Test // DATAJDBC-309
	public void shouldInWithNamedParameter() {

		Table table = SQL.table("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().select(bar).from(table).where(Conditions.in(bar, SQL.bindMarker(":name"))).build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar IN (:name)");
	}

	@Test // DATAJDBC-309
	public void shouldInWithNamedParameters() {

		Table table = SQL.table("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().select(bar).from(table)
				.where(Conditions.in(bar, SQL.bindMarker(":name"), SQL.bindMarker(":name2"))).build();

		assertThat(NaiveSqlRenderer.render(select)).isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar IN (:name, :name2)");
	}

	@Test // DATAJDBC-309
	public void shouldRenderInSubselect() {

		Table foo = SQL.table("foo");
		Column bar = foo.column("bar");

		Table floo = SQL.table("floo");
		Column bah = floo.column("bah");

		Select subselect = Select.builder().select(bah).from(floo).build();

		Select select = Select.builder().select(bar).from(foo).where(Conditions.in(bar, subselect)).build();

		assertThat(NaiveSqlRenderer.render(select))
				.isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar IN (SELECT floo.bah FROM floo)");
	}

	@Test // DATAJDBC-309
	public void shouldConsiderNamingStrategy() {

		Table foo = SQL.table("Foo");
		Column bar = foo.column("BaR");
		Column baz = foo.column("BaZ");

		Select select = Select.builder().select(bar).from(foo).where(bar.isEqualTo(baz)).build();

		String upper = NaiveSqlRenderer.create(select, new SimpleRenderContext(NamingStrategies.toUpper())).render();
		assertThat(upper).isEqualTo("SELECT FOO.BAR FROM FOO WHERE FOO.BAR = FOO.BAZ");

		String lower = NaiveSqlRenderer.create(select, new SimpleRenderContext(NamingStrategies.toLower())).render();
		assertThat(lower).isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar = foo.baz");

		String mapped = NaiveSqlRenderer
				.create(select, new SimpleRenderContext(NamingStrategies.mapWith(StringUtils::uncapitalize))).render();
		assertThat(mapped).isEqualTo("SELECT foo.baR FROM foo WHERE foo.baR = foo.baZ");
	}

}
