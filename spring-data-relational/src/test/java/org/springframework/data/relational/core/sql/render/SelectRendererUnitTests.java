/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.relational.core.sql.render;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.dialect.PostgresDialect;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.sql.*;
import org.springframework.util.StringUtils;

/**
 * Unit tests for {@link SqlRenderer}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public class SelectRendererUnitTests {

	@Test // DATAJDBC-309, DATAJDBC-278
	public void shouldRenderSingleColumn() {

		Table bar = SQL.table("bar");
		Column foo = bar.column("foo");

		Select select = Select.builder().select(foo).from(bar).limitOffset(1, 2).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT bar.foo FROM bar");
	}

	@Test
	public void honorsNamingStrategy() {

		Table bar = SQL.table("bar");
		Column foo = bar.column("foo");

		Select select = Select.builder().select(foo).from(bar).build();

		assertThat(SqlRenderer.create(new SimpleRenderContext(NamingStrategies.toUpper())).render(select))
				.isEqualTo("SELECT BAR.FOO FROM BAR");
	}

	@Test // DATAJDBC-309
	public void shouldRenderAliasedColumnAndFrom() {

		Table table = Table.create("bar").as("my_bar");

		Select select = Select.builder().select(table.column("foo").as("my_foo")).from(table).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT my_bar.foo AS my_foo FROM bar my_bar");
	}

	@Test // DATAJDBC-309
	public void shouldRenderMultipleColumnsFromTables() {

		Table table1 = Table.create("table1");
		Table table2 = Table.create("table2");

		Select select = Select.builder().select(table1.column("col1")).select(table2.column("col2")).from(table1)
				.from(table2).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT table1.col1, table2.col2 FROM table1, table2");
	}

	@Test // DATAJDBC-309
	public void shouldRenderDistinct() {

		Table table = SQL.table("bar");
		Column foo = table.column("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().distinct().select(foo, bar).from(table).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT DISTINCT bar.foo, bar.bar FROM bar");
	}

	@Test // DATAJDBC-309
	public void shouldRenderCountFunction() {

		Table table = SQL.table("bar");
		Column foo = table.column("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().select(Functions.count(foo), bar).from(table).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT COUNT(bar.foo), bar.bar FROM bar");
	}

	@Test // DATAJDBC-340
	public void shouldRenderCountFunctionWithAliasedColumn() {

		Table table = SQL.table("bar");
		Column foo = table.column("foo").as("foo_bar");

		Select select = Select.builder().select(Functions.count(foo), foo).from(table).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT COUNT(bar.foo), bar.foo AS foo_bar FROM bar");
	}

	@Test // DATAJDBC-309
	public void shouldRenderSimpleJoin() {

		Table employee = SQL.table("employee");
		Table department = SQL.table("department");

		Select select = Select.builder().select(employee.column("id"), department.column("name")).from(employee) //
				.join(department).on(employee.column("department_id")).equals(department.column("id")) //
				.build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT employee.id, department.name FROM employee "
				+ "JOIN department ON employee.department_id = department.id");
	}

	@Test // DATAJDBC-340
	public void shouldRenderOuterJoin() {

		Table employee = SQL.table("employee");
		Table department = SQL.table("department");

		Select select = Select.builder().select(employee.column("id"), department.column("name")) //
				.from(employee) //
				.leftOuterJoin(department).on(employee.column("department_id")).equals(department.column("id")) //
				.build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT employee.id, department.name FROM employee "
				+ "LEFT OUTER JOIN department ON employee.department_id = department.id");
	}

	@Test // DATAJDBC-309
	public void shouldRenderSimpleJoinWithAnd() {

		Table employee = SQL.table("employee");
		Table department = SQL.table("department");

		Select select = Select.builder().select(employee.column("id"), department.column("name")).from(employee) //
				.join(department).on(employee.column("department_id")).equals(department.column("id")) //
				.and(employee.column("tenant")).equals(department.column("tenant")) //
				.build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT employee.id, department.name FROM employee " //
				+ "JOIN department ON employee.department_id = department.id " //
				+ "AND employee.tenant = department.tenant");
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

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT employee.id, department.name FROM employee " //
				+ "JOIN department ON employee.department_id = department.id " //
				+ "AND employee.tenant = department.tenant " //
				+ "JOIN tenant tenant_base ON tenant_base.tenant_id = department.tenant");
	}

	@Test // GH-1003
	public void shouldRenderJoinWithInlineQuery() {

		Table employee = SQL.table("employee");
		Table department = SQL.table("department");

		Select innerSelect = Select.builder()
				.select(employee.column("id"), employee.column("department_Id"), employee.column("name")).from(employee)
				.build();

		final InlineQuery one = InlineQuery.create(innerSelect, "one");

		Select select = Select.builder().select(one.column("id"), department.column("name")).from(department) //
				.join(one).on(one.column("department_id")).equals(department.column("id")) //
				.build();

		final String sql = SqlRenderer.toString(select);

		assertThat(sql).isEqualTo("SELECT one.id, department.name FROM department " //
				+ "JOIN (SELECT employee.id, employee.department_Id, employee.name FROM employee) one " //
				+ "ON one.department_id = department.id");
	}

	@Test // GH-1003
	public void shouldRenderJoinWithTwoInlineQueries() {

		Table employee = SQL.table("employee");
		Table department = SQL.table("department");

		Select innerSelectOne = Select.builder()
				.select(employee.column("id"), employee.column("department_Id"), employee.column("name")).from(employee)
				.build();
		Select innerSelectTwo = Select.builder().select(department.column("id"), department.column("name")).from(department)
				.build();

		final InlineQuery one = InlineQuery.create(innerSelectOne, "one");
		final InlineQuery two = InlineQuery.create(innerSelectTwo, "two");

		Select select = Select.builder().select(one.column("id"), two.column("name")).from(one) //
				.join(two).on(two.column("department_id")).equals(one.column("id")) //
				.build();

		final String sql = SqlRenderer.toString(select);
		assertThat(sql).isEqualTo("SELECT one.id, two.name FROM (" //
				+ "SELECT employee.id, employee.department_Id, employee.name FROM employee) one " //
				+ "JOIN (SELECT department.id, department.name FROM department) two " //
				+ "ON two.department_id = one.id");
	}

	@Test // DATAJDBC-309
	public void shouldRenderOrderByName() {

		Table employee = SQL.table("employee").as("emp");
		Column column = employee.column("name").as("emp_name");

		Select select = Select.builder().select(column).from(employee).orderBy(OrderByField.from(column).asc()).build();

		assertThat(SqlRenderer.toString(select))
				.isEqualTo("SELECT emp.name AS emp_name FROM employee emp ORDER BY emp_name ASC");
	}

	@Test // DATAJDBC-309
	public void shouldRenderIsNull() {

		Table table = SQL.table("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().select(bar).from(table).where(Conditions.isNull(bar)).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar IS NULL");
	}

	@Test // DATAJDBC-309
	public void shouldRenderNotNull() {

		Table table = SQL.table("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().select(bar).from(table).where(Conditions.isNull(bar).not()).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar IS NOT NULL");
	}

	@Test // DATAJDBC-309
	public void shouldRenderEqualityCondition() {

		Table table = SQL.table("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().select(bar).from(table).where(Conditions.isEqual(bar, SQL.bindMarker(":name")))
				.build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar = :name");
	}

	@Test // DATAJDBC-309
	public void shouldRendersAndOrConditionWithProperParentheses() {

		Table table = SQL.table("foo");
		Column bar = table.column("bar");
		Column baz = table.column("baz");

		Select select = Select.builder().select(bar).from(table).where(Conditions.isEqual(bar, SQL.bindMarker(":name"))
				.or(Conditions.isEqual(bar, SQL.bindMarker(":name2"))).and(Conditions.isNull(baz))).build();

		assertThat(SqlRenderer.toString(select))
				.isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar = :name OR foo.bar = :name2 AND foo.baz IS NULL");
	}

	@Test // DATAJDBC-309
	public void shouldInWithNamedParameter() {

		Table table = SQL.table("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().select(bar).from(table).where(Conditions.in(bar, SQL.bindMarker(":name"))).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar IN (:name)");
	}

	@Test // DATAJDBC-309
	public void shouldInWithNamedParameters() {

		Table table = SQL.table("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().select(bar).from(table)
				.where(Conditions.in(bar, SQL.bindMarker(":name"), SQL.bindMarker(":name2"))).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar IN (:name, :name2)");
	}

	@Test // DATAJDBC-309
	public void shouldRenderInSubselect() {

		Table foo = SQL.table("foo");
		Column bar = foo.column("bar");

		Table floo = SQL.table("floo");
		Column bah = floo.column("bah");

		Select subselect = Select.builder().select(bah).from(floo).build();

		Select select = Select.builder().select(bar).from(foo).where(bar.in(subselect)).build();

		assertThat(SqlRenderer.toString(select))
				.isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar IN (SELECT floo.bah FROM floo)");
	}

	@Test // DATAJDBC-309
	public void shouldConsiderNamingStrategy() {

		Table foo = SQL.table("Foo");
		Column bar = foo.column("BaR");
		Column baz = foo.column("BaZ");

		Select select = Select.builder().select(bar).from(foo).where(bar.isEqualTo(baz)).build();

		String upper = SqlRenderer.create(new SimpleRenderContext(NamingStrategies.toUpper())).render(select);
		assertThat(upper).isEqualTo("SELECT FOO.BAR FROM FOO WHERE FOO.BAR = FOO.BAZ");

		String lower = SqlRenderer.create(new SimpleRenderContext(NamingStrategies.toLower())).render(select);
		assertThat(lower).isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar = foo.baz");

		String mapped = SqlRenderer.create(new SimpleRenderContext(NamingStrategies.mapWith(StringUtils::uncapitalize)))
				.render(select);
		assertThat(mapped).isEqualTo("SELECT foo.baR FROM foo WHERE foo.baR = foo.baZ");
	}

	@Test // DATAJDBC-340
	public void shouldRenderCountStar() {

		Select select = Select.builder() //
				.select(Functions.count(Expressions.asterisk())) //
				.from(SQL.table("foo")) //
				.build();

		String rendered = SqlRenderer.toString(select);

		assertThat(rendered).isEqualTo("SELECT COUNT(*) FROM foo");
	}

	@Test // DATAJDBC-340
	public void shouldRenderCountTableStar() {

		Table foo = SQL.table("foo");
		Select select = Select.builder() //
				.select(Functions.count(foo.asterisk())) //
				.from(foo) //
				.build();

		String rendered = SqlRenderer.toString(select);

		assertThat(rendered).isEqualTo("SELECT COUNT(foo.*) FROM foo");
	}

	@Test // DATAJDBC-340
	public void shouldRenderFunctionWithAlias() {

		Table foo = SQL.table("foo");
		Select select = Select.builder() //
				.select(Functions.count(foo.asterisk()).as("counter")) //
				.from(foo) //
				.build();

		String rendered = SqlRenderer.toString(select);

		assertThat(rendered).isEqualTo("SELECT COUNT(foo.*) AS counter FROM foo");
	}

	@Test // DATAJDBC-479
	public void shouldRenderWithRenderContext() {

		Table table = Table.create(SqlIdentifier.quoted("my_table"));
		Table join_table = Table.create(SqlIdentifier.quoted("join_table"));
		Select select = Select.builder() //
				.select(Functions.count(table.asterisk()).as("counter"), table.column(SqlIdentifier.quoted("reserved_keyword"))) //
				.from(table) //
				.join(join_table).on(table.column("source")).equals(join_table.column("target")).build();

		String rendered = SqlRenderer.create(new RenderContextFactory(PostgresDialect.INSTANCE).createRenderContext())
				.render(select);

		assertThat(rendered).isEqualTo(
				"SELECT COUNT(\"my_table\".*) AS counter, \"my_table\".\"reserved_keyword\" FROM \"my_table\" JOIN \"join_table\" ON \"my_table\".source = \"join_table\".target");
	}


	@Test // GH-1034
	void simpleComparisonWithStringArguments() {

		Table table_user = SQL.table("User");
		Select select = StatementBuilder
				.select(table_user.column("name"),table_user.column("age"))
				.from(table_user)
				.where(Comparison.create("age",">",20))
				.build();

		final String rendered = SqlRenderer.toString(select);
		assertThat(rendered).isEqualTo("SELECT User.name, User.age FROM User WHERE age > 20");
	}

	@Test // GH-1034
	void simpleComparison() {

		Table table_user = SQL.table("User");
		Select select = StatementBuilder
				.select(table_user.column("name"),table_user.column("age"))
				.from(table_user)
				.where(Comparison.create(table_user.column("age"),">",SQL.literalOf(20)))
				.build();

		final String rendered = SqlRenderer.toString(select);
		assertThat(rendered).isEqualTo("SELECT User.name, User.age FROM User WHERE User.age > 20");
	}
}
