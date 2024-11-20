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
package org.springframework.data.relational.core.sql.render;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.dialect.PostgresDialect;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.sql.*;
import org.springframework.util.StringUtils;

import java.util.List;

/**
 * Unit tests for {@link SqlRenderer}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Sven Rienstra
 */
class SelectRendererUnitTests {

	@Test // DATAJDBC-309, DATAJDBC-278
	void shouldRenderSingleColumn() {

		Table bar = SQL.table("bar");
		Column foo = bar.column("foo");

		Select select = Select.builder().select(foo).from(bar).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT bar.foo FROM bar");
	}

	@Test
	void honorsNamingStrategy() {

		Table bar = SQL.table("bar");
		Column foo = bar.column("foo");

		Select select = Select.builder().select(foo).from(bar).build();

		assertThat(SqlRenderer.create(new SimpleRenderContext(NamingStrategies.toUpper())).render(select))
				.isEqualTo("SELECT BAR.FOO FROM BAR");
	}

	@Test // DATAJDBC-309
	void shouldRenderAliasedColumnAndFrom() {

		Table table = Table.create("bar").as("my_bar");

		Select select = Select.builder().select(table.column("foo").as("my_foo")).from(table).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT my_bar.foo AS my_foo FROM bar my_bar");
	}

	@Test // DATAJDBC-309
	void shouldRenderMultipleColumnsFromTables() {

		Table table1 = Table.create("table1");
		Table table2 = Table.create("table2");

		Select select = Select.builder().select(table1.column("col1")).select(table2.column("col2")).from(table1)
				.from(table2).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT table1.col1, table2.col2 FROM table1, table2");
	}

	@Test // DATAJDBC-309
	void shouldRenderDistinct() {

		Table table = SQL.table("bar");
		Column foo = table.column("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().distinct().select(foo, bar).from(table).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT DISTINCT bar.foo, bar.bar FROM bar");
	}

	@Test // DATAJDBC-309
	void shouldRenderCountFunction() {

		Table table = SQL.table("bar");
		Column foo = table.column("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().select(Functions.count(foo), bar).from(table).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT COUNT(bar.foo), bar.bar FROM bar");
	}

	@Test // DATAJDBC-340
	void shouldRenderCountFunctionWithAliasedColumn() {

		Table table = SQL.table("bar");
		Column foo = table.column("foo").as("foo_bar");

		Select select = Select.builder().select(Functions.count(foo), foo).from(table).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT COUNT(bar.foo), bar.foo AS foo_bar FROM bar");
	}

	@Test // DATAJDBC-309
	void shouldRenderSimpleJoin() {

		Table employee = SQL.table("employee");
		Table department = SQL.table("department");

		Select select = Select.builder().select(employee.column("id"), department.column("name")).from(employee) //
				.join(department).on(employee.column("department_id")).equals(department.column("id")) //
				.build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT employee.id, department.name FROM employee "
				+ "JOIN department ON employee.department_id = department.id");
	}

	@Test // DATAJDBC-340
	void shouldRenderOuterJoin() {

		Table employee = SQL.table("employee");
		Table department = SQL.table("department");

		Select select = Select.builder().select(employee.column("id"), department.column("name")) //
				.from(employee) //
				.leftOuterJoin(department).on(employee.column("department_id")).equals(department.column("id")) //
				.build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT employee.id, department.name FROM employee "
				+ "LEFT OUTER JOIN department ON employee.department_id = department.id");
	}

	@Test // GH-1421
	void shouldRenderFullOuterJoin() {

		Table employee = SQL.table("employee");
		Table department = SQL.table("department");

		Select select = Select.builder().select(employee.column("id"), department.column("name")) //
				.from(employee) //
				.join(department, Join.JoinType.FULL_OUTER_JOIN).on(employee.column("department_id"))
				.equals(department.column("id")) //
				.build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT employee.id, department.name FROM employee "
				+ "FULL OUTER JOIN department ON employee.department_id = department.id");
	}

	@Test // DATAJDBC-309
	void shouldRenderSimpleJoinWithAnd() {

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

	@Test // #995
	void shouldRenderArbitraryJoinCondition() {

		Table employee = SQL.table("employee");
		Table department = SQL.table("department");

		Select select = Select.builder() //
				.select(employee.column("id"), department.column("name")) //
				.from(employee) //
				.join(department) //
				.on(Conditions.isEqual(employee.column("department_id"), department.column("id")) //
						.or(Conditions.isNotEqual(employee.column("tenant"), department.column("tenant")) //
						)).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT employee.id, department.name FROM employee " //
				+ "JOIN department ON employee.department_id = department.id " //
				+ "OR employee.tenant != department.tenant");
	}

	@Test // #1009
	void shouldRenderJoinWithJustExpression() {

		Table employee = SQL.table("employee");
		Table department = SQL.table("department");

		Select select = Select.builder().select(employee.column("id"), department.column("name")).from(employee) //
				.join(department).on(Expressions.just("alpha")).equals(Expressions.just("beta")) //
				.build();

		assertThat(SqlRenderer.toString(select))
				.isEqualTo("SELECT employee.id, department.name FROM employee " + "JOIN department ON alpha = beta");
	}

	@Test // DATAJDBC-309
	void shouldRenderMultipleJoinWithAnd() {

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
	void shouldRenderJoinWithInlineQuery() {

		Table employee = SQL.table("employee");
		Table department = SQL.table("department");

		Select innerSelect = Select.builder()
				.select(employee.column("id"), employee.column("department_Id"), employee.column("name")).from(employee)
				.build();

		InlineQuery one = InlineQuery.create(innerSelect, "one");

		Select select = Select.builder().select(one.column("id"), department.column("name")).from(department) //
				.join(one).on(one.column("department_id")).equals(department.column("id")) //
				.build();

		String sql = SqlRenderer.toString(select);

		assertThat(sql).isEqualTo("SELECT one.id, department.name FROM department " //
				+ "JOIN (SELECT employee.id, employee.department_Id, employee.name FROM employee) one " //
				+ "ON one.department_id = department.id");
	}

	@Test // GH-1362
	void shouldRenderNestedJoins() {

		Table merchantCustomers = Table.create("merchants_customers");
		Table customerDetails = Table.create("customer_details");

		Select innerSelect = Select.builder().select(customerDetails.column("cd_user_id")).from(customerDetails)
				.join(merchantCustomers)
				.on(merchantCustomers.column("mc_user_id").isEqualTo(customerDetails.column("cd_user_id"))).build();

		InlineQuery innerTable = InlineQuery.create(innerSelect, "inner");

		Select select = Select.builder().select(merchantCustomers.asterisk()) //
				.from(merchantCustomers) //
				.join(innerTable).on(innerTable.column("i_user_id").isEqualTo(merchantCustomers.column("mc_user_id"))) //
				.build();

		String sql = SqlRenderer.toString(select);

		assertThat(sql).isEqualTo("SELECT merchants_customers.* FROM merchants_customers " + //
				"JOIN (" + //
				"SELECT customer_details.cd_user_id " + //
				"FROM customer_details " + //
				"JOIN merchants_customers ON merchants_customers.mc_user_id = customer_details.cd_user_id" + //
				") inner " + //
				"ON inner.i_user_id = merchants_customers.mc_user_id");
	}

	@Test // GH-1003
	void shouldRenderJoinWithTwoInlineQueries() {

		Table employee = SQL.table("employee");
		Table department = SQL.table("department");

		Select innerSelectOne = Select.builder()
				.select(employee.column("id").as("empId"), employee.column("department_Id"), employee.column("name"))
				.from(employee).build();
		Select innerSelectTwo = Select.builder().select(department.column("id"), department.column("name")).from(department)
				.build();

		InlineQuery one = InlineQuery.create(innerSelectOne, "one");
		InlineQuery two = InlineQuery.create(innerSelectTwo, "two");

		Select select = Select.builder().select(one.column("empId"), two.column("name")).from(one) //
				.join(two).on(two.column("department_id")).equals(one.column("empId")) //
				.build();

		String sql = SqlRenderer.toString(select);
		assertThat(sql).isEqualTo("SELECT one.empId, two.name FROM (" //
				+ "SELECT employee.id AS empId, employee.department_Id, employee.name FROM employee) one " //
				+ "JOIN (SELECT department.id, department.name FROM department) two " //
				+ "ON two.department_id = one.empId");
	}

	@Test // DATAJDBC-309
	void shouldRenderOrderByName() {

		Table employee = SQL.table("employee").as("emp");
		Column column = employee.column("name");

		Select select = Select.builder().select(column).from(employee).orderBy(OrderByField.from(column).asc()).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT emp.name FROM employee emp ORDER BY emp.name ASC");
	}

	@Test // GH-968
	void shouldRenderOrderByAlias() {

		Table employee = SQL.table("employee").as("emp");
		Column column = employee.column("name").as("my_emp_name");

		Select select = Select.builder().select(column).from(employee).orderBy(OrderByField.from(column).asc()).build();

		assertThat(SqlRenderer.toString(select))
				.isEqualTo("SELECT emp.name AS my_emp_name FROM employee emp ORDER BY my_emp_name ASC");
	}

	@Test // DATAJDBC-309
	void shouldRenderIsNull() {

		Table table = SQL.table("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().select(bar).from(table).where(Conditions.isNull(bar)).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar IS NULL");
	}

	@Test // DATAJDBC-309
	void shouldRenderNotNull() {

		Table table = SQL.table("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().select(bar).from(table).where(Conditions.isNull(bar).not()).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar IS NOT NULL");
	}

	@Test // DATAJDBC-309
	void shouldRenderEqualityCondition() {

		Table table = SQL.table("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().select(bar).from(table).where(Conditions.isEqual(bar, SQL.bindMarker(":name")))
				.build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar = :name");
	}

	@Test // DATAJDBC-309
	void shouldRendersAndOrConditionWithProperParentheses() {

		Table table = SQL.table("foo");
		Column bar = table.column("bar");
		Column baz = table.column("baz");

		Select select = Select.builder().select(bar).from(table).where(Conditions.isEqual(bar, SQL.bindMarker(":name"))
				.or(Conditions.isEqual(bar, SQL.bindMarker(":name2"))).and(Conditions.isNull(baz))).build();

		assertThat(SqlRenderer.toString(select))
				.isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar = :name OR foo.bar = :name2 AND foo.baz IS NULL");
	}

	@Test // DATAJDBC-309
	void shouldInWithNamedParameter() {

		Table table = SQL.table("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().select(bar).from(table).where(Conditions.in(bar, SQL.bindMarker(":name"))).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar IN (:name)");
	}

	@Test // DATAJDBC-309
	void shouldInWithNamedParameters() {

		Table table = SQL.table("foo");
		Column bar = table.column("bar");

		Select select = Select.builder().select(bar).from(table)
				.where(Conditions.in(bar, SQL.bindMarker(":name"), SQL.bindMarker(":name2"))).build();

		assertThat(SqlRenderer.toString(select)).isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar IN (:name, :name2)");
	}

	@Test // DATAJDBC-309
	void shouldRenderInSubselect() {

		Table foo = SQL.table("foo");
		Column bar = foo.column("bar");

		Table floo = SQL.table("floo");
		Column bah = floo.column("bah");

		Select subselect = Select.builder().select(bah).from(floo).build();

		Select select = Select.builder().select(bar).from(foo).where(bar.in(subselect)).build();

		assertThat(SqlRenderer.toString(select))
				.isEqualTo("SELECT foo.bar FROM foo WHERE foo.bar IN (SELECT floo.bah FROM floo)");
	}

	@Test // GH-1831
	void shouldRenderSimpleFunctionWithSubselect() {

		Table foo = SQL.table("foo");

		Table floo = SQL.table("floo");
		Column bah = floo.column("bah");


		Select subselect = Select.builder().select(bah).from(floo).build();

		SimpleFunction func = SimpleFunction.create("func", List.of(SubselectExpression.of(subselect)));

		Select select = Select.builder() //
				.select(func.as("alias")) //
				.from(foo) //
				.where(Conditions.isEqual(func, SQL.literalOf(23))) //
				.build();

		assertThat(SqlRenderer.toString(select))
				.isEqualTo("SELECT func(SELECT floo.bah FROM floo) AS alias FROM foo WHERE func(SELECT floo.bah FROM floo) = 23");
	}

	@Test // DATAJDBC-309
	void shouldConsiderNamingStrategy() {

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
	void shouldRenderCountStar() {

		Select select = Select.builder() //
				.select(Functions.count(Expressions.asterisk())) //
				.from(SQL.table("foo")) //
				.build();

		String rendered = SqlRenderer.toString(select);

		assertThat(rendered).isEqualTo("SELECT COUNT(*) FROM foo");
	}

	@Test // DATAJDBC-340
	void shouldRenderCountTableStar() {

		Table foo = SQL.table("foo");
		Select select = Select.builder() //
				.select(Functions.count(foo.asterisk())) //
				.from(foo) //
				.build();

		String rendered = SqlRenderer.toString(select);

		assertThat(rendered).isEqualTo("SELECT COUNT(foo.*) FROM foo");
	}

	@Test // DATAJDBC-340
	void shouldRenderFunctionWithAlias() {

		Table foo = SQL.table("foo");
		Select select = Select.builder() //
				.select(Functions.count(foo.asterisk()).as("counter")) //
				.from(foo) //
				.build();

		String rendered = SqlRenderer.toString(select);

		assertThat(rendered).isEqualTo("SELECT COUNT(foo.*) AS counter FROM foo");
	}

	@Test // DATAJDBC-479
	void shouldRenderWithRenderContext() {

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
		Select select = StatementBuilder.select(table_user.column("name"), table_user.column("age")).from(table_user)
				.where(Comparison.create("age", ">", 20)).build();

		String rendered = SqlRenderer.toString(select);
		assertThat(rendered).isEqualTo("SELECT User.name, User.age FROM User WHERE age > 20");
	}

	@Test // GH-1034
	void simpleComparison() {

		Table table_user = SQL.table("User");
		Select select = StatementBuilder.select(table_user.column("name"), table_user.column("age")).from(table_user)
				.where(Comparison.create(table_user.column("age"), ">", SQL.literalOf(20))).build();

		String rendered = SqlRenderer.toString(select);
		assertThat(rendered).isEqualTo("SELECT User.name, User.age FROM User WHERE User.age > 20");
	}

	@Test // GH-1066
	void shouldRenderCast() {

		Table table_user = SQL.table("User");
		Select select = StatementBuilder.select(Expressions.cast(table_user.column("name"), "VARCHAR2")).from(table_user)
				.build();

		String rendered = SqlRenderer.toString(select);
		assertThat(rendered).isEqualTo("SELECT CAST(User.name AS VARCHAR2) FROM User");
	}

	@Test // GH-1076
	void rendersLimitAndOffset() {

		Table table_user = SQL.table("User");
		Select select = StatementBuilder.select(table_user.column("name")).from(table_user).limitOffset(10, 5).build();

		String rendered = SqlRenderer.toString(select);
		assertThat(rendered).isEqualTo("SELECT User.name FROM User OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY");
	}

	@Test // GH-1076
	void rendersLimit() {

		Table table_user = SQL.table("User");
		Select select = StatementBuilder.select(table_user.column("name")).from(table_user) //
				.limit(3) //
				.build();

		String rendered = SqlRenderer.toString(select);
		assertThat(rendered).isEqualTo("SELECT User.name FROM User FETCH FIRST 3 ROWS ONLY");
	}

	@Test // GH-1076
	void rendersLock() {

		Table table_user = SQL.table("User");
		Select select = StatementBuilder.select(table_user.column("name")).from(table_user) //
				.lock(LockMode.PESSIMISTIC_READ) //
				.build();

		String rendered = SqlRenderer.toString(select);
		assertThat(rendered).isEqualTo("SELECT User.name FROM User FOR UPDATE");
	}

	@Test // GH-1076
	void rendersLockAndOffset() {

		Table table_user = SQL.table("User");
		Select select = StatementBuilder.select(table_user.column("name")).from(table_user).offset(3) //
				.lock(LockMode.PESSIMISTIC_WRITE) //
				.build();

		String rendered = SqlRenderer.toString(select);
		assertThat(rendered).isEqualTo("SELECT User.name FROM User FOR UPDATE OFFSET 3 ROWS");
	}

	@Test // GH-1076
	void rendersLockAndOffsetUsingDialect() {

		Table table_user = SQL.table("User");
		Select select = StatementBuilder.select(table_user.column("name")).from(table_user).limitOffset(3, 6) //
				.lock(LockMode.PESSIMISTIC_WRITE) //
				.build();

		String rendered = SqlRenderer.create(new RenderContextFactory(PostgresDialect.INSTANCE).createRenderContext())
				.render(select);
		assertThat(rendered).isEqualTo("SELECT User.name FROM User LIMIT 3 OFFSET 6 FOR UPDATE OF User");
	}

	@Test // GH-1007
	void shouldRenderConditionAsExpression() {

		Table table = SQL.table("User");
		Select select = StatementBuilder.select( //
				Conditions.isGreater(table.column("age"), SQL.literalOf(18))) //
				.from(table) //
				.build();

		String rendered = SqlRenderer.toString(select);
		assertThat(rendered).isEqualTo("SELECT User.age > 18 FROM User");
	}

	@Test // GH-968
	void rendersFullyQualifiedNamesInOrderBy() {

		Table tableA = SQL.table("tableA");
		Column tableAName = tableA.column("name");
		Column tableAId = tableA.column("id");

		Table tableB = SQL.table("tableB");
		Column tableBId = tableB.column("id");
		Column tableBName = tableB.column("name");

		Select select = StatementBuilder.select(Expressions.asterisk()) //
				.from(tableA) //
				.join(tableB).on(tableAId.isEqualTo(tableBId)) //
				.orderBy(tableAName, tableBName) //
				.build();

		String rendered = SqlRenderer.toString(select);
		assertThat(rendered)
				.isEqualTo("SELECT * FROM tableA JOIN tableB ON tableA.id = tableB.id ORDER BY tableA.name, tableB.name");
	}

	@Test // GH-1446
	void rendersAliasedExpression() {

		Table table = SQL.table("table");
		Column tableName = table.column("name");

		Select select = StatementBuilder.select(new AliasedExpression(tableName, "alias")) //
				.from(table) //
				.build();

		String rendered = SqlRenderer.toString(select);
		assertThat(rendered).isEqualTo("SELECT table.name AS alias FROM table");
	}

	@Test // GH-1653
	void notOfNested() {

		Table table = SQL.table("atable");

		Select select = StatementBuilder.select(table.asterisk()).from(table).where(Conditions.nest(
				table.column("id").isEqualTo(Expressions.just("1")).and(table.column("id").isEqualTo(Expressions.just("2"))))
				.not()).build();
		String sql = SqlRenderer.toString(select);

		assertThat(sql).isEqualTo("SELECT atable.* FROM atable WHERE NOT (atable.id = 1 AND atable.id = 2)");

		select = StatementBuilder.select(table.asterisk()).from(table).where(Conditions.not(Conditions.nest(
				table.column("id").isEqualTo(Expressions.just("1")).and(table.column("id").isEqualTo(Expressions.just("2"))))))
				.build();
		sql = SqlRenderer.toString(select);

		assertThat(sql).isEqualTo("SELECT atable.* FROM atable WHERE NOT (atable.id = 1 AND atable.id = 2)");
	}

	@Test // GH-1945
	void notOfTrue() {

		Select selectFalse = Select.builder().select(Expressions.just("*")).from("test_table")
				.where(Conditions.just("true").not()).build();
		String renderSelectFalse = SqlRenderer.create().render(selectFalse);

		assertThat(renderSelectFalse).isEqualTo("SELECT * FROM test_table WHERE NOT true");
	}

	@Test // GH-1945
	void notOfNestedTrue() {

		Select selectFalseNested = Select.builder().select(Expressions.just("*")).from("test_table")
				.where(Conditions.nest(Conditions.just("true")).not()).build();
		String renderSelectFalseNested = SqlRenderer.create().render(selectFalseNested);

		assertThat(renderSelectFalseNested).isEqualTo("SELECT * FROM test_table WHERE NOT (true)");
	}

	@Test // GH-1651
	void asteriskOfAliasedTableUsesAlias() {

		Table employee = SQL.table("employee").as("e");
		Select select = Select.builder().select(employee.asterisk()).select(employee.column("id")).from(employee).build();

		String rendered = SqlRenderer.toString(select);

		assertThat(rendered).isEqualTo("SELECT e.*, e.id FROM employee e");
	}

	@Test
	void rendersCaseExpression() {

		Table table = SQL.table("table");
		Column column = table.column("name");

		CaseExpression caseExpression = CaseExpression.create(When.when(column.isNull(), SQL.literalOf(1))) //
				.when(When.when(column.isNotNull(), column)) //
				.elseExpression(SQL.literalOf(3));

		Select select = StatementBuilder.select(caseExpression) //
				.from(table) //
				.build();

		String rendered = SqlRenderer.toString(select);
		assertThat(rendered).isEqualTo("SELECT CASE WHEN table.name IS NULL THEN 1 WHEN table.name IS NOT NULL THEN table.name ELSE 3 END FROM table");
	}

	/**
	 * Tests the rendering of analytic functions.
	 */
	@Nested
	class AnalyticFunctionsTests {

		Table employee = SQL.table("employee");
		Column department = employee.column("department");
		Column age = employee.column("age");
		Column salary = employee.column("salary");

		@Test // GH-1019
		void renderEmptyOver() {

			Select select = StatementBuilder.select( //
							AnalyticFunction.create("MAX", salary) //
					) //
					.from(employee) //
					.build();

			String rendered = SqlRenderer.toString(select);

			assertThat(rendered).isEqualTo("SELECT MAX(employee.salary) OVER() FROM employee");
		}

		@Test // GH-1019
		void renderPartition() {

			Select select = StatementBuilder.select( //
					AnalyticFunction.create("MAX", salary) //
							.partitionBy(department) //
			) //
					.from(employee) //
					.build();

			String rendered = SqlRenderer.toString(select);

			assertThat(rendered)
					.isEqualTo("SELECT MAX(employee.salary) OVER(PARTITION BY employee.department) FROM employee");
		}

		@Test // GH-1019
		void renderOrderBy() {

			Select select = StatementBuilder.select( //
					AnalyticFunction.create("MAX", salary) //
							.orderBy(age) //
			) //
					.from(employee) //
					.build();

			String rendered = SqlRenderer.toString(select);

			assertThat(rendered).isEqualTo("SELECT MAX(employee.salary) OVER(ORDER BY employee.age) FROM employee");
		}

		@Test // GH-1019
		void renderFullAnalyticFunction() {

			final Select select = StatementBuilder.select( //
					AnalyticFunction.create("MAX", salary) //
							.partitionBy(department) //
							.orderBy(age) //
			) //
					.from(employee) //
					.build();

			String rendered = SqlRenderer.toString(select);

			assertThat(rendered).isEqualTo(
					"SELECT MAX(employee.salary) OVER(PARTITION BY employee.department ORDER BY employee.age) FROM employee");
		}

		@Test // GH-1019
		void renderAnalyticFunctionWithAlias() {

			final Select select = StatementBuilder.select( //
					AnalyticFunction.create("MAX", salary) //
							.partitionBy(department) //
							.orderBy(age) //
							.as("MAX_SELECT")) //
					.from(employee) //
					.build();

			String rendered = SqlRenderer.toString(select);

			assertThat(rendered).isEqualTo(
					"SELECT MAX(employee.salary) OVER(PARTITION BY employee.department ORDER BY employee.age) AS MAX_SELECT FROM employee");
		}

		@Test // GH-1153
		void renderAnalyticFunctionWithOutArgument() {

			final Select select = StatementBuilder.select( //
					AnalyticFunction.create("ROW_NUMBER") //
							.partitionBy(department)) //
					.from(employee) //
					.build();

			String rendered = SqlRenderer.toString(select);

			assertThat(rendered).isEqualTo("SELECT ROW_NUMBER() OVER(PARTITION BY employee.department) FROM employee");
		}
	}
}
