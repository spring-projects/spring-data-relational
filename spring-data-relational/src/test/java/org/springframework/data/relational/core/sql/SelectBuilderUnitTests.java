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

import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

import org.springframework.data.relational.core.sql.Join.JoinType;

/**
 * Unit tests for {@link SelectBuilder}.
 *
 * @author Mark Paluch
 * @author Myeonghyeon Lee
 */
public class SelectBuilderUnitTests {

	@Test // DATAJDBC-309
	public void simpleSelect() {

		SelectBuilder builder = StatementBuilder.select();

		Table table = SQL.table("mytable");
		Column foo = table.column("foo");
		Column bar = table.column("bar");

		Select select = builder.select(foo, bar).from(table).build();

		CapturingVisitor visitor = new CapturingVisitor();
		select.visit(visitor);

		assertThat(visitor.enter).containsSequence(foo, table, bar, table, new From(table), table);
	}

	@Test // DATAJDBC-309
	public void selectTop() {

		SelectBuilder builder = StatementBuilder.select();

		Table table = SQL.table("mytable");
		Column foo = table.column("foo");

		Select select = builder.top(10).select(foo).from(table).build();

		CapturingVisitor visitor = new CapturingVisitor();
		select.visit(visitor);

		assertThat(visitor.enter).containsSequence(foo, table, new From(table), table);
		assertThat(select.getLimit()).isEqualTo(OptionalLong.of(10));
	}

	@Test // DATAJDBC-347
	public void selectWithWhere() {

		SelectBuilder builder = StatementBuilder.select();

		Table table = SQL.table("mytable");
		Column foo = table.column("foo");

		Comparison condition = foo.isEqualTo(SQL.literalOf("bar"));
		Select select = builder.select(foo).from(table).where(condition).build();

		CapturingVisitor visitor = new CapturingVisitor();
		select.visit(visitor);

		assertThat(visitor.enter).containsSequence(foo, table, new From(table), table, new Where(condition));
	}

	@Test // DATAJDBC-309
	public void moreAdvancedSelect() {

		SelectBuilder builder = StatementBuilder.select();

		Table table1 = SQL.table("mytable1");
		Table table2 = SQL.table("mytable2");

		Column foo = SQL.column("foo", table1).as("foo_from_table1");
		Column bar = SQL.column("foo", table2).as("foo_from_table1");

		Select select = builder.select(foo, bar).from(table1, table2).build();

		CapturingVisitor visitor = new CapturingVisitor();
		select.visit(visitor);

		assertThat(visitor.enter).containsSequence(foo, table1, bar, table2, new From(table1, table2), table1, table2);
	}

	@Test // DATAJDBC-309
	public void orderBy() {

		SelectBuilder builder = StatementBuilder.select();

		Table table = SQL.table("mytable");

		Column foo = SQL.column("foo", table).as("foo");

		OrderByField orderByField = OrderByField.from(foo).asc();
		Select select = builder.select(foo).from(table).orderBy(orderByField).build();

		CapturingVisitor visitor = new CapturingVisitor();
		select.visit(visitor);

		assertThat(visitor.enter).containsSequence(foo, table, new From(table), table, orderByField, foo);
	}

	@Test // DATAJDBC-309
	public void joins() {

		SelectBuilder builder = StatementBuilder.select();

		Table employee = SQL.table("employee");
		Table department = SQL.table("department");

		Column name = employee.column("name").as("emp_name");
		Column department_name = employee.column("name").as("department_name");

		Select select = builder.select(name, department_name).from(employee).join(department)
				.on(SQL.column("department_id", employee)).equals(SQL.column("id", department))
				.and(SQL.column("tenant", employee)).equals(SQL.column("tenant", department))
				.orderBy(OrderByField.from(name).asc()).build();

		CapturingVisitor visitor = new CapturingVisitor();
		select.visit(visitor);

		assertThat(visitor.enter).filteredOn(Join.class::isInstance).hasSize(1);

		Join join = visitor.enter.stream().filter(Join.class::isInstance).map(Join.class::cast).findFirst().get();

		assertThat(join.getJoinTable()).isEqualTo(department);
		assertThat(join.getOn().toString()).isEqualTo(
				new SimpleSegment("employee.department_id = department.id AND employee.tenant = department.tenant").toString());
		assertThat(join.getType()).isEqualTo(JoinType.JOIN);
	}

	@Test // DATAJDBC-498
	public void selectWithLock() {

		SelectBuilder builder = StatementBuilder.select();

		Table table = SQL.table("mytable");
		Column foo = table.column("foo");
		Column bar = table.column("bar");
		LockMode lockMode = LockMode.PESSIMISTIC_WRITE;

		Select select = builder.select(foo, bar).from(table).lock(lockMode).build();

		CapturingVisitor visitor = new CapturingVisitor();
		select.visit(visitor);

		assertThat(visitor.enter).containsSequence(foo, table, bar, table, new From(table), table);
		assertThat(select.getLockMode()).isEqualTo(lockMode);
	}

	@Test // DATAJDBC-498
	public void selectWithWhereWithLock() {

		SelectBuilder builder = StatementBuilder.select();

		Table table = SQL.table("mytable");
		Column foo = table.column("foo");

		Comparison condition = foo.isEqualTo(SQL.literalOf("bar"));
		LockMode lockMode = LockMode.PESSIMISTIC_WRITE;

		Select select = builder.select(foo).from(table).where(condition).lock(lockMode).build();

		CapturingVisitor visitor = new CapturingVisitor();
		select.visit(visitor);

		assertThat(visitor.enter).containsSequence(foo, table, new From(table), table, new Where(condition));
		assertThat(select.getLockMode()).isEqualTo(lockMode);
	}

	@Test // DATAJDBC-498
	public void orderByWithLock() {

		SelectBuilder builder = StatementBuilder.select();

		Table table = SQL.table("mytable");

		Column foo = SQL.column("foo", table).as("foo");

		OrderByField orderByField = OrderByField.from(foo).asc();
		LockMode lockMode = LockMode.PESSIMISTIC_WRITE;

		Select select = builder.select(foo).from(table).orderBy(orderByField).lock(lockMode).build();

		CapturingVisitor visitor = new CapturingVisitor();
		select.visit(visitor);

		assertThat(visitor.enter).containsSequence(foo, table, new From(table), table, orderByField, foo);
		assertThat(select.getLockMode()).isEqualTo(lockMode);
	}
}
