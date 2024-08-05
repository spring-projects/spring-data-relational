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

import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.sql.*;

import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link OrderByClauseVisitor}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Koen Punt
 * @author Sven Rienstra
 */
class OrderByClauseVisitorUnitTests {

	@Test // DATAJDBC-309
	void shouldRenderOrderByAlias() {

		Table employee = SQL.table("employee").as("emp");
		Column column = employee.column("name").as("emp_name");

		Select select = Select.builder().select(column).from(employee).orderBy(OrderByField.from(column).asc()).build();

		OrderByClauseVisitor visitor = new OrderByClauseVisitor(new SimpleRenderContext(NamingStrategies.asIs()));
		select.visit(visitor);

		assertThat(visitor.getRenderedPart().toString()).isEqualTo("emp_name ASC");
	}

	@Test // DATAJDBC-309
	void shouldApplyNamingStrategy() {

		Table employee = SQL.table("employee").as("emp");
		Column column = employee.column("name").as("emp_name");

		Select select = Select.builder().select(column).from(employee).orderBy(OrderByField.from(column).asc()).build();

		OrderByClauseVisitor visitor = new OrderByClauseVisitor(new SimpleRenderContext(NamingStrategies.toUpper()));
		select.visit(visitor);

		assertThat(visitor.getRenderedPart().toString()).isEqualTo("EMP_NAME ASC");
	}

	@Test // GH-968
	void shouldRenderOrderByFullyQualifiedName() {

		Table employee = SQL.table("employee");
		Column column = employee.column("name");

		Select select = Select.builder().select(column).from(employee).orderBy(OrderByField.from(column).asc()).build();

		OrderByClauseVisitor visitor = new OrderByClauseVisitor(new SimpleRenderContext(NamingStrategies.asIs()));
		select.visit(visitor);

		assertThat(visitor.getRenderedPart().toString()).isEqualTo("employee.name ASC");
	}

	@Test // GH-968
	void shouldRenderOrderByFullyQualifiedNameWithTableAlias() {

		Table employee = SQL.table("employee").as("emp");
		Column column = employee.column("name");

		Select select = Select.builder().select(column).from(employee).orderBy(OrderByField.from(column).asc()).build();

		OrderByClauseVisitor visitor = new OrderByClauseVisitor(new SimpleRenderContext(NamingStrategies.asIs()));
		select.visit(visitor);

		assertThat(visitor.getRenderedPart().toString()).isEqualTo("emp.name ASC");
	}

	@Test // GH-1348
	void shouldRenderOrderBySimpleFunction() {

		Table employee = SQL.table("employee").as("emp");
		Column column = employee.column("name");
		List<Expression> columns = Arrays.asList(employee.column("id"), column);

		SimpleFunction simpleFunction = SimpleFunction.create("GREATEST", columns);

		Select select = Select.builder().select(column).from(employee)
				.orderBy(OrderByField.from(simpleFunction).asc(), OrderByField.from(column).asc()).build();

		OrderByClauseVisitor visitor = new OrderByClauseVisitor(new SimpleRenderContext(NamingStrategies.asIs()));
		select.visit(visitor);

		assertThat(visitor.getRenderedPart().toString()).isEqualTo("GREATEST(emp.id, emp.name) ASC, emp.name ASC");
	}

	@Test // GH-1348
	void shouldRenderOrderBySimpleExpression() {

		Table employee = SQL.table("employee").as("emp");
		Column column = employee.column("name");

		Expression simpleExpression = Expressions.just("1");

		Select select = Select.builder().select(column).from(employee).orderBy(OrderByField.from(simpleExpression).asc())
				.build();

		OrderByClauseVisitor visitor = new OrderByClauseVisitor(new SimpleRenderContext(NamingStrategies.asIs()));
		select.visit(visitor);

		assertThat(visitor.getRenderedPart().toString()).isEqualTo("1 ASC");
	}

	@Test
	void shouldRenderOrderByCase() {

		Table employee = SQL.table("employee").as("emp");
		Column column = employee.column("name");

		CaseExpression caseExpression = CaseExpression.create(When.when(column.isNull(), SQL.literalOf(1))).elseExpression(SQL.literalOf(column));
		Select select = Select.builder().select(column).from(employee).orderBy(OrderByField.from(caseExpression).asc()).build();

		OrderByClauseVisitor visitor = new OrderByClauseVisitor(new SimpleRenderContext(NamingStrategies.asIs()));
		select.visit(visitor);

		assertThat(visitor.getRenderedPart().toString()).isEqualTo("CASE WHEN emp.name IS NULL THEN 1 ELSE emp.name END ASC");
	}
}
