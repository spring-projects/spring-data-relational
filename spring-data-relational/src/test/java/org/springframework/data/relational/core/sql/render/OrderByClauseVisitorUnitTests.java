/*
 * Copyright 2019-2023 the original author or authors.
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
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.OrderByField;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.Table;

/**
 * Unit tests for {@link OrderByClauseVisitor}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
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

}
