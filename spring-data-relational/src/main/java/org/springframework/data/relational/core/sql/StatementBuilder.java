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

import java.util.Collection;

import org.springframework.data.relational.core.sql.SelectBuilder.SelectAndFrom;

/**
 * Entrypoint to build SQL statements.
 *
 * @author Mark Paluch
 * @see SQL
 * @see Expressions
 * @see Conditions
 * @see Functions
 */
public abstract class StatementBuilder {

	/**
	 * Creates a new {@link SelectBuilder} by specifying a {@code SELECT} column.
	 *
	 * @param expression the select list expression.
	 * @return the {@link SelectBuilder} containing {@link Expression}.
	 * @see SelectBuilder#select(Expression)
	 */
	public static SelectAndFrom select(Expression expression) {
		return Select.builder().select(expression);
	}

	/**
	 * Creates a new {@link SelectBuilder} by specifying one or more {@code SELECT} columns.
	 *
	 * @param expressions the select list expressions.
	 * @return the {@link SelectBuilder} containing {@link Expression}s.
	 * @see SelectBuilder#select(Expression...)
	 */
	public static SelectAndFrom select(Expression... expressions) {
		return Select.builder().select(expressions);
	}

	/**
	 * Include one or more {@link Expression}s in the select list.
	 *
	 * @param expressions the expressions to include.
	 * @return {@code this} builder.
	 * @see Table#columns(String...)
	 */
	public static SelectAndFrom select(Collection<? extends Expression> expressions) {
		return Select.builder().select(expressions);
	}

	/**
	 * Creates a new {@link SelectBuilder}.
	 *
	 * @return the new {@link SelectBuilder}.
	 * @see SelectBuilder
	 */
	public static SelectBuilder select() {
		return Select.builder();
	}

	private StatementBuilder() {

	}
}
