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

import static org.springframework.data.relational.core.sql.UpdateBuilder.*;

import java.util.Collection;

import org.springframework.data.relational.core.sql.DeleteBuilder.DeleteWhere;
import org.springframework.data.relational.core.sql.InsertBuilder.InsertIntoColumnsAndValues;
import org.springframework.data.relational.core.sql.SelectBuilder.SelectAndFrom;

/**
 * Entrypoint to build SQL statements.
 *
 * @author Mark Paluch
 * @since 1.1
 * @see SQL
 * @see Expressions
 * @see Conditions
 * @see Functions
 */
public abstract class StatementBuilder {

	/**
	 * Creates a new {@link SelectBuilder} and includes the {@code SELECT} columns.
	 *
	 * @param expression the select list expression.
	 * @return the {@link SelectBuilder} containing {@link Expression}.
	 * @see SelectBuilder#select(Expression)
	 */
	public static SelectAndFrom select(Expression expression) {
		return Select.builder().select(expression);
	}

	/**
	 * Creates a new {@link SelectBuilder} and includes one or more {@code SELECT} columns.
	 *
	 * @param expressions the select list expressions.
	 * @return the {@link SelectBuilder} containing {@link Expression}s.
	 * @see SelectBuilder#select(Expression...)
	 */
	public static SelectAndFrom select(Expression... expressions) {
		return Select.builder().select(expressions);
	}

	/**
	 * Creates a new {@link SelectBuilder} and includes one or more {@link Expression}s in the select list.
	 *
	 * @param expressions the expressions to include.
	 * @return the {@link SelectBuilder} containing {@link Expression}s.
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

	/**
	 * Creates a new {@link InsertBuilder} and declare the {@link Table} to insert into.
	 *
	 * @param table the table to insert into.
	 * @return the new {@link InsertBuilder}.
	 * @see Table#create(String)
	 */
	public static InsertIntoColumnsAndValues insert(Table table) {
		return insert().into(table);
	}

	/**
	 * Creates a new {@link InsertBuilder}.
	 *
	 * @return the new {@link InsertBuilder}.
	 * @see InsertBuilder
	 */
	public static InsertBuilder insert() {
		return Insert.builder();
	}

	/**
	 * Creates a new {@link UpdateBuilder} and declare the {@link Table} for the update.
	 *
	 * @param table the table for the update.
	 * @return the new {@link UpdateBuilder}.
	 * @see Table#create(String)
	 */
	public static UpdateAssign update(Table table) {
		return update().table(table);
	}

	/**
	 * Creates a new {@link UpdateBuilder}.
	 *
	 * @return the new {@link UpdateBuilder}.
	 * @see UpdateBuilder
	 */
	public static UpdateBuilder update() {
		return Update.builder();
	}

	/**
	 * Creates a new {@link DeleteBuilder} and declares the {@link Table} to delete from.
	 *
	 * @param table the table to delete from.
	 * @return {@code this} builder.
	 * @see Table#columns(String...)
	 */
	public static DeleteWhere delete(Table table) {
		return delete().from(table);
	}

	/**
	 * Creates a new {@link DeleteBuilder}.
	 *
	 * @return the new {@link DeleteBuilder}.
	 * @see DeleteBuilder
	 */
	public static DeleteBuilder delete() {
		return Delete.builder();
	}

	private StatementBuilder() {}
}
