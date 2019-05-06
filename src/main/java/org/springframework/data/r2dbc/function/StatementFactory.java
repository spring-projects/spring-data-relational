/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.r2dbc.function;

import java.util.Collection;
import java.util.function.Consumer;

import org.springframework.data.r2dbc.dialect.Dialect;
import org.springframework.data.r2dbc.domain.PreparedOperation;
import org.springframework.data.r2dbc.domain.SettableValue;
import org.springframework.data.relational.core.sql.Delete;
import org.springframework.data.relational.core.sql.Insert;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.Update;

/**
 * Interface declaring statement methods that are commonly used for {@code SELECT/INSERT/UPDATE/DELETE} operations.
 * These methods consider {@link Dialect} specifics and accept bind parameters with values.
 *
 * @author Mark Paluch
 * @see PreparedOperation
 */
public interface StatementFactory {

	/**
	 * Creates a {@link Select} statement.
	 *
	 * @param tableName must not be {@literal null} or empty.
	 * @param columnNames the columns to project, must not be {@literal null} or empty.
	 * @param binderConsumer customizer for bindings. Supports only
	 *          {@link StatementBinderBuilder#filterBy(String, SettableValue)} bindings.
	 * @return the {@link PreparedOperation} to select the given columns.
	 */
	PreparedOperation<Select> select(String tableName, Collection<String> columnNames,
			Consumer<StatementBinderBuilder> binderConsumer);

	/**
	 * Creates a {@link Insert} statement.
	 *
	 * @param tableName must not be {@literal null} or empty.
	 * @param generatedKeysNames must not be {@literal null}.
	 * @param binderConsumer customizer for bindings. Supports only
	 *          {@link StatementBinderBuilder#bind(String, SettableValue)} bindings.
	 * @return the {@link PreparedOperation} to update values in {@code tableName} assigning bound values.
	 */
	PreparedOperation<Insert> insert(String tableName, Collection<String> generatedKeysNames,
			Consumer<StatementBinderBuilder> binderConsumer);

	/**
	 * Creates a {@link Update} statement.
	 *
	 * @param tableName must not be {@literal null} or empty.
	 * @param binderConsumer customizer for bindings.
	 * @return the {@link PreparedOperation} to update values in {@code tableName} assigning bound values.
	 */
	PreparedOperation<Update> update(String tableName, Consumer<StatementBinderBuilder> binderConsumer);

	/**
	 * Creates a {@link Delete} statement.
	 *
	 * @param tableName must not be {@literal null} or empty.
	 * @param binderConsumer customizer for bindings. Supports only
	 *          {@link StatementBinderBuilder#filterBy(String, SettableValue)} bindings.
	 * @return the {@link PreparedOperation} to delete rows from {@code tableName}.
	 */
	PreparedOperation<Delete> delete(String tableName, Consumer<StatementBinderBuilder> binderConsumer);

	/**
	 * Binder to specify parameter bindings by name. Bindings match to equals comparisons.
	 */
	interface StatementBinderBuilder {

		/**
		 * Bind the given Id {@code value} to this builder using the underlying binding strategy to express a filter
		 * condition. {@link Collection} type values translate to {@code IN} matching.
		 *
		 * @param identifier named identifier that is considered by the underlying binding strategy.
		 * @param settable must not be {@literal null}. Use {@link SettableValue#empty(Class)} for {@code NULL} values.
		 */
		void filterBy(String identifier, SettableValue settable);

		/**
		 * Bind the given {@code value} to this builder using the underlying binding strategy.
		 *
		 * @param identifier named identifier that is considered by the underlying binding strategy.
		 * @param settable must not be {@literal null}. Use {@link SettableValue#empty(Class)} for {@code NULL} values.
		 */
		void bind(String identifier, SettableValue settable);
	}
}
