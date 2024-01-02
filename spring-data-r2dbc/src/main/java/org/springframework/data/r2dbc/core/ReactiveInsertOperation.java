/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.r2dbc.core;

import reactor.core.publisher.Mono;

import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * The {@link ReactiveInsertOperation} interface allows creation and execution of {@code INSERT} operations in a fluent
 * API style.
 * <p>
 * By default,the table to operate on is derived from the initial {@link Class domainType} and can be defined there via
 * {@link org.springframework.data.relational.core.mapping.Table} annotation. Using {@code inTable} allows to override
 * the table name for the execution.
 *
 * <pre>
 *     <code>
 *         insert(Jedi.class)
 *             .into("star_wars")
 *             .using(luke);
 *     </code>
 * </pre>
 *
 * @author Mark Paluch
 * @since 1.1
 */
public interface ReactiveInsertOperation {

	/**
	 * Begin creating an {@code INSERT} operation for given {@link Class domainType}.
	 *
	 * @param <T> {@link Class type} of the application domain object.
	 * @param domainType {@link Class type} of the domain object to insert; must not be {@literal null}.
	 * @return new instance of {@link ReactiveInsert}.
	 * @throws IllegalArgumentException if {@link Class domainType} is {@literal null}.
	 * @see ReactiveInsert
	 */
	<T> ReactiveInsert<T> insert(Class<T> domainType);

	/**
	 * Table override (optional).
	 */
	interface InsertWithTable<T> extends TerminatingInsert<T> {

		/**
		 * Explicitly set the {@link String name} of the table.
		 * <p>
		 * Skip this step to use the default table derived from the {@link Class domain type}.
		 *
		 * @param table {@link String name} of the table; must not be {@literal null} or empty.
		 * @return new instance of {@link TerminatingInsert}.
		 * @throws IllegalArgumentException if {@link String table} is {@literal null} or empty.
		 */
		default TerminatingInsert<T> into(String table) {
			return into(SqlIdentifier.unquoted(table));
		}

		/**
		 * Explicitly set the {@link SqlIdentifier name} of the table.
		 * <p>
		 * Skip this step to use the default table derived from the {@link Class domain type}.
		 *
		 * @param table {@link SqlIdentifier name} of the table; must not be {@literal null}.
		 * @return new instance of {@link TerminatingInsert}.
		 * @throws IllegalArgumentException if {@link SqlIdentifier table} is {@literal null}.
		 */
		TerminatingInsert<T> into(SqlIdentifier table);
	}

	/**
	 * Trigger {@code INSERT} execution by calling one of the terminating methods.
	 */
	interface TerminatingInsert<T> {

		/**
		 * Insert exactly one {@link Object}.
		 *
		 * @param object {@link Object} to insert; must not be {@literal null}.
		 * @return the write result for this operation.
		 * @throws IllegalArgumentException if {@link Object} is {@literal null}.
		 * @see Mono
		 */
		Mono<T> using(T object);
	}

	/**
	 * The {@link ReactiveInsert} interface provides methods for constructing {@code INSERT} operations in a fluent way.
	 */
	interface ReactiveInsert<T> extends InsertWithTable<T> {}

}
