/*
 * Copyright 2026-present the original author or authors.
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
 * The {@link ReactiveUpsertOperation} interface allows creation and execution of {@code UPSERT} (insert-or-update)
 * operations in a fluent API style.
 * <p>
 * By default, the table to operate on is derived from the initial {@link Class domainType} and can be defined there
 * via {@link org.springframework.data.relational.core.mapping.Table} annotation. Using {@code inTable} allows
 * overriding the table name for the execution.
 *
 * <pre>
 *     <code>
 *         upsert(Jedi.class)
 *             .inTable("star_wars")
 *             .using(luke);
 *     </code>
 * </pre>
 *
 * @author Christoph Strobl
 * @since 4.x
 */
public interface ReactiveUpsertOperation {

	/**
	 * Begin creating an {@code UPSERT} operation for given {@link Class domainType}.
	 *
	 * @param <T> {@link Class type} of the application domain object.
	 * @param domainType {@link Class type} of the domain object to upsert; must not be {@literal null}.
	 * @return new instance of {@link ReactiveUpsert}.
	 * @throws IllegalArgumentException if {@link Class domainType} is {@literal null}.
	 * @see ReactiveUpsert
	 */
	<T> ReactiveUpsert<T> upsert(Class<T> domainType);

	/**
	 * Table override (optional).
	 */
	interface UpsertWithTable<T> extends TerminatingUpsert<T> {

		/**
		 * Explicitly set the {@link String name} of the table.
		 * <p>
		 * Skip this step to use the default table derived from the {@link Class domain type}.
		 *
		 * @param table {@link String name} of the table; must not be {@literal null} or empty.
		 * @return new instance of {@link TerminatingUpsert}.
		 * @throws IllegalArgumentException if {@link String table} is {@literal null} or empty.
		 */
		default TerminatingUpsert<T> inTable(String table) {
			return inTable(SqlIdentifier.unquoted(table));
		}

		/**
		 * Explicitly set the {@link SqlIdentifier name} of the table.
		 * <p>
		 * Skip this step to use the default table derived from the {@link Class domain type}.
		 *
		 * @param table {@link SqlIdentifier name} of the table; must not be {@literal null}.
		 * @return new instance of {@link TerminatingUpsert}.
		 * @throws IllegalArgumentException if {@link SqlIdentifier table} is {@literal null}.
		 */
		TerminatingUpsert<T> inTable(SqlIdentifier table);
	}

	/**
	 * Trigger {@code UPSERT} execution by calling one of the terminating methods.
	 */
	interface TerminatingUpsert<T> {

		/**
		 * Upsert exactly one {@link Object}.
		 *
		 * @param object {@link Object} to upsert; must not be {@literal null}.
		 * @return the upserted entity.
		 * @throws IllegalArgumentException if {@link Object} is {@literal null}.
		 * @see Mono
		 */
		Mono<T> one(T object);
	}

	/**
	 * The {@link ReactiveUpsert} interface provides methods for constructing {@code UPSERT} operations in a fluent way.
	 */
	interface ReactiveUpsert<T> extends UpsertWithTable<T> {}

}
