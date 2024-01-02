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

import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * The {@link ReactiveDeleteOperation} interface allows creation and execution of {@code DELETE} operations in a fluent
 * API style.
 * <p>
 * The starting {@literal domainType} is used for mapping the {@link Query} provided via {@code matching}. By default,
 * the table to operate on is derived from the initial {@literal domainType} and can be defined there via
 * {@link org.springframework.data.relational.core.mapping.Table} annotation. Using {@code inTable} allows to override
 * the table name for the execution.
 *
 * <pre>
 *     <code>
 *         delete(Jedi.class)
 *             .from("star_wars")
 *             .matching(query(where("firstname").is("luke")))
 *             .all();
 *     </code>
 * </pre>
 *
 * @author Mark Paluch
 * @since 1.1
 */
public interface ReactiveDeleteOperation {

	/**
	 * Begin creating a {@code DELETE} operation for the given {@link Class domainType}.
	 *
	 * @param domainType {@link Class type} of domain object to delete; must not be {@literal null}.
	 * @return new instance of {@link ReactiveDelete}.
	 * @throws IllegalArgumentException if {@link Class domainType} is {@literal null}.
	 * @see ReactiveDelete
	 */
	ReactiveDelete delete(Class<?> domainType);

	/**
	 * Table override (optional).
	 */
	interface DeleteWithTable extends TerminatingDelete {

		/**
		 * Explicitly set the {@link String name} of the table on which to perform the delete.
		 * <p>
		 * Skip this step to use the default table derived from the {@link Class domain type}.
		 *
		 * @param table {@link String name} of the table; must not be {@literal null} or empty.
		 * @return new instance of {@link DeleteWithQuery}.
		 * @throws IllegalArgumentException if {@link String table} is {@literal null} or empty.
		 * @see DeleteWithQuery
		 */
		default DeleteWithQuery from(String table) {
			return from(SqlIdentifier.unquoted(table));
		}

		/**
		 * Explicitly set the {@link SqlIdentifier name} of the table on which to perform the delete.
		 * <p>
		 * Skip this step to use the default table derived from the {@link Class domain type}.
		 *
		 * @param table {@link SqlIdentifier name} of the table; must not be {@literal null}.
		 * @return new instance of {@link DeleteWithQuery}.
		 * @throws IllegalArgumentException if {@link SqlIdentifier table} is {@literal null}.
		 * @see DeleteWithQuery
		 */
		DeleteWithQuery from(SqlIdentifier table);
	}

	/**
	 * Required {@link Query filter}.
	 */
	interface DeleteWithQuery extends TerminatingDelete {

		/**
		 * Define the {@link Query} used to filter elements in the delete.
		 *
		 * @param query {@link Query} used as the filter in the delete; must not be {@literal null}.
		 * @return new instance of {@link TerminatingDelete}.
		 * @throws IllegalArgumentException if {@link Query} is {@literal null}.
		 * @see TerminatingDelete
		 * @see Query
		 */
		TerminatingDelete matching(Query query);
	}

	/**
	 * Trigger {@code DELETE} operation by calling one of the terminating methods.
	 */
	interface TerminatingDelete {

		/**
		 * Remove all matching rows.
		 *
		 * @return the number of affected rows; never {@literal null}.
		 * @see Mono
		 */
		Mono<Long> all();
	}

	/**
	 * The {@link ReactiveDelete} interface provides methods for constructing {@code DELETE} operations in a fluent way.
	 */
	interface ReactiveDelete extends DeleteWithTable, DeleteWithQuery {}

}
