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

import org.springframework.data.relational.core.sql.BindMarker.NamedBindMarker;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Utility to create SQL {@link Segment}s. Typically used as entry point to the Statement Builder. Objects and dependent
 * objects created by the Query AST are immutable except for builders.
 * <p>
 * The Statement Builder API is intended for framework usage to produce SQL required for framework operations.
 * </p>
 * 
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 * @see Expressions
 * @see Conditions
 * @see Functions
 * @see StatementBuilder
 */
public abstract class SQL {

	/**
	 * Creates a new {@link Column} associated with a source {@link Table}.
	 *
	 * @param name column name, must not be {@literal null} or empty.
	 * @param table table name, must not be {@literal null}.
	 * @return the column with {@code name} associated with {@link Table}.
	 */
	public static Column column(String name, Table table) {
		return Column.create(name, table);
	}

	/**
	 * Creates a new {@link Table}.
	 *
	 * @param name table name, must not be {@literal null} or empty.
	 * @return the column with {@code name}.
	 */
	public static Table table(String name) {
		return Table.create(name);
	}

	/**
	 * Creates a new parameter bind marker.
	 *
	 * @return a new {@link BindMarker}.
	 */
	public static BindMarker bindMarker() {
		return new BindMarker();
	}

	/**
	 * Creates a new parameter bind marker associated with a {@code name} hint.
	 *
	 * @param name name hint, must not be {@literal null} or empty.
	 * @return a new {@link BindMarker}.
	 */
	public static BindMarker bindMarker(String name) {

		Assert.hasText(name, "Name must not be null or empty");

		return new NamedBindMarker(name);
	}

	/**
	 * Creates a new {@link BooleanLiteral} rendering either {@code TRUE} or {@literal FALSE} depending on the given
	 * {@code value}.
	 *
	 * @param value the literal content.
	 * @return a new {@link BooleanLiteral}.
	 * @since 2.0
	 */
	public static BooleanLiteral literalOf(boolean value) {
		return new BooleanLiteral(value);
	}

	/**
	 * Creates a new {@link StringLiteral} from the {@code content}.
	 *
	 * @param content the literal content.
	 * @return a new {@link StringLiteral}.
	 */
	public static StringLiteral literalOf(@Nullable CharSequence content) {
		return new StringLiteral(content);
	}

	/**
	 * Creates a new {@link NumericLiteral} from the {@code content}.
	 *
	 * @param content the literal content.
	 * @return a new {@link NumericLiteral}.
	 */
	public static NumericLiteral literalOf(@Nullable Number content) {
		return new NumericLiteral(content);
	}

	/**
	 * Creates a new {@link Literal} from the {@code content}.
	 *
	 * @param content the literal content.
	 * @return a new {@link Literal}.
	 */
	public static <T> Literal<T> literalOf(@Nullable T content) {
		return new Literal<>(content);
	}

	/**
	 * Creates a new {@code NULL} {@link Literal}.
	 *
	 * @return a new {@link Literal}.
	 */
	public static <T> Literal<T> nullLiteral() {
		return new Literal<>(null);
	}

	// Utility constructor.
	private SQL() {}
}
