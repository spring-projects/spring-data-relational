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
package org.springframework.data.relational.core.sql;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.UnaryOperator;

import org.springframework.data.util.Streamable;

/**
 * Represents a named object that exists in the database like a table name or a column name. SQL identifiers are created
 * from a {@link String name} with specifying whether the name should be quoted or unquoted.
 * <p>
 * {@link SqlIdentifier} renders its name using {@link IdentifierProcessing} rules. Use {@link #getReference()} to refer
 * to an object using the identifier when e.g. obtaining values from a result or providing values for a prepared
 * statement. {@link #toSql(IdentifierProcessing)} renders the identifier for SQL statement usage.
 * <p>
 * {@link SqlIdentifier} objects are immutable. Calling transformational methods such as
 * {@link #transform(UnaryOperator)} creates a new instance.
 * <p>
 * {@link SqlIdentifier} are composable so an identifier may consist of a single identifier part or can be composed from
 * multiple parts. Composed identifier can be traversed with {@link #stream()} or {@link #iterator()}. The iteration
 * order depends on the actual composition ordering.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Kurt Niemi
 * @since 2.0
 */
public interface SqlIdentifier extends Streamable<SqlIdentifier> {

	/**
	 * Null-object.
	 */
	SqlIdentifier EMPTY = new SqlIdentifier() {

		@Override
		public Iterator<SqlIdentifier> iterator() {
			return Collections.emptyIterator();
		}

		@Override
		public SqlIdentifier transform(UnaryOperator<String> transformationFunction) {
			return this;
		}

		@Override
		public String toSql(IdentifierProcessing processing) {
			throw new UnsupportedOperationException("An empty SqlIdentifier can't be used in to create SQL snippets");
		}

		public String toString() {
			return "<NULL-IDENTIFIER>";
		}
	};

	/**
	 * The reference name is used for programmatic access to the object identified by this {@link SqlIdentifier}. Use this
	 * method whenever accessing a column in a ResultSet and we do not want any quoting applied.
	 *
	 * @return the string representation of the identifier, which may be used to access columns in a {@link java.sql.ResultSet}
	 * @see IdentifierProcessing#NONE
	 */
	default String getReference() {
		return toSql(IdentifierProcessing.NONE);
	}

	/**
	 * Use this method when rendering an identifier in SQL statements as in:
	 * 
	 * <pre>
	 * <code>select yourColumn from someTable</code>
	 * </pre>
	 * 
	 * {@link IdentifierProcessing} rules are applied to the identifier.
	 *
	 * @param processing identifier processing rules.
	 * @return
	 */
	String toSql(IdentifierProcessing processing);

	/**
	 * Transform the SQL identifier name by applying a {@link UnaryOperator transformation function}. The transformation
	 * function must return a valid, {@literal non-null} identifier {@link String}.
	 *
	 * @param transformationFunction the transformation function. Must return a {@literal non-null} identifier
	 *          {@link String}.
	 * @return a new {@link SqlIdentifier} with the transformation applied.
	 */
	SqlIdentifier transform(UnaryOperator<String> transformationFunction);

	/**
	 * Create a new quoted identifier given {@code name}.
	 *
	 * @param name the identifier.
	 * @return a new quoted identifier given {@code name}.
	 */
	static SqlIdentifier quoted(String name) {
		return new DefaultSqlIdentifier(name, true);
	}

	/**
	 * Create a new unquoted identifier given {@code name}.
	 *
	 * @param name the identifier.
	 * @return a new unquoted identifier given {@code name}.
	 */
	static SqlIdentifier unquoted(String name) {
		return new DefaultSqlIdentifier(name, false);
	}

	/**
	 * Create a new composite {@link SqlIdentifier} from one or more {@link SqlIdentifier}s.
	 * <p>
	 * Composite identifiers do not allow {@link #transform(UnaryOperator)} transformation.
	 * </p>
	 *
	 * @param sqlIdentifiers the elements of the new identifier.
	 * @return the new composite identifier.
	 */
	static SqlIdentifier from(SqlIdentifier... sqlIdentifiers) {
		return new CompositeSqlIdentifier(sqlIdentifiers);
	}
}
