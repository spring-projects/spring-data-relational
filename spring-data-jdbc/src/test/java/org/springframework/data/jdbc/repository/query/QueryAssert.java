/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.jdbc.repository.query;

import java.util.Collection;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

import org.springframework.data.jdbc.core.dialect.JdbcH2Dialect;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.Escaper;
import org.springframework.data.relational.core.sql.Aliased;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * AssertJ Assertions for {@link ParametrizedQuery} allowing to assert the query string and bind values.
 *
 * @author Mark Paluch
 */
class QueryAssert extends AbstractAssert<QueryAssert, ParametrizedQuery> {

	private static final Dialect DIALECT = JdbcH2Dialect.INSTANCE;

	private QueryAssert(ParametrizedQuery parametrizedQuery) {
		super(parametrizedQuery, QueryAssert.class);
	}

	/**
	 * Entrypoint.
	 *
	 * @param parametrizedQuery
	 * @return
	 */
	public static QueryAssert assertThat(ParametrizedQuery parametrizedQuery) {
		return new QueryAssert(parametrizedQuery).as("Query: ", parametrizedQuery.getQuery());
	}

	/**
	 * Assert that the query contains the given columns, quoted and aliased.
	 *
	 * @param columns
	 * @return
	 */
	public QueryAssert containsQuotedAliasedColumns(Collection<Column> columns) {

		isNotNull();

		for (Column column : columns) {

			String item = quote(column.getTable().getReferenceName()) + "." + quote(column.getName());
			if (column instanceof Aliased aliased) {
				item += " AS " + quote(aliased.getAlias());
			} else {
				item += " AS " + quote(column.getReferenceName());

			}

			Assertions.assertThat(actual.getQuery()).contains(item);
		}

		return this;
	}

	/**
	 * Assert that the query contains the given string.
	 *
	 * @param expected
	 * @return
	 */
	public QueryAssert contains(String expected) {

		isNotNull();

		Assertions.assertThat(actual.getQuery()).contains(expected);

		return this;
	}

	/**
	 * Assert that the query defines a bind value for the given key and that the value matches the expected value.
	 *
	 * @param key
	 * @param value
	 * @return
	 */
	public QueryAssert hasBindValue(String key, Object value) {

		SqlParameterSource parameterSource = actual.getParameterSource(Escaper.DEFAULT);
		Assertions.assertThat(parameterSource.getValue(key))
				.describedAs("Parameter source [%s] shouldn contain value [%s] for key [%s]", parameterSource, value, key)
				.isEqualTo(value);

		return this;
	}

	private static String quote(SqlIdentifier identifier) {
		return DIALECT.getIdentifierProcessing().quote(identifier.getReference());
	}
}
