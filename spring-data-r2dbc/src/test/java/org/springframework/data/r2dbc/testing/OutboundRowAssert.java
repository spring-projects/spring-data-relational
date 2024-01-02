/*
 * Copyright 2021-2024 the original author or authors.
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
package org.springframework.data.r2dbc.testing;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AbstractMapAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.error.BasicErrorMessageFactory;
import org.assertj.core.error.ShouldBeEmpty;
import org.assertj.core.error.ShouldContain;
import org.assertj.core.error.ShouldNotBeEmpty;

import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.r2dbc.core.Parameter;

/**
 * Assertion methods for {@link OutboundRow}.
 * <p>
 * To create an instance of this class, invoke <code>{@link Assertions#assertThat(OutboundRow)}</code>.
 * <p>
 */
public class OutboundRowAssert extends AbstractMapAssert<OutboundRowAssert, OutboundRow, SqlIdentifier, Parameter> {

	private OutboundRowAssert(OutboundRow actual) {
		super(actual, OutboundRowAssert.class);
	}

	public static OutboundRowAssert assertThat(OutboundRow actual) {
		return new OutboundRowAssert(actual);
	}

	/**
	 * Verifies that the actual row contains the given column.
	 *
	 * @param identifier the given identifier.
	 * @return {@code this} assertions object.
	 * @throws AssertionError if the actual row is {@code null}.
	 * @throws AssertionError if the actual row does not contain the given column.
	 * @see SqlIdentifier#unquoted(String)
	 */
	public OutboundRowAssert containsColumn(String identifier) {
		return containsColumn(SqlIdentifier.unquoted(identifier));
	}

	/**
	 * Verifies that the actual row contains the given column.
	 *
	 * @param identifier the given identifier.
	 * @return {@code this} assertions object.
	 * @throws AssertionError if the actual row is {@code null}.
	 * @throws AssertionError if the actual row does not contain the given column.
	 */
	public OutboundRowAssert containsColumn(SqlIdentifier identifier) {

		isNotNull();
		if (!this.actual.containsKey(identifier)) {
			failWithMessage(ShouldContain.shouldContain(actual, identifier, actual.keySet()).create());
		}

		return this;
	}

	/**
	 * Verifies that the actual row contains only the given columns and nothing else, in any order.
	 *
	 * @param columns the given columns that should be in the actual row.
	 * @return {@code this} assertions object
	 * @throws AssertionError if the actual row is {@code null} or empty.
	 * @throws AssertionError if the actual row does not contain the given columns, i.e. the actual row contains some or
	 *           none of the given columns, or the actual row's columns contains columns not in the given ones.
	 * @throws IllegalArgumentException if the given argument is an empty array.
	 */
	public OutboundRowAssert containsOnlyColumns(String... columns) {
		return containsOnlyColumns(Arrays.stream(columns).map(SqlIdentifier::unquoted).collect(Collectors.toList()));
	}

	/**
	 * Verifies that the actual row contains only the given columns and nothing else, in any order.
	 *
	 * @param columns the given columns that should be in the actual row.
	 * @return {@code this} assertions object
	 * @throws AssertionError if the actual row is {@code null} or empty.
	 * @throws AssertionError if the actual row does not contain the given columns, i.e. the actual row contains some or
	 *           none of the given columns, or the actual row's columns contains columns not in the given ones.
	 * @throws IllegalArgumentException if the given argument is an empty array.
	 */
	public OutboundRowAssert containsOnlyColumns(SqlIdentifier... columns) {
		return containsOnlyColumns(Arrays.asList(columns));
	}

	/**
	 * Verifies that the actual row contains only the given columns and nothing else, in any order.
	 *
	 * @param columns the given columns that should be in the actual row.
	 * @return {@code this} assertions object
	 * @throws AssertionError if the actual row is {@code null} or empty.
	 * @throws AssertionError if the actual row does not contain the given columns, i.e. the actual row contains some or
	 *           none of the given columns, or the actual row's columns contains columns not in the given ones.
	 * @throws IllegalArgumentException if the given argument is an empty array.
	 */
	public OutboundRowAssert containsOnlyColumns(Iterable<SqlIdentifier> columns) {
		containsOnlyKeys(columns);
		return this;
	}

	/**
	 * Verifies that the actual row contains the given column with a specific {@code value}.
	 *
	 * @param identifier the given identifier.
	 * @param value the value to assert for.
	 * @return {@code this} assertions object.
	 * @throws AssertionError if the actual row is {@code null}.
	 * @throws AssertionError if the actual row does not contain the given column.
	 * @see SqlIdentifier#unquoted(String)
	 */
	public OutboundRowAssert containsColumnWithValue(String identifier, Object value) {
		return containsColumnWithValue(SqlIdentifier.unquoted(identifier), value);
	}

	/**
	 * Verifies that the actual row contains the given column with a specific {@code value}.
	 *
	 * @param identifier the given identifier.
	 * @param value the value to assert for.
	 * @return {@code this} assertions object.
	 * @throws AssertionError if the actual row is {@code null}.
	 * @throws AssertionError if the actual row does not contain the given column.
	 */
	public OutboundRowAssert containsColumnWithValue(SqlIdentifier identifier, Object value) {

		isNotNull();

		containsColumn(identifier);
		withColumn(identifier).hasValue(value);

		return this;
	}

	/**
	 * Verifies that the actual row contains the given columns.
	 *
	 * @param identifier the given identifier.
	 * @return {@code this} assertions object.
	 * @throws AssertionError if the actual row is {@code null}.
	 * @throws AssertionError if the actual row does not contain the given column.
	 * @see SqlIdentifier#unquoted(String)
	 */
	public OutboundRowAssert containsColumns(String... identifier) {
		return containsColumns(Arrays.stream(identifier).map(SqlIdentifier::unquoted).toArray(SqlIdentifier[]::new));
	}

	/**
	 * Verifies that the actual row contains the given columns.
	 *
	 * @param identifier the given identifier.
	 * @return {@code this} assertions object.
	 * @throws AssertionError if the actual row is {@code null}.
	 * @throws AssertionError if the actual row does not contain the given column.
	 */
	public OutboundRowAssert containsColumns(SqlIdentifier... identifier) {

		isNotNull();

		containsKeys(identifier);

		return this;
	}

	/**
	 * Verifies that the actual row contains the given column that is {@link Parameter#isEmpty() empty}.
	 *
	 * @param identifier the given identifier.
	 * @return {@code this} assertions object.
	 * @throws AssertionError if the actual row is {@code null}.
	 * @throws AssertionError if the actual row does not contain the given column.
	 * @see SqlIdentifier#unquoted(String)
	 */
	public OutboundRowAssert containsEmptyColumn(String identifier) {
		return containsColumn(SqlIdentifier.unquoted(identifier));
	}

	/**
	 * Verifies that the actual row contains the given column that is {@link Parameter#isEmpty() empty}.
	 *
	 * @param identifier the given identifier.
	 * @return {@code this} assertions object.
	 * @throws AssertionError if the actual row is {@code null}.
	 * @throws AssertionError if the actual row does not contain the given column.
	 */
	public OutboundRowAssert containsEmptyColumn(SqlIdentifier identifier) {

		containsColumn(identifier);

		Parameter parameter = actual.get(identifier);
		if (!parameter.isEmpty()) {
			failWithMessage(ShouldBeEmpty.shouldBeEmpty(parameter).create());
		}

		return this;
	}

	/**
	 * Create a nested assertion for {@link Parameter}.
	 *
	 * @param identifier the given identifier.
	 * @return a new assertions object.
	 */
	public ParameterAssert withColumn(String identifier) {
		return withColumn(SqlIdentifier.unquoted(identifier));
	}

	/**
	 * Create a nested assertion for {@link Parameter}.
	 *
	 * @param identifier the given identifier.
	 * @return a new assertions object.
	 */
	public ParameterAssert withColumn(SqlIdentifier identifier) {
		return new ParameterAssert(actual.get(identifier)).as("Parameter " + identifier);
	}

	/**
	 * Assertion methods for {@link Parameter}.
	 * <p>
	 * To create an instance of this class, invoke <code>Assertions#assertThat(row).withColumn("myColumn")</code>.
	 * <p>
	 */
	public static class ParameterAssert extends AbstractAssert<ParameterAssert, Parameter> {

		ParameterAssert(Parameter parameter) {
			super(parameter, ParameterAssert.class);
		}

		/**
		 * Verifies that the parameter is empty.
		 *
		 * @throws AssertionError if the actual parameter is {@code null}.
		 * @throws AssertionError if the actual parameter contains a value.
		 * @return {@code this} assertions object.
		 */
		public ParameterAssert isEmpty() {

			isNotNull();

			if (!this.actual.isEmpty()) {
				failWithMessage(ShouldBeEmpty.shouldBeEmpty(actual).create());
			}

			return this;
		}

		/**
		 * Verifies that the parameter contains a value.
		 *
		 * @throws AssertionError if the actual parameter is {@code null}.
		 * @throws AssertionError if the actual parameter is empty.
		 * @return {@code this} assertions object.
		 */
		public ParameterAssert hasValue() {

			isNotNull();

			if (!this.actual.hasValue()) {
				failWithMessage(ShouldNotBeEmpty.shouldNotBeEmpty().create());
			}

			return this;
		}

		/**
		 * Verifies that the parameter contains a specific value.
		 *
		 * @throws AssertionError if the actual parameter is {@code null}.
		 * @throws AssertionError if the actual parameter does not match the value.
		 * @return {@code this} assertions object.
		 */
		public ParameterAssert hasValue(Object value) {

			isNotNull();

			Assertions.assertThat(actual.getValue()).isEqualTo(value);

			return this;
		}

		/**
		 * Verifies that the parameter contains a value that is instance of {@code type}.
		 *
		 * @return {@code this} assertions object.
		 */
		public ParameterAssert hasValueInstanceOf(Class<?> type) {

			isNotNull();

			Assertions.assertThat(actual.getValue()).isInstanceOf(type);

			return this;
		}

		/**
		 * Verifies that the parameter type is equal to {@code type}.
		 *
		 * @return {@code this} assertions object.
		 */
		public ParameterAssert hasType(Class<?> type) {

			isNotNull();

			if (!this.actual.getType().equals(type)) {
				failWithMessage(new BasicErrorMessageFactory(
						"Expecting\n" + "  <%s>\n" + "to be instance of:\n" + "  <%s>\n" + "but was not", actual.getType(), type)
								.create());
			}

			return this;
		}

	}

}
