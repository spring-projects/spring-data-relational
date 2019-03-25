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
package org.springframework.data.r2dbc.domain;

import io.r2dbc.spi.Statement;

import org.springframework.data.r2dbc.domain.BindTarget;
import org.springframework.data.r2dbc.domain.PreparedOperation;
import org.springframework.data.r2dbc.domain.QueryOperation;
import org.springframework.data.r2dbc.domain.SettableValue;

/**
 * Extension to {@link QueryOperation} for operations that allow parameter substitution by binding parameter values.
 * {@link BindableOperation} is typically created with a {@link Set} of column names or parameter names that accept bind
 * parameters by calling {@link #bind(Statement, String, Object)}.
 *
 * @author Mark Paluch
 * @see Statement#bind
 * @see Statement#bindNull TODO: Refactor to {@link PreparedOperation}.
 */
public interface BindableOperation extends QueryOperation {

	/**
	 * Bind the given {@code value} to the {@link Statement} using the underlying binding strategy.
	 *
	 * @param bindTarget the bindTarget to bind the value to.
	 * @param identifier named identifier that is considered by the underlying binding strategy.
	 * @param value the actual value. Must not be {@literal null}. Use {@link #bindNull(Statement, Class)} for
	 *          {@literal null} values.
	 * @see Statement#bind
	 */
	void bind(BindTarget bindTarget, String identifier, Object value);

	/**
	 * Bind a {@literal null} value to the {@link Statement} using the underlying binding strategy.
	 *
	 * @param bindTarget the bindTarget to bind the value to.
	 * @param identifier named identifier that is considered by the underlying binding strategy.
	 * @param valueType value type, must not be {@literal null}.
	 * @see Statement#bindNull
	 */
	void bindNull(BindTarget bindTarget, String identifier, Class<?> valueType);

	/**
	 * Bind a {@link SettableValue} to the {@link Statement} using the underlying binding strategy. Binds either the
	 * {@link SettableValue#getValue()} or {@literal null}, depending on whether the value is {@literal null}.
	 *
	 * @param bindTarget the bindTarget to bind the value to.
	 * @param value the settable value
	 * @see Statement#bind
	 * @see Statement#bindNull
	 */
	default void bind(BindTarget bindTarget, String identifier, SettableValue value) {

		if (value.getValue() == null) {
			bindNull(bindTarget, identifier, value.getType());
		} else {
			bind(bindTarget, identifier, value.getValue());
		}
	}

}
