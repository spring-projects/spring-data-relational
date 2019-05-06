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

/**
 * Target to apply bindings to.
 *
 * @author Mark Paluch
 * @see PreparedOperation
 * @see io.r2dbc.spi.Statement#bind
 * @see io.r2dbc.spi.Statement#bindNull
 */
public interface BindTarget {

	/**
	 * Bind a value.
	 *
	 * @param identifier the identifier to bind to.
	 * @param value the value to bind.
	 */
	void bind(Object identifier, Object value);

	/**
	 * Bind a value to an index. Indexes are zero-based.
	 *
	 * @param index the index to bind to.
	 * @param value the value to bind.
	 */
	void bind(int index, Object value);

	/**
	 * Bind a {@code null} value.
	 *
	 * @param identifier the identifier to bind to.
	 * @param type the type of {@literal null} value.
	 */
	void bindNull(Object identifier, Class<?> type);

	/**
	 * Bind a {@code null} value.
	 *
	 * @param index the index to bind to.
	 * @param type the type of {@literal null} value.
	 */
	void bindNull(int index, Class<?> type);
}
