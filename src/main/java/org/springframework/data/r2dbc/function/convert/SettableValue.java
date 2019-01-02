/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.r2dbc.function.convert;

import org.springframework.lang.Nullable;

/**
 * A database value that can be set in a statement.
 *
 * @author Mark Paluch
 */
public class SettableValue {

	private final Object identifier;
	private final @Nullable Object value;
	private final Class<?> type;

	/**
	 * Create a {@link SettableValue} using an integer index.
	 *
	 * @param index
	 * @param value
	 * @param type
	 */
	public SettableValue(int index, @Nullable Object value, Class<?> type) {

		this.identifier = index;
		this.value = value;
		this.type = type;
	}

	/**
	 * Create a {@link SettableValue} using a {@link String} identifier.
	 *
	 * @param identifier
	 * @param value
	 * @param type
	 */
	public SettableValue(String identifier, @Nullable Object value, Class<?> type) {

		this.identifier = identifier;
		this.value = value;
		this.type = type;
	}

	public Object getIdentifier() {
		return identifier;
	}

	@Nullable
	public Object getValue() {
		return value;
	}

	public Class<?> getType() {
		return type;
	}
}
