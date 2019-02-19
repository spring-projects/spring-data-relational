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

import java.util.Objects;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * A database value that can be set in a statement.
 *
 * @author Mark Paluch
 * @see OutboundRow
 */
public class SettableValue {

	private final @Nullable Object value;
	private final Class<?> type;

	/**
	 * Create a {@link SettableValue}.
	 *
	 * @param value
	 * @param type
	 */
	public SettableValue(@Nullable Object value, Class<?> type) {

		Assert.notNull(type, "Type must not be null");

		this.value = value;
		this.type = type;
	}

	/**
	 * Returns the column value. Can be {@literal null}.
	 *
	 * @return the column value. Can be {@literal null}.
	 * @see #hasValue()
	 */
	@Nullable
	public Object getValue() {
		return value;
	}

	/**
	 * Returns the column value type. Must be also present if the {@code value} is {@literal null}.
	 *
	 * @return the column value type
	 */
	public Class<?> getType() {
		return type;
	}

	/**
	 * Returns whether this {@link SettableValue} has a value.
	 *
	 * @return whether this {@link SettableValue} has a value. {@literal false} if {@link #getValue()} is {@literal null}.
	 */
	public boolean hasValue() {
		return value != null;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (!(o instanceof SettableValue))
			return false;
		SettableValue value1 = (SettableValue) o;
		return Objects.equals(value, value1.value) && Objects.equals(type, value1.type);
	}

	@Override
	public int hashCode() {
		return Objects.hash(value, type);
	}

	@Override
	public String toString() {
		final StringBuffer sb = new StringBuffer();
		sb.append(getClass().getSimpleName());
		sb.append(" [value=").append(value);
		sb.append(", type=").append(type);
		sb.append(']');
		return sb.toString();
	}
}
