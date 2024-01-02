/*
 * Copyright 2017-2024 the original author or authors.
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
package org.springframework.data.relational.core.mapping.event;

import java.util.Objects;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Wrapper for an identifier of an entity.
 *
 * @author Jens Schauder
 */
public final class Identifier {

	private final Object value;

	private Identifier(Object value) {

		Assert.notNull(value, "Identifier must not be null");

		this.value = value;
	}

	/**
	 * Creates a new {@link Identifier} identifier for the given, non-null value.
	 *
	 * @param identifier must not be {@literal null}.
	 * @return will never be {@literal null}.
	 */
	public static Identifier of(Object identifier) {

		return new Identifier(identifier);
	}

	/**
	 * Returns the identifier value.
	 *
	 * @return will never be {@literal null}.
	 */
	public Object getValue() {
		return value;
	}

	@Override
	public boolean equals(@Nullable Object o) {

		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		Identifier that = (Identifier) o;
		return value.equals(that.value);
	}

	@Override
	public int hashCode() {
		return Objects.hash(value);
	}
}
