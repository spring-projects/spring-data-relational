/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.jdbc.mapping.event;

import lombok.Data;
import lombok.NonNull;

import java.util.Optional;

/**
 * Wrapper for an identifier of an entity. Might either be a {@link Specified} or {@link Unset#UNSET}
 *
 * @author Jens Schauder
 * @since 2.0
 */
public interface Identifier {

	static Identifier fromNullable(Object value) {
		return (value != null) ? new Specified(value) : Unset.UNSET;
	}

	Optional<Object> getOptionalValue();

	/**
	 * An unset identifier. Always returns {@link Optional#empty()} as value.
	 */
	enum Unset implements Identifier {
		UNSET {
			@Override
			public Optional<Object> getOptionalValue() {
				return Optional.empty();
			}
		}
	}

	/**
	 * An {@link Identifier} guaranteed to have a non empty value. Since it is guaranteed to exist the value can get
	 * access directly.
	 */
	@Data
	class Specified implements Identifier {

		@NonNull private final Object value;

		@Override
		public Optional<Object> getOptionalValue() {
			return Optional.of(value);
		}
	}
}
