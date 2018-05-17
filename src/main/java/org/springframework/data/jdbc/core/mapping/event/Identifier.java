/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.jdbc.core.mapping.event;

import java.util.Optional;

import org.springframework.util.Assert;

/**
 * Wrapper for an identifier of an entity. Might either be a {@link Specified} or {@link Unset#UNSET}
 *
 * @author Jens Schauder
 * @since 1.0
 */
public interface Identifier {

	/**
	 * Creates a new {@link Specified} identifier for the given, non-null value.
	 *
	 * @param identifier must not be {@literal null}.
	 * @return will never be {@literal null}.
	 */
	static Specified of(Object identifier) {

		Assert.notNull(identifier, "Identifier must not be null!");

		return SpecifiedIdentifier.of(identifier);
	}

	static Identifier ofNullable(Object identifier) {
		return identifier == null ? Unset.UNSET : of(identifier);
	}

	/**
	 * Creates a new {@link Identifier} for the given optional source value.
	 *
	 * @param identifier must not be {@literal null}.
	 * @return
	 */
	static Identifier of(Optional<? extends Object> identifier) {

		Assert.notNull(identifier, "Identifier must not be null!");

		return identifier.map(it -> (Identifier) Identifier.of(it)).orElse(Unset.UNSET);
	}

	/**
	 * Returns the identifier value.
	 *
	 * @return will never be {@literal null}.
	 */
	Optional<? extends Object> getOptionalValue();

	/**
	 * A specified identifier that exposes a definitely present identifier value.
	 *
	 * @author Oliver Gierke
	 */
	interface Specified extends Identifier {

		/**
		 * Returns the identifier value.
		 *
		 * @return will never be {@literal null}.
		 */
		default Object getValue() {
			return getOptionalValue().orElseThrow(() -> new IllegalStateException("Should not happen!"));
		}
	}
}
