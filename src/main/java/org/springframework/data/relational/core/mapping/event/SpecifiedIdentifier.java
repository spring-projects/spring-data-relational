/*
 * Copyright 2017-2020 the original author or authors.
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

import lombok.NonNull;
import lombok.Value;

import java.util.Optional;

import org.springframework.data.relational.core.mapping.event.Identifier.Specified;

/**
 * Simple value object for {@link Specified}.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 */
@Value(staticConstructor = "of")
class SpecifiedIdentifier implements Specified {

	@NonNull Object value;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.mapping.event.Identifier#getOptionalValue()
	 */
	@Override
	public Optional<?> getOptionalValue() {
		return Optional.of(value);
	}
}
