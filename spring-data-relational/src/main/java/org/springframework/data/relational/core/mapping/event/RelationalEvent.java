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

import org.springframework.core.ResolvableType;
import org.springframework.core.ResolvableTypeProvider;
import org.springframework.lang.Nullable;

/**
 * an event signalling JDBC processing.
 *
 * @param <E> the type of the entity to which the event relates.
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Jens Schauder
 */
public interface RelationalEvent<E> extends ResolvableTypeProvider {

	/**
	 * @return the entity to which this event refers. Might be {@literal null}.
	 */
	@Nullable
	E getEntity();

	/**
	 * @return the type of the entity to which the event relates.
	 * @since 2.0
	 */
	Class<E> getType();

	@Override
	default ResolvableType getResolvableType() {
		return ResolvableType.forClassWithGenerics(getClass(), getType());
	}
}
