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

import org.springframework.data.relational.core.mapping.event.Identifier.Specified;

/**
 * Interface for {@link SimpleRelationalEvent}s which are guaranteed to have a {@link Specified} identifier. Offers direct
 * access to the {@link Specified} identifier.
 *
 * @author Jens Schauder
 */
public interface WithId extends RelationalEvent {

	/**
	 * Events with an identifier will always return a {@link Specified} one.
	 */
	Specified getId();
}
