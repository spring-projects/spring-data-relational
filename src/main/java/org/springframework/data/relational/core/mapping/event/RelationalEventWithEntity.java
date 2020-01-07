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

import java.util.Optional;

import org.springframework.data.relational.core.conversion.AggregateChange;

/**
 * A {@link SimpleRelationalEvent} which is guaranteed to have an entity.
 *
 * @author Jens Schauder
 */
public class RelationalEventWithEntity extends SimpleRelationalEvent implements WithEntity {

	private static final long serialVersionUID = 4891455396602090638L;

	RelationalEventWithEntity(Identifier id, Object entity, AggregateChange change) {
		super(id, Optional.of(entity), change);
	}
}
