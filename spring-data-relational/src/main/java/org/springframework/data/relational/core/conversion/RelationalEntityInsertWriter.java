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
package org.springframework.data.relational.core.conversion;

import org.springframework.data.convert.EntityWriter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

/**
 * Converts an aggregate represented by its root into a {@link RootAggregateChange}. Does not perform any isNew
 * check.
 *
 * @author Thomas Lang
 * @author Jens Schauder
 * @author Chirag Tailor
 * @since 1.1
 */
public class RelationalEntityInsertWriter<T> implements EntityWriter<T, RootAggregateChange<T>> {

	private final RelationalMappingContext context;

	public RelationalEntityInsertWriter(RelationalMappingContext context) {
		this.context = context;
	}

	@Override
	public void write(T root, RootAggregateChange<T> aggregateChange) {
		new WritingContext<>(context, root, aggregateChange).insert();
	}
}
