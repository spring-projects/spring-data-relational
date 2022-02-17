/*
 * Copyright 2017-2022 the original author or authors.
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

import java.util.List;

import org.springframework.data.convert.EntityWriter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

/**
 * Converts an aggregate represented by its root into an {@link MutableAggregateChange}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
public class RelationalEntityWriter implements EntityWriter<Object, MutableAggregateChange<?>> {

	private final RelationalMappingContext context;

	public RelationalEntityWriter(RelationalMappingContext context) {
		this.context = context;
	}

	@Override
	public void write(Object root, MutableAggregateChange<?> aggregateChange) {

		List<DbAction<?>> actions = new WritingContext(context, root, aggregateChange).save();
		actions.forEach(aggregateChange::addAction);
	}
}
