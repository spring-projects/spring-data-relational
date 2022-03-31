/*
 * Copyright 2022 the original author or authors.
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

/**
 * Represents the changes happening to one or more aggregates (as used in the context of Domain Driven Design) as a
 * whole. This change allows additional {@link MutableAggregateChange} of a particular kind to be merged into it to
 * broadly represent the changes to multiple aggregates across all such merged changes.
 *
 * @author Chirag Tailor
 * @since 3.0
 */
public interface MergedAggregateChange<T, C extends MutableAggregateChange<T>> extends AggregateChange<T> {
	/**
	 * Merges a {@code MutableAggregateChange} into this {@code MergedAggregateChange}.
	 *
	 * @param aggregateChange must not be {@literal null}.
	 * @return the change resulting from the merge. Guaranteed to be not {@literal null}.
	 */
	MergedAggregateChange<T, C> merge(C aggregateChange);
}
