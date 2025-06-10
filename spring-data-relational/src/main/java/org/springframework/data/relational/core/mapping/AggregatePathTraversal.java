/*
 * Copyright 2023-2025 the original author or authors.
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
package org.springframework.data.relational.core.mapping;

import java.util.function.Predicate;

/**
 * Traversal methods for {@link AggregatePath} to find paths that define the ID or own the table.
 *
 * @author Mark Paluch
 * @since 3.2
 */
public class AggregatePathTraversal {

	/**
	 * Get the path that defines the identifier of the aggregate.
	 *
	 * @param aggregatePath
	 * @return
	 */
	public static AggregatePath getIdDefiningPath(AggregatePath aggregatePath) {

		Predicate<AggregatePath> idDefiningPathFilter = ap -> !ap.equals(aggregatePath)
				&& (ap.isRoot() || ap.hasIdProperty());

		AggregatePath result = aggregatePath.filter(idDefiningPathFilter);
		if (result == null) {
			throw new IllegalStateException(
					"No identifier associated within this aggregate path: %s".formatted(aggregatePath));
		}
		return result;
	}

	/**
	 * Get the path that owns the table of the aggregate.
	 *
	 * @param aggregatePath
	 * @return
	 */
	public static AggregatePath getTableOwningPath(AggregatePath aggregatePath) {

		Predicate<AggregatePath> tableOwningPathFilter = ap -> ap.isEntity() && !ap.isEmbedded();

		AggregatePath result = aggregatePath.filter(tableOwningPathFilter);
		if (result == null) {
			throw new IllegalStateException("No table associated within this aggregate path: %s".formatted(aggregatePath));
		}
		return result;
	}

}
