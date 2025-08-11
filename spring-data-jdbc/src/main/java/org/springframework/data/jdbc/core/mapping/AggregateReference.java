/*
 * Copyright 2018-2025 the original author or authors.
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
package org.springframework.data.jdbc.core.mapping;

/**
 * A reference to the aggregate root of a different aggregate.
 *
 * @param <T> the type of the referenced aggregate root.
 * @param <ID> the type of the id of the referenced aggregate root.
 * @author Jens Schauder
 * @author Myeonghyeon Lee
 * @since 1.0
 */
public interface AggregateReference<T, ID> {

	/**
	 * Creates an {@link AggregateReference} that refers to the target aggregate root with the given id.
	 *
	 * @param id aggregate identifier. Must not be {@literal null}, can be a simple value or a composite id (complex
	 *          object).
	 * @return
	 * @param <T> target aggregate type.
	 * @param <ID> target aggregate identifier type.
	 */
	static <T, ID> AggregateReference<T, ID> to(ID id) {
		return new IdOnlyAggregateReference<>(id);
	}

	/**
	 * @return the id of the referenced aggregate.
	 */
	ID getId();

}
