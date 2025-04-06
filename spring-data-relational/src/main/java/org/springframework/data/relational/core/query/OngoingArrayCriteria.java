/*
 * Copyright 2020-2025 the original author or authors.
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

package org.springframework.data.relational.core.query;

/**
 * An Ongoing Criteria builder for {@link java.sql.Types#ARRAY SQL Array} related operations.
 * Used by intermediate builder objects returned from the {@link Criteria}.
 *
 * @author Mikhail Polivakha
 */
public interface OngoingArrayCriteria {

	/**
	 * Builds a {@link Criteria} where the pre-defined array must contain given values.
	 *
	 * @param values values to be present in the array
	 * @return built {@link Criteria}
	 */
	Criteria contains(Object... values);
}
