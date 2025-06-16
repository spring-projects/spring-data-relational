/*
 * Copyright 2025 the original author or authors.
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

import org.assertj.core.api.AbstractAssert;

/**
 * Custom AssertJ assertions for {@link AggregatePath} instances
 * 
 * @author Jens Schauder
 * @since 4.0
 */
public class AggregatePathAssertions extends AbstractAssert<AggregatePathAssertions, AggregatePath> {

	/**
	 * Constructor taking the actual {@link AggregatePath} to assert over.
	 * 
	 * @param actual
	 */
	public AggregatePathAssertions(AggregatePath actual) {
		super(actual, AggregatePathAssertions.class);
	}

	/**
	 * Entry point for creating assertions for AggregatePath.
	 */
	public static AggregatePathAssertions assertThat(AggregatePath actual) {
		return new AggregatePathAssertions(actual);
	}

	/**
	 * Assertion method comparing the path of the actual AggregatePath with the provided String representation of a path
	 * in dot notation. Note that the assertion does not test the root entity type of the AggregatePath.
	 */
	public AggregatePathAssertions hasPath(String expectedPath) {
		isNotNull();

		if (!actual.toDotPath().equals(expectedPath)) { // Adjust this condition based on your AggregatePath's path logic
			failWithMessage("Expected path to be <%s> but was <%s>", expectedPath, actual.toString());
		}
		return this;
	}

	/**
	 * assertion testing if the actual path is a root path.
	 */
	public AggregatePathAssertions isRoot() {
		isNotNull();

		if (!actual.isRoot()) {
			failWithMessage("Expected AggregatePath to be root path, but it was not");
		}
		return this;
	}

	/**
	 * assertion testing if the actual path is NOT a root path.
	 */
	public AggregatePathAssertions isNotRoot() {
		isNotNull();

		if (actual.isRoot()) {
			failWithMessage("Expected AggregatePath not to be root path, but it was.");
		}
		return this;
	}
}
