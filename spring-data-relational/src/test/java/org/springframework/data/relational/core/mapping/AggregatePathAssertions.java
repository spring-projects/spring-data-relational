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

public class AggregatePathAssertions extends AbstractAssert<AggregatePathAssertions, AggregatePath> {

	// Constructor for initializing with an AggregatePath instance
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
	 * Example custom assertion method: Asserts that the AggregatePath has a specific property.
	 */
	public AggregatePathAssertions hasPath(String expectedPath) {
		isNotNull();

		if (!actual.toDotPath().equals(expectedPath)) { // Adjust this condition based on your AggregatePath's path logic
			failWithMessage("Expected path to be <%s> but was <%s>", expectedPath, actual.toString());
		}
		return this;
	}

	public AggregatePathAssertions isRoot() {
		isNotNull();

		if (!actual.isRoot()) {
			failWithMessage("Expected AggregatePath to be root path, but it was not");
		}
		return this;
	}

	public AggregatePathAssertions isNotRoot() {
		isNotNull();

		if (actual.isRoot()) {
			failWithMessage("Expected AggregatePath not to be root path, but it was.");
		}
		return this;
	}

	/**
	 * Example custom assertion method: Validates the depth of the path.
	 */
	public AggregatePathAssertions hasLength(int expectedLength) {
		isNotNull();

		if (actual.getLength() != expectedLength) {
			failWithMessage("Expected path length to be <%d> but was <%d>", expectedLength, actual.getLength());
		}
		return this;
	}

}