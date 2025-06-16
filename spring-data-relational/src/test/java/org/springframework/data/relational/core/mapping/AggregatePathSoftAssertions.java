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

import java.util.function.Consumer;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.SoftAssertionsProvider;

/**
 * Soft assertions for {@link AggregatePath} instances.
 *
 * @author Jens Schauder
 * @since 4.0
 */
public class AggregatePathSoftAssertions extends SoftAssertions {

	/**
	 * Entry point for assertions. The default {@literal assertThat} can't be used, since it collides with {@link SoftAssertions#assertThat(Iterable)}
	 */
	public AggregatePathAssertions assertAggregatePath(AggregatePath actual) {
		return proxy(AggregatePathAssertions.class, AggregatePath.class, actual);
	}

	static void assertAggregatePathsSoftly(Consumer<AggregatePathSoftAssertions> softly) {
		SoftAssertionsProvider.assertSoftly(AggregatePathSoftAssertions.class, softly);
	}
}
