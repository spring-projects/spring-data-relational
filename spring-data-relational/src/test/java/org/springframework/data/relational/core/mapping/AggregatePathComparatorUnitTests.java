/*
 * Copyright 2024 the original author or authors.
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

import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;
import org.springframework.lang.NonNull;

import static org.assertj.core.api.Assertions.*;

class AggregatePathComparatorUnitTests {

	RelationalMappingContext context = new RelationalMappingContext();

	@Test
	void sortPaths() {

		Set<AggregatePath> sorted = new TreeSet<>();

		AggregatePath alpha = path(EntityAlpha.class, "");
		AggregatePath ab = path(EntityAlpha.class, "beta");
		AggregatePath abg = path(EntityAlpha.class, "beta.gamma");
		AggregatePath ag = path(EntityAlpha.class, "gamma");

		sorted.add(ag);
		sorted.add(abg);
		sorted.add(ab);
		sorted.add(alpha);

		assertThat(sorted)
				.containsExactly(alpha, ab, abg, ag);

	}

	@NonNull
	private AggregatePath path(Class<?> baseType, String path) {

		if (path.isEmpty()) {
			return context.getAggregatePath(context.getRequiredPersistentEntity(baseType));
		}

		return context.getAggregatePath(PersistentPropertyPathTestUtils.getPath(context, path, baseType));
	}

	record EntityAlpha(Long id, Beta beta, Gamma gamma) {
	}

	record Beta(Long id, Gamma gamma) {}

	record Gamma(Long id) {}
}
