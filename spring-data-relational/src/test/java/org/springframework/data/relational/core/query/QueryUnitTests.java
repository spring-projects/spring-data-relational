/*
* Copyright 2020-2024 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

/**
 * Tests the {@link Query} class.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
class QueryUnitTests {

	@Test // DATAJDBC-614
	void withCombinesSortAndPaging() {

		Query query = Query.empty() //
				.sort(Sort.by("alpha")) //
				.with(PageRequest.of(2, 20, Sort.by("beta")));

		assertThat(query.getSort().get()) //
				.extracting(Sort.Order::getProperty) //
				.containsExactly("alpha", "beta");
	}

	@Test // DATAJDBC-614
	void withCombinesEmptySortAndPaging() {

		Query query = Query.empty() //
				.with(PageRequest.of(2, 20, Sort.by("beta")));

		assertThat(query.getSort().get()) //
				.extracting(Sort.Order::getProperty) //
				.containsExactly("beta");
	}

	@Test // DATAJDBC-614
	void withCombinesSortAndUnsortedPaging() {

		Query query = Query.empty() //
				.sort(Sort.by("alpha")) //
				.with(PageRequest.of(2, 20));

		assertThat(query.getSort().get()) //
				.extracting(Sort.Order::getProperty) //
				.containsExactly("alpha");
	}

	@Test // GH-1939
	void withCombinesUnpagedWithSort() {

		Query query = Query.empty() //
				.with(Pageable.unpaged(Sort.by("beta")));

		assertThat(query.getSort().get()) //
				.extracting(Sort.Order::getProperty) //
				.containsExactly("beta");
		assertThat(query.getLimit()).isEqualTo(-1);
		assertThat(query.getOffset()).isEqualTo(-1);
	}
}
