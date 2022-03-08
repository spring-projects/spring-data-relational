/*
* Copyright 2020-2022 the original author or authors.
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
package org.springframework.data.relational.core.sql;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link In}.
 *
 * @author Mark Paluch
 */
public class InTests {

	@Test // DATAJDBC-604
	void shouldRenderToString() {

		Table table = Table.create("table");

		assertThat(In.create(table.column("col"), SQL.bindMarker())).hasToString("table.col IN (?)");
		assertThat(In.create(table.column("col"), SQL.bindMarker()).not()).hasToString("table.col NOT IN (?)");
	}

	@Test // DATAJDBC-604
	void shouldRenderEmptyExpressionToString() {

		Table table = Table.create("table");

		assertThat(In.create(table.column("col"))).hasToString("1 = 0");
		assertThat(In.create(table.column("col")).not()).hasToString("1 = 1");
	}
}
