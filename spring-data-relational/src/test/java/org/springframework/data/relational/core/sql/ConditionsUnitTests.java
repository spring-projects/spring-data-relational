package org.springframework.data.relational.core.sql;

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

/*
 * Copyright 2021-2024 the original author or authors.
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
class ConditionsUnitTests {

	@Test // GH-916
	void notInOfColumnAndExpression() {

		Table table = Table.create("t");
		Column column = Column.create("col", table);
		Expression expression = new Literal<>("expression");

		In notIn = Conditions.notIn(column, expression);

		List<Visitable> segments = new ArrayList<>();
		notIn.visit(segments::add);

		assertThat(notIn.isNotIn()).isTrue();
		assertThat(segments).containsExactly(notIn, column, table, expression);
	}
}
