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

package org.springframework.data.relational.core.sql;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for construction of {@link TupleExpression}.
 *
 * @author Jens Schauder
 */
class TupleExpressionUnitTests {

	@Test // GH-574
	void singleExpressionDoesNotGetWrapped() {

		Column testColumn = Column.create("name", Table.create("employee"));

		Expression wrapped = Expressions.of(List.of(testColumn));

		assertThat(wrapped).isSameAs(testColumn);
	}

	@Test // GH-574
	void multipleExpressionsDoGetWrapped() {

		Column testColumn1 = Column.create("first", Table.create("employee"));
		Column testColumn2 = Column.create("last", Table.create("employee"));

		Expression wrapped = Expressions.of(List.of(testColumn1, testColumn2));

		assertThat(wrapped).isInstanceOf(TupleExpression.class);
	}

}
