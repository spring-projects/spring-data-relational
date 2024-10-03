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
package org.springframework.data.r2dbc.repository.query;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

import org.springframework.data.expression.ValueExpressionParser;

/**
 * Unit tests for {@link ExpressionQuery}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
class ExpressionQueryUnitTests {

	@Test // gh-373
	void bindsMultipleSpelParametersCorrectly() {

		ExpressionQuery query = ExpressionQuery
				.create(ValueExpressionParser.create(), "INSERT IGNORE INTO table (x, y) VALUES (:#{#point.x}, :${point.y})");

		assertThat(query.getQuery())
				.isEqualTo("INSERT IGNORE INTO table (x, y) VALUES (:__synthetic_0__, :__synthetic_1__)");

		assertThat(query.getBindings()).hasSize(2);
		assertThat(query.getBindings().get(0).getExpression()).isEqualTo("#{#point.x}");
		assertThat(query.getBindings().get(0).getParameterName()).isEqualTo("__synthetic_0__");
		assertThat(query.getBindings().get(1).getExpression()).isEqualTo("${point.y}");
		assertThat(query.getBindings().get(1).getParameterName()).isEqualTo("__synthetic_1__");
	}
}
