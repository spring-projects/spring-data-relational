/*
 * Copyright 2026-present the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.relational.core.sql.SqlIdentifier.*;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.data.annotation.Id;

/**
 * Unit tests for the default {@link org.springframework.core.env.Environment} of
 * {@link SqlIdentifierExpressionEvaluator}.
 *
 * @author Paul Jones
 */
class SqlIdentifierExpressionEvaluatorEnvironmentUnitTests {

	@Test // GH-2431
	void mappingEntityWithoutExpressionsCreatesNoStandardEnvironment() {

		try (MockedConstruction<StandardEnvironment> environments = mockConstruction(StandardEnvironment.class)) {

			RelationalMappingContext context = new RelationalMappingContext();
			RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(WithPlainColumns.class);
			entity.doWithProperties((RelationalPersistentProperty property) -> property.getColumnName());

			assertThat(environments.constructed()).isEmpty();
		}
	}

	@Test // GH-2431
	void resolvesPlaceholderWithDefaultEnvironment() {

		RelationalMappingContext context = new RelationalMappingContext();
		RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(WithPlaceholderColumn.class);

		assertThat(entity.getRequiredPersistentProperty("name").getColumnName()).isEqualTo(quoted("default_column"));
	}

	static class WithPlainColumns {

		@Id Long id;
		String name;
		@Column("custom_column") String other;
	}

	static class WithPlaceholderColumn {

		@Id Long id;
		@Column("${relational.test.unset-property:default_column}") String name;
	}
}
