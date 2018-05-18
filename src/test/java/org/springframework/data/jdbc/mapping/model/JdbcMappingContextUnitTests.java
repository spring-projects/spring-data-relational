/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.mapping.model;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;

import org.junit.Test;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * Unit tests for {@link JdbcMappingContext}.
 *
 * @author Jens Schauder
 */
public class JdbcMappingContextUnitTests {

	NamingStrategy namingStrategy = NamingStrategy.INSTANCE;
	NamedParameterJdbcOperations jdbcTemplate = mock(NamedParameterJdbcOperations.class);
	ConversionCustomizer customizer = mock(ConversionCustomizer.class);

	@Test // DATAJDBC-142
	public void referencedEntitiesGetFound() {

		JdbcMappingContext mappingContext = new JdbcMappingContext(namingStrategy, jdbcTemplate, customizer);

		List<PropertyPath> propertyPaths = mappingContext.referencedEntities(DummyEntity.class, null);

		assertThat(propertyPaths) //
				.extracting(PropertyPath::toDotPath) //
				.containsExactly( //
						"one.two", //
						"one" //
				);
	}

	@Test // DATAJDBC-142
	public void propertyPathDoesNotDependOnNamingStrategy() {

		namingStrategy = mock(NamingStrategy.class);

		JdbcMappingContext mappingContext = new JdbcMappingContext(namingStrategy, jdbcTemplate, customizer);

		List<PropertyPath> propertyPaths = mappingContext.referencedEntities(DummyEntity.class, null);

		assertThat(propertyPaths) //
				.extracting(PropertyPath::toDotPath) //
				.containsExactly( //
						"one.two", //
						"one" //
				);
	}

	static class DummyEntity {

		String simpleProperty;
		LevelOne one;
	}

	static class LevelOne {
		LevelTwo two;
	}

	static class LevelTwo {
		String someValue;
	}
}
