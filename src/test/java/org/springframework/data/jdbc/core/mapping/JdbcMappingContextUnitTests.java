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
package org.springframework.data.jdbc.core.mapping;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.Test;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.mapping.PropertyPath;

/**
 * Unit tests for {@link JdbcMappingContext}.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 */
public class JdbcMappingContextUnitTests {

	@Test // DATAJDBC-142
	public void referencedEntitiesGetFound() {

		JdbcMappingContext mappingContext = new JdbcMappingContext();

		List<PropertyPath> propertyPaths = mappingContext.referencedEntities(DummyEntity.class, null);

		assertThat(propertyPaths) //
				.extracting(PropertyPath::toDotPath) //
				.containsExactly("one.two", "one");
	}

	@Test // DATAJDBC-142
	public void propertyPathDoesNotDependOnNamingStrategy() {

		JdbcMappingContext mappingContext = new JdbcMappingContext();

		List<PropertyPath> propertyPaths = mappingContext.referencedEntities(DummyEntity.class, null);

		assertThat(propertyPaths) //
				.extracting(PropertyPath::toDotPath) //
				.containsExactly("one.two", "one");
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
