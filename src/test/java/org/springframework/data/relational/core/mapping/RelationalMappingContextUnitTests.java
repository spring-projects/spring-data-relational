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
package org.springframework.data.relational.core.mapping;

import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.PropertyPath;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RelationalMappingContext}.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Toshiaki Maki
 */
public class RelationalMappingContextUnitTests {

	@Test // DATAJDBC-142
	public void referencedEntitiesGetFound() {

		RelationalMappingContext mappingContext = new RelationalMappingContext();

		List<PropertyPath> propertyPaths = mappingContext.referencedEntities(DummyEntity.class, null);

		assertThat(propertyPaths) //
				.extracting(PropertyPath::toDotPath) //
				.containsExactly("one.two", "one");
	}

	@Test // DATAJDBC-142
	public void propertyPathDoesNotDependOnNamingStrategy() {

		RelationalMappingContext mappingContext = new RelationalMappingContext();

		List<PropertyPath> propertyPaths = mappingContext.referencedEntities(DummyEntity.class, null);

		assertThat(propertyPaths) //
				.extracting(PropertyPath::toDotPath) //
				.containsExactly("one.two", "one");
	}

    @Test // DATAJDBC-229
    public void uuidPropertyIsNotEntity() {
        RelationalMappingContext mappingContext = new RelationalMappingContext();
        RelationalPersistentEntity<?> entity = mappingContext.getPersistentEntity(EntityWithUuid.class);
        RelationalPersistentProperty uuidProperty = entity.getRequiredPersistentProperty("uuid");
        assertThat(uuidProperty.isEntity()).isFalse();
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

	static class EntityWithUuid {
		@Id
		UUID uuid;
	}
}
