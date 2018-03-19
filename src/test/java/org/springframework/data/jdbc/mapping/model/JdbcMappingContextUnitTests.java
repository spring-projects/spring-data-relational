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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.mapping.PropertyPath;

/**
 * @author Jens Schauder
 */
public class JdbcMappingContextUnitTests {

	JdbcMappingContext context = new JdbcMappingContext();

	// DATAJDBC-188
	@Test
	public void simpleEntityDoesntReferenceOtherEntities() {

		List<PropertyPath> paths = context.referencedEntities(SimpleEntity.class, null);

		assertThat(paths).isEmpty();
	}

	// DATAJDBC-188
	@Test
	public void cascadingReferencesGetFound() {

		List<PropertyPath> paths = context.referencedEntities(CascadingEntity.class, null);

		assertThat(paths).extracting(PropertyPath::toDotPath) //
				.containsExactly( //
						"reference.reference", //
						"reference" //
		);
	}

	// DATAJDBC-188
	@Test
	public void setReferencesGetFound() {

		List<PropertyPath> paths = context.referencedEntities(EntityWithSet.class, null);

		assertThat(paths).extracting(PropertyPath::toDotPath) //
				.containsExactly( //
						"set.reference", //
						"set" //
		);
	}

	// DATAJDBC-188
	@Test
	public void mapReferencesGetFound() {

		List<PropertyPath> paths = context.referencedEntities(EntityWithMap.class, null);

		assertThat(paths).extracting(PropertyPath::toDotPath) //
				.containsExactly( //
						"map.reference", //
						"map" //
		);
	}

	private static class SimpleEntity {
		String name;
	}

	private static class CascadingEntity {
		MiddleEntity reference;
	}

	private static class MiddleEntity {
		SimpleEntity reference;
	}

	private static class EntityWithMap {
		Map<String, MiddleEntity> map;
	}

	private static class EntityWithSet {
		Set<MiddleEntity> set;
	}
}
