/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.jdbc.core;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.jdbc.core.PropertyPathUtils.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * @author Jens Schauder
 */
public class ParentKeysUnitTests {

	JdbcMappingContext context = new JdbcMappingContext();

	@Test // DATAJDBC-326
	public void parametersWithStringKeysUseTheValuesType() {

		HashMap<String, Object> parameters = new HashMap<>();
		parameters.put("one", "eins");
		parameters.put("two", 2L);

		ParentKeys parentKeys = ParentKeys.fromNamedValues(parameters);

		assertThat(parentKeys.getParameters()).containsExactlyInAnyOrder( //
				new ParentKeys.ParentKey("one", "eins", String.class), //
				new ParentKeys.ParentKey("two", 2L, Long.class) //
		);
	}

	@Test // DATAJDBC-326
	public void parametersWithStringKeysUseObjectAsTypeForNull() {

		HashMap<String, Object> parameters = new HashMap<>();
		parameters.put("one", null);

		ParentKeys parentKeys = ParentKeys.fromNamedValues(parameters);

		assertThat(parentKeys.getParameters()).containsExactly( //
				new ParentKeys.ParentKey("one", null, Object.class) //
		);
	}

	@Test // DATAJDBC-326
	public void parametersWithPropertyKeysUseTheParentPropertyJdbcType() {

		ParentKeys parentKeys = ParentKeys.forBackReferences(getPath("child"), "eins");

		assertThat(parentKeys.getParameters()).containsExactly( //
				new ParentKeys.ParentKey("dummy_entity", "eins", UUID.class) //
		);
	}

	@Test // DATAJDBC-326
	public void qualifiersForMaps() {

		PersistentPropertyPath<RelationalPersistentProperty> path = getPath("children");

		ParentKeys parentKeys = ParentKeys //
				.forBackReferences(path, "parent-eins") //
				.withQualifier(path, "map-key-eins");

		assertThat(parentKeys.getParameters()).containsExactlyInAnyOrder( //
				new ParentKeys.ParentKey("dummy_entity", "parent-eins", UUID.class), //
				new ParentKeys.ParentKey("dummy_entity_key", "map-key-eins", String.class) //
		);
	}

	@Test // DATAJDBC-326
	public void qualifiersForLists() {

		PersistentPropertyPath<RelationalPersistentProperty> path = getPath("moreChildren");

		ParentKeys parentKeys = ParentKeys //
				.forBackReferences(path, "parent-eins") //
				.withQualifier(path, "list-index-eins");

		assertThat(parentKeys.getParameters()).containsExactlyInAnyOrder( //
				new ParentKeys.ParentKey("dummy_entity", "parent-eins", UUID.class), //
				new ParentKeys.ParentKey("dummy_entity_key", "list-index-eins", Integer.class) //
		);
	}

	@Test // DATAJDBC-326
	public void backreferenceAcrossEmbeddable() {

		ParentKeys parentKeys = ParentKeys.forBackReferences(getPath("embeddable.child"), "parent-eins");

		assertThat(parentKeys.getParameters()).containsExactly( //
				new ParentKeys.ParentKey("embeddable", "parent-eins", UUID.class) //
		);
	}

	@NotNull
	private PersistentPropertyPath<RelationalPersistentProperty> getPath(String dotPath) {
		return toPath(dotPath, DummyEntity.class, context);
	}

	@SuppressWarnings("unused")
	static class DummyEntity {

		@Id UUID id;
		String one;
		Long two;
		Child child;

		Map<String, Child> children;

		List<Child> moreChildren;

		Embeddable embeddable;
	}

	@SuppressWarnings("unused")
	static class Embeddable {
		Child child;
	}

	static class Child {}
}
