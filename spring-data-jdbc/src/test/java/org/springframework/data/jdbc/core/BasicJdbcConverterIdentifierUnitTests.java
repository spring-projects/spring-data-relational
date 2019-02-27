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
import org.springframework.data.jdbc.core.convert.BasicJdbcConverter;
import org.springframework.data.relational.domain.Identifier;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * Unit tests for the {@link Identifier} creating methods in the {@link BasicJdbcConverter}.
 *
 * @author Jens Schauder
 */
public class BasicJdbcConverterIdentifierUnitTests {

	JdbcMappingContext context = new JdbcMappingContext();

	@Test // DATAJDBC-326
	public void parametersWithStringKeysUseTheValuesType() {

		HashMap<String, Object> parameters = new HashMap<>();
		parameters.put("one", "eins");
		parameters.put("two", 2L);

		Identifier identifier = BasicJdbcConverter.fromNamedValues(parameters);

		assertThat(identifier.getParameters()).containsExactlyInAnyOrder( //
				new Identifier.SingleIdentifierValue("one", "eins", String.class), //
				new Identifier.SingleIdentifierValue("two", 2L, Long.class) //
		);
	}

	@Test // DATAJDBC-326
	public void parametersWithStringKeysUseObjectAsTypeForNull() {

		HashMap<String, Object> parameters = new HashMap<>();
		parameters.put("one", null);

		Identifier identifier = BasicJdbcConverter.fromNamedValues(parameters);

		assertThat(identifier.getParameters()).containsExactly( //
				new Identifier.SingleIdentifierValue("one", null, Object.class) //
		);
	}

	@Test // DATAJDBC-326
	public void parametersWithPropertyKeysUseTheParentPropertyJdbcType() {

		Identifier identifier = BasicJdbcConverter.forBackReferences(getPath("child"), "eins");

		assertThat(identifier.getParameters()).containsExactly( //
				new Identifier.SingleIdentifierValue("dummy_entity", "eins", UUID.class) //
		);
	}

	@Test // DATAJDBC-326
	public void qualifiersForMaps() {

		PersistentPropertyPath<RelationalPersistentProperty> path = getPath("children");

		Identifier identifier = BasicJdbcConverter //
				.forBackReferences(path, "parent-eins") //
				.withQualifier(path, "map-key-eins");

		assertThat(identifier.getParameters()).containsExactlyInAnyOrder( //
				new Identifier.SingleIdentifierValue("dummy_entity", "parent-eins", UUID.class), //
				new Identifier.SingleIdentifierValue("dummy_entity_key", "map-key-eins", String.class) //
		);
	}

	@Test // DATAJDBC-326
	public void qualifiersForLists() {

		PersistentPropertyPath<RelationalPersistentProperty> path = getPath("moreChildren");

		Identifier identifier = BasicJdbcConverter //
				.forBackReferences(path, "parent-eins") //
				.withQualifier(path, "list-index-eins");

		assertThat(identifier.getParameters()).containsExactlyInAnyOrder( //
				new Identifier.SingleIdentifierValue("dummy_entity", "parent-eins", UUID.class), //
				new Identifier.SingleIdentifierValue("dummy_entity_key", "list-index-eins", Integer.class) //
		);
	}

	@Test // DATAJDBC-326
	public void backreferenceAcrossEmbeddable() {

		Identifier identifier = BasicJdbcConverter.forBackReferences(getPath("embeddable.child"), "parent-eins");

		assertThat(identifier.getParameters()).containsExactly( //
				new Identifier.SingleIdentifierValue("embeddable", "parent-eins", UUID.class) //
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
