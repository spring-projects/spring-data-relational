/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.relational.core.conversion;

import static org.assertj.core.api.Assertions.*;

import java.util.AbstractMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.mapping.MappedCollection;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.Identifier;

/**
 * Unit tests for {@link IdentifierContext}.
 *
 * @author Jens Schauder
 */
public class IdentifierContextUnitTests {

	private MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty> context = new RelationalMappingContext();

	@Test
	public void withId() {

		IdentifierContext withId = IdentifierContext.of(__ -> 23);

		Identifier identifier = withId.toIdentifier(context, path("simple"));
		assertThat(identifier.toMap()).containsExactly(new AbstractMap.SimpleEntry<>("dummy_entity", 23));
	}

	@Test
	public void pathAndIdAreIncludedForPath() {

		IdentifierContext identifierContext = IdentifierContext.of(__ -> 23) // IdentifierContext for root
				.withQualifier(path("simple.map"), "some map key"); // IdentifierContext for map element

		Identifier identifier = identifierContext.toIdentifier(context, path("simple.map"));
		assertThat(identifier.toMap()).containsOnly(new AbstractMap.SimpleEntry<>("dummy_entity", 23),
				new AbstractMap.SimpleEntry<>("element_key", "some map key"));
	}

	@Test
	public void onlyOwnIdIsIncludedForLongerPath() {

		IdentifierContext identifierContext = IdentifierContext.of(__ -> 23) // IdentifierContext for root
				.withQualifier(path("simple.other"), "some map key", (__) -> 24); // IdentifierContext for map element

		Identifier identifier = identifierContext.toIdentifier(context, path("simple.other.simple"));
		assertThat(identifier.toMap()).containsOnly(new AbstractMap.SimpleEntry<>("element_with_id", 24));
	}

	@Test
	public void addMultiplePaths() {

		IdentifierContext identifierContext = IdentifierContext.of(__ -> 23)
				.withQualifier(path("simple.map"), "some map key")
				.withQualifier(path("simple.map.other"), "some other map key");

		Identifier identifier = identifierContext.toIdentifier(context, path("simple.other"));
		assertThat(identifier.toMap()).containsOnly(new AbstractMap.SimpleEntry<>("dummy_entity", 23),
				new AbstractMap.SimpleEntry<>("element_key", "some map key"),
				new AbstractMap.SimpleEntry<>("other_key", "some other map key"));
	}

	private PersistentPropertyPath<RelationalPersistentProperty> path(String s) {
		return context.getPersistentPropertyPath(s, DummyEntity.class);
	}

	@SuppressWarnings("unused")
	private static class DummyEntity {
		@Id Long id;

		Element simple;

		ElementWithId withId;
	}

	@SuppressWarnings("unused")
	private static class ElementWithId {
		@Id Long id;
		Map<String, Element> map;
		@MappedCollection(keyColumn = "other_key") Map<String, ElementWithId> other;
		Element simple;

	}

	@SuppressWarnings("unused")
	private static class Element {
		Map<String, Element> map;
		@MappedCollection(keyColumn = "other_key") Map<String, ElementWithId> other;
		Element simple;
	}
}
