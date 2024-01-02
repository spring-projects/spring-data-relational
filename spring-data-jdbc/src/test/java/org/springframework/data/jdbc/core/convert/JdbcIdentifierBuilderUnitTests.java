/*
 * Copyright 2019-2024 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.relational.core.sql.SqlIdentifier.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.PersistentPropertyPathTestUtils;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.mapping.AggregatePath;

/**
 * Unit tests for the {@link JdbcIdentifierBuilder}.
 *
 * @author Jens Schauder
 */
public class JdbcIdentifierBuilderUnitTests {

	JdbcMappingContext context = new JdbcMappingContext();
	JdbcConverter converter = new MappingJdbcConverter(context, (identifier, path) -> {
		throw new UnsupportedOperationException();
	});

	@Test // DATAJDBC-326
	public void parametersWithPropertyKeysUseTheParentPropertyJdbcType() {

		Identifier identifier = JdbcIdentifierBuilder.forBackReferences(converter, getPath("child"), "eins").build();

		assertThat(identifier.getParts()) //
				.extracting("name", "value", "targetType") //
				.containsExactly( //
						tuple(quoted("DUMMY_ENTITY"), "eins", UUID.class) //
				);
	}

	@Test // DATAJDBC-326
	public void qualifiersForMaps() {

		AggregatePath path = getPath("children");

		Identifier identifier = JdbcIdentifierBuilder //
				.forBackReferences(converter, path, "parent-eins") //
				.withQualifier(path, "map-key-eins") //
				.build();

		assertThat(identifier.getParts()) //
				.extracting("name", "value", "targetType") //
				.containsExactlyInAnyOrder( //
						tuple(quoted("DUMMY_ENTITY"), "parent-eins", UUID.class), //
						tuple(quoted("DUMMY_ENTITY_KEY"), "map-key-eins", String.class) //
				);
	}

	@Test // DATAJDBC-326
	public void qualifiersForLists() {

		AggregatePath path = getPath("moreChildren");

		Identifier identifier = JdbcIdentifierBuilder //
				.forBackReferences(converter, path, "parent-eins") //
				.withQualifier(path, "list-index-eins") //
				.build();

		assertThat(identifier.getParts()) //
				.extracting("name", "value", "targetType") //
				.containsExactlyInAnyOrder( //
						tuple(quoted("DUMMY_ENTITY"), "parent-eins", UUID.class), //
						tuple(quoted("DUMMY_ENTITY_KEY"), "list-index-eins", Integer.class) //
				);
	}

	@Test // DATAJDBC-326
	public void backreferenceAcrossEmbeddable() {

		Identifier identifier = JdbcIdentifierBuilder //
				.forBackReferences(converter, getPath("embeddable.child"), "parent-eins") //
				.build();

		assertThat(identifier.getParts()) //
				.extracting("name", "value", "targetType") //
				.containsExactly( //
						tuple(quoted("DUMMY_ENTITY"), "parent-eins", UUID.class) //
				);
	}

	@Test // DATAJDBC-326
	public void backreferenceAcrossNoId() {

		Identifier identifier = JdbcIdentifierBuilder //
				.forBackReferences(converter, getPath("noId.child"), "parent-eins") //
				.build();

		assertThat(identifier.getParts()) //
				.extracting("name", "value", "targetType") //
				.containsExactly( //
						tuple(quoted("DUMMY_ENTITY"), "parent-eins", UUID.class) //
				);
	}

	private AggregatePath getPath(String dotPath) {
		return context.getAggregatePath(PersistentPropertyPathTestUtils.getPath(dotPath, DummyEntity.class, context));
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

		NoId noId;
	}

	@SuppressWarnings("unused")
	static class Embeddable {
		Child child;
	}

	@SuppressWarnings("unused")
	static class NoId {
		Child child;
	}

	static class Child {}
}
