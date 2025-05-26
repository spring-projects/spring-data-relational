/*
 * Copyright 2019-2025 the original author or authors.
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
import java.util.function.Function;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.PersistentPropertyPathTestUtils;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.mapping.PersistentPropertyPathAccessor;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;

/**
 * Unit tests for the {@link JdbcIdentifierBuilder}.
 *
 * @author Jens Schauder
 */
class JdbcIdentifierBuilderUnitTests {

	JdbcMappingContext context = new JdbcMappingContext();
	JdbcConverter converter = new MappingJdbcConverter(context, (identifier, path) -> {
		throw new UnsupportedOperationException();
	});

	@Nested
	class WithSimpleId {
		@Test // DATAJDBC-326
		void parametersWithPropertyKeysUseTheParentPropertyJdbcType() {

			Identifier identifier = JdbcIdentifierBuilder
					.forBackReferences(converter, getPath("child"), getValueProvider("eins", getPath("child"), converter))
					.build();

			assertThat(identifier.getParts()) //
					.extracting("name", "value", "targetType") //
					.containsExactly( //
							tuple(quoted("DUMMY_ENTITY"), "eins", UUID.class) //
					);
		}

		@Test // DATAJDBC-326
		void qualifiersForMaps() {

			AggregatePath path = getPath("children");

			Identifier identifier = JdbcIdentifierBuilder //
					.forBackReferences(converter, path, getValueProvider("parent-eins", path, converter)) //
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
		void qualifiersForLists() {

			AggregatePath path = getPath("moreChildren");

			Identifier identifier = JdbcIdentifierBuilder //
					.forBackReferences(converter, path, getValueProvider("parent-eins", path, converter)) //
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
		void backreferenceAcrossEmbeddable() {

			Identifier identifier = JdbcIdentifierBuilder //
					.forBackReferences(converter, getPath("embeddable.child"),
							getValueProvider("parent-eins", getPath("embeddable.child"), converter)) //
					.build();

			assertThat(identifier.getParts()) //
					.extracting("name", "value", "targetType") //
					.containsExactly( //
							tuple(quoted("DUMMY_ENTITY"), "parent-eins", UUID.class) //
					);
		}

		@Test // DATAJDBC-326
		void backreferenceAcrossNoId() {

			Identifier identifier = JdbcIdentifierBuilder //
					.forBackReferences(converter, getPath("noId.child"),
							getValueProvider("parent-eins", getPath("noId.child"), converter)) //
					.build();

			assertThat(identifier.getParts()) //
					.extracting("name", "value", "targetType") //
					.containsExactly( //
							tuple(quoted("DUMMY_ENTITY"), "parent-eins", UUID.class) //
					);
		}

		private AggregatePath getPath(String dotPath) {
			return JdbcIdentifierBuilderUnitTests.this.getPath(dotPath, DummyEntity.class);
		}
	}

	/**
	 * copied from JdbcAggregateChangeExecutionContext
	 */
	static Function<AggregatePath, Object> getValueProvider(Object idValue, AggregatePath path, JdbcConverter converter) {

		RelationalPersistentEntity<?> entity = converter.getMappingContext()
				.getPersistentEntity(path.getIdDefiningParentPath().getRequiredIdProperty().getType());

		Function<AggregatePath, Object> valueProvider = ap -> {
			if (entity == null) {
				return idValue;
			} else {
				PersistentPropertyPathAccessor<Object> propertyPathAccessor = entity.getPropertyPathAccessor(idValue);
				return propertyPathAccessor.getProperty(ap.getRequiredPersistentPropertyPath());
			}
		};
		return valueProvider;
	}

	@Nested
	class WithCompositeId {

		CompositeId exampleId = new CompositeId("parent-eins", 23);

		@Test // GH-574
		void forBackReferences() {

			AggregatePath path = getPath("children");

			Identifier identifier = JdbcIdentifierBuilder //
					.forBackReferences(converter, path, getValueProvider(exampleId, path, converter)) //
					.build();

			assertThat(identifier.getParts()) //
					.extracting("name", "value", "targetType") //
					.containsExactlyInAnyOrder( //
							tuple(quoted("DUMMY_ENTITY_WITH_COMPOSITE_ID_ONE"), exampleId.one, String.class), //
							tuple(quoted("DUMMY_ENTITY_WITH_COMPOSITE_ID_TWO"), exampleId.two, Integer.class) //
					);
		}

		private AggregatePath getPath(String dotPath) {
			return JdbcIdentifierBuilderUnitTests.this.getPath(dotPath, DummyEntityWithCompositeId.class);
		}
	}

	private AggregatePath getPath(String dotPath, Class<?> entityType) {
		return context.getAggregatePath(PersistentPropertyPathTestUtils.getPath(dotPath, entityType, context));
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

	record CompositeId(String one, Integer two) {
	}

	static class DummyEntityWithCompositeId {

		@Embedded(onEmpty = Embedded.OnEmpty.USE_NULL)
		@Id CompositeId id;
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
