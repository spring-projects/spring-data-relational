/*
 * Copyright 2023-2024 the original author or authors.
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

package org.springframework.data.relational.core.sqlgeneration;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

/**
 * Unit tests for the {@link AliasFactory}.
 * @author Jens Schauder
 */
class AliasFactoryUnitTests {

	RelationalMappingContext context = new RelationalMappingContext();
	AliasFactory aliasFactory = new AliasFactory();

	@Nested
	class SimpleAlias {
		@Test // GH-1446
		void aliasForRoot() {

			String alias = aliasFactory
					.getColumnAlias(context.getAggregatePath(context.getRequiredPersistentEntity(DummyEntity.class)));

			assertThat(alias).isEqualTo("c_dummy_entity_1");
		}

		@Test // GH-1446
		void aliasSimpleProperty() {

			String alias = aliasFactory
					.getColumnAlias(context.getAggregatePath(context.getPersistentPropertyPath("name", DummyEntity.class)));

			assertThat(alias).isEqualTo("c_name_1");
		}

		@Test // GH-1446
		void nameGetsSanitized() {

			String alias = aliasFactory.getColumnAlias(
					context.getAggregatePath( context.getPersistentPropertyPath("evil", DummyEntity.class)));

			assertThat(alias).isEqualTo("c_ameannamecontains3illegal_characters_1");
		}

		@Test // GH-1446
		void aliasIsStable() {

			String alias1 = aliasFactory.getColumnAlias(
					context.getAggregatePath( context.getRequiredPersistentEntity(DummyEntity.class)));
			String alias2 = aliasFactory.getColumnAlias(
					context.getAggregatePath( context.getRequiredPersistentEntity(DummyEntity.class)));

			assertThat(alias1).isEqualTo(alias2);
		}
	}

	@Nested
	class RnAlias {

		@Test // GH-1446
		void aliasIsStable() {

			String alias1 = aliasFactory.getRowNumberAlias(
					context.getAggregatePath(context.getRequiredPersistentEntity(DummyEntity.class)));
			String alias2 = aliasFactory.getRowNumberAlias(
					context.getAggregatePath( context.getRequiredPersistentEntity(DummyEntity.class)));

			assertThat(alias1).isEqualTo(alias2);
		}

		@Test // GH-1446
		void aliasProjectsOnTableReferencingPath() {

			String alias1 = aliasFactory.getRowNumberAlias(
					context.getAggregatePath(context.getRequiredPersistentEntity(DummyEntity.class)));

			String alias2 = aliasFactory.getRowNumberAlias(
					context.getAggregatePath(context.getPersistentPropertyPath("evil", DummyEntity.class)));

			assertThat(alias1).isEqualTo(alias2);
		}

		@Test // GH-1446
		void rnAliasIsIndependentOfTableAlias() {

			String alias1 = aliasFactory.getRowNumberAlias(
					context.getAggregatePath(context.getRequiredPersistentEntity(DummyEntity.class)));
			String alias2 = aliasFactory.getColumnAlias(
					context.getAggregatePath(context.getRequiredPersistentEntity(DummyEntity.class)));

			assertThat(alias1).isNotEqualTo(alias2);
		}

	}

	@Nested
	class BackReferenceAlias {
		@Test // GH-1446
		void testBackReferenceAlias() {

			String alias = aliasFactory.getBackReferenceAlias(
					context.getAggregatePath(context.getPersistentPropertyPath("dummy", Reference.class)));

			assertThat(alias).isEqualTo("br_dummy_entity_1");
		}
	}

	@Nested
	class KeyAlias {
		@Test // GH-1446
		void testKeyAlias() {

			String alias = aliasFactory.getKeyAlias(
					context.getAggregatePath(context.getPersistentPropertyPath("dummy", Reference.class)));

			assertThat(alias).isEqualTo("key_dummy_entity_1");
		}
	}

	@Nested
	class TableAlias {
		@Test // GH-1448
		void tableAliasIsDifferentForDifferentPathsToSameEntity() {

			String alias = aliasFactory.getTableAlias(
					context.getAggregatePath(context.getPersistentPropertyPath("dummy", Reference.class)));

			String alias2 = aliasFactory.getTableAlias(
					context.getAggregatePath(context.getPersistentPropertyPath("dummy2", Reference.class)));

			assertThat(alias).isNotEqualTo(alias2);
		}
	}

	static class DummyEntity {
		String name;

		@Column("a mean name <-- contains > 3 illegal_characters.") String evil;
	}

	static class Reference {
		DummyEntity dummy;
		DummyEntity dummy2;
	}
}
