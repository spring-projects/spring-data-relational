/*
 * Copyright 2017-2025 the original author or authors.
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
package org.springframework.data.relational.core.mapping;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.relational.core.sql.SqlIdentifier.*;

import junit.framework.AssertionFailedError;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.Embedded.OnEmpty;
import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * Unit tests for the {@link BasicRelationalPersistentProperty}.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Florian Lüdiger
 * @author Bastian Wilhelm
 * @author Kurt Niemi
 * @author Mark Paluch
 */
class BasicRelationalPersistentPropertyUnitTests {

	private final RelationalMappingContext context = new RelationalMappingContext();
	private final RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);

	@Test // DATAJDBC-106
	void detectsAnnotatedColumnName() {

		assertThat(entity.getRequiredPersistentProperty("name").getColumnName()).isEqualTo(quoted("dummy_name"));
		assertThat(entity.getRequiredPersistentProperty("localDateTime").getColumnName())
				.isEqualTo(quoted("dummy_last_updated_at"));
	}

	@Test // DATAJDBC-218
	void detectsAnnotatedColumnAndKeyName() {

		RelationalPersistentProperty listProperty = entity.getRequiredPersistentProperty("someList");

		PersistentPropertyPath<RelationalPersistentProperty> path = context
				.findPersistentPropertyPaths(DummyEntity.class, p -> p.getName().equals("someList")).getFirst()
				.orElseThrow(() -> new AssertionFailedError("Couldn't find path for 'someList'"));

		assertThat(listProperty.getReverseColumnName(path.getLeafProperty().getOwner()))
				.isEqualTo(quoted("dummy_column_name"));
		assertThat(listProperty.getKeyColumn()).isEqualTo(quoted("dummy_key_column_name"));
	}

	@Test // GH-1325
	void testRelationalPersistentEntitySpelExpressions() {

		assertThat(entity.getRequiredPersistentProperty("spelExpression1").getColumnName())
				.isEqualTo(quoted("THE_FORCE_IS_WITH_YOU"));
		assertThat(entity.getRequiredPersistentProperty("littleBobbyTables").getColumnName())
				.isEqualTo(quoted("DROPALLTABLES"));

		// Test that sanitizer does affect non-spel expressions
		assertThat(entity.getRequiredPersistentProperty("poorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot")
				.getColumnName()).isEqualTo(quoted("--; DROP ALL TABLES;--"));
	}

	@Test // GH-1325
	void shouldEvaluateMappedCollectionExpressions() {

		RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(WithMappedCollection.class);
		RelationalPersistentProperty property = entity.getRequiredPersistentProperty("someList");

		assertThat(property.getKeyColumn()).isEqualTo(quoted("key_col"));
	}

	@Test // DATAJDBC-111
	void detectsEmbeddedEntity() {

		final RelationalPersistentEntity<?> requiredPersistentEntity = context
				.getRequiredPersistentEntity(DummyEntity.class);

		SoftAssertions softly = new SoftAssertions();

		BiConsumer<String, String> checkEmbedded = (name, prefix) -> {

			RelationalPersistentProperty property = requiredPersistentEntity.getRequiredPersistentProperty(name);

			if (!prefix.isEmpty()) {
				softly.assertThat(property.isEmbedded()) //
						.describedAs(name + " is embedded") //
						.isTrue();
			}

			softly.assertThat(property.getEmbeddedPrefix()) //
					.describedAs(name + " prefix") //
					.isEqualTo(prefix);
		};

		checkEmbedded.accept("someList", "");
		checkEmbedded.accept("id", "");
		checkEmbedded.accept("embeddableEntity", "");
		checkEmbedded.accept("prefixedEmbeddableEntity", "prefix");

		softly.assertAll();
	}

	@Test // DATAJDBC-259
	void classificationOfCollectionLikeProperties() {

		RelationalPersistentProperty listOfString = entity.getRequiredPersistentProperty("listOfString");
		RelationalPersistentProperty arrayOfString = entity.getRequiredPersistentProperty("arrayOfString");
		RelationalPersistentProperty listOfEntity = entity.getRequiredPersistentProperty("listOfEntity");
		RelationalPersistentProperty arrayOfEntity = entity.getRequiredPersistentProperty("arrayOfEntity");

		SoftAssertions softly = new SoftAssertions();

		softly.assertThat(listOfString.isCollectionLike() && !listOfString.isEntity())
				.describedAs("listOfString is a Collection of a simple type.").isEqualTo(true);
		softly.assertThat(arrayOfString.isCollectionLike() && !arrayOfString.isEntity())
				.describedAs("arrayOfString is a Collection of a simple type.").isTrue();
		softly.assertThat(listOfEntity.isCollectionLike() && !listOfEntity.isEntity())
				.describedAs("listOfEntity  is a Collection of a simple type.").isFalse();
		softly.assertThat(arrayOfEntity.isCollectionLike() && !arrayOfEntity.isEntity())
				.describedAs("arrayOfEntity is a Collection of a simple type.").isFalse();

		BiConsumer<RelationalPersistentProperty, String> checkEitherOr = (p, s) -> softly
				.assertThat(p.isCollectionLike() && !p.isEntity()).describedAs(s + " contains either simple types or entities")
				.isNotEqualTo(p.isCollectionLike() && p.isEntity());

		checkEitherOr.accept(listOfString, "listOfString");
		checkEitherOr.accept(arrayOfString, "arrayOfString");
		checkEitherOr.accept(listOfEntity, "listOfEntity");
		checkEitherOr.accept(arrayOfEntity, "arrayOfEntity");

		softly.assertAll();
	}

	@Test // GH-1923
	void entityWithNoSequence() {

		RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);

		assertThat(entity.getRequiredIdProperty().getSequence()).isNull();
	}

	@Test // GH-1923
	void determineSequenceName() {

		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(EntityWithSequence.class);

		assertThat(persistentEntity.getRequiredIdProperty().getSequence()).isEqualTo(SqlIdentifier.quoted("my_seq"));
	}

	@Test // GH-1923
	void determineSequenceNameFromValue() {

		RelationalPersistentEntity<?> persistentEntity = context
				.getRequiredPersistentEntity(EntityWithSequenceValueAlias.class);

		assertThat(persistentEntity.getRequiredIdProperty().getSequence()).isEqualTo(SqlIdentifier.quoted("my_seq"));
	}

	@Test // GH-1923
	void determineSequenceNameWithSchemaSpecified() {

		RelationalPersistentEntity<?> persistentEntity = context
				.getRequiredPersistentEntity(EntityWithSequenceAndSchema.class);

		assertThat(persistentEntity.getRequiredIdProperty().getSequence())
				.isEqualTo(SqlIdentifier.from(SqlIdentifier.quoted("public"), SqlIdentifier.quoted("my_seq")));
	}

	@SuppressWarnings("unused")
	static class DummyEntity {

		@Id private final Long id;
		private final SomeEnum someEnum;
		private final LocalDateTime localDateTime;
		private final ZonedDateTime zonedDateTime;

		// DATAJDBC-259
		private final List<String> listOfString;
		private final String[] arrayOfString;
		private final List<OtherEntity> listOfEntity;
		private final OtherEntity[] arrayOfEntity;

		@MappedCollection(idColumn = "dummy_column_name",
				keyColumn = "dummy_key_column_name") private List<Integer> someList;

		// DATACMNS-106
		private @Column("dummy_name") String name;

		public static String spelExpression1Value = "THE_FORCE_IS_WITH_YOU";

		public static String littleBobbyTablesValue = "--; DROP ALL TABLES;--";
		@Column(value = "#{T(org.springframework.data.relational.core.mapping."
				+ "BasicRelationalPersistentPropertyUnitTests$DummyEntity"
				+ ").spelExpression1Value}") private String spelExpression1;

		@Column(value = "#{T(org.springframework.data.relational.core.mapping."
				+ "BasicRelationalPersistentPropertyUnitTests$DummyEntity"
				+ ").littleBobbyTablesValue}") private String littleBobbyTables;

		@Column(
				value = "--; DROP ALL TABLES;--") private String poorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot;

		// DATAJDBC-111
		private @Embedded(onEmpty = OnEmpty.USE_NULL) EmbeddableEntity embeddableEntity;

		// DATAJDBC-111
		private @Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "prefix") EmbeddableEntity prefixedEmbeddableEntity;

		public DummyEntity(Long id, SomeEnum someEnum, LocalDateTime localDateTime, ZonedDateTime zonedDateTime,
				List<String> listOfString, String[] arrayOfString, List<OtherEntity> listOfEntity,
				OtherEntity[] arrayOfEntity) {
			this.id = id;
			this.someEnum = someEnum;
			this.localDateTime = localDateTime;
			this.zonedDateTime = zonedDateTime;
			this.listOfString = listOfString;
			this.arrayOfString = arrayOfString;
			this.listOfEntity = listOfEntity;
			this.arrayOfEntity = arrayOfEntity;
		}

		@Column("dummy_last_updated_at")
		public LocalDateTime getLocalDateTime() {
			return localDateTime;
		}

		public void setListSetter(Integer integer) {

		}

		public List<Date> getListGetter() {
			return null;
		}

		Long getId() {
			return this.id;
		}

		SomeEnum getSomeEnum() {
			return this.someEnum;
		}

		ZonedDateTime getZonedDateTime() {
			return this.zonedDateTime;
		}

		List<String> getListOfString() {
			return this.listOfString;
		}

		String[] getArrayOfString() {
			return this.arrayOfString;
		}

		List<OtherEntity> getListOfEntity() {
			return this.listOfEntity;
		}

		OtherEntity[] getArrayOfEntity() {
			return this.arrayOfEntity;
		}

		List<Integer> getSomeList() {
			return this.someList;
		}

		String getName() {
			return this.name;
		}

		String getSpelExpression1() {
			return this.spelExpression1;
		}

		String getLittleBobbyTables() {
			return this.littleBobbyTables;
		}

		String getPoorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot() {
			return this.poorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot;
		}

		EmbeddableEntity getEmbeddableEntity() {
			return this.embeddableEntity;
		}

		EmbeddableEntity getPrefixedEmbeddableEntity() {
			return this.prefixedEmbeddableEntity;
		}

		public void setSomeList(List<Integer> someList) {
			this.someList = someList;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setSpelExpression1(String spelExpression1) {
			this.spelExpression1 = spelExpression1;
		}

		public void setLittleBobbyTables(String littleBobbyTables) {
			this.littleBobbyTables = littleBobbyTables;
		}

		public void setPoorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot(
				String poorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot) {
			this.poorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot = poorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot;
		}

		public void setEmbeddableEntity(EmbeddableEntity embeddableEntity) {
			this.embeddableEntity = embeddableEntity;
		}

		public void setPrefixedEmbeddableEntity(EmbeddableEntity prefixedEmbeddableEntity) {
			this.prefixedEmbeddableEntity = prefixedEmbeddableEntity;
		}

		public boolean equals(final Object o) {
			if (o == this)
				return true;
			if (!(o instanceof DummyEntity other))
				return false;
			if (!other.canEqual(this))
				return false;
			final Object this$id = this.getId();
			final Object other$id = other.getId();
			if (!Objects.equals(this$id, other$id))
				return false;
			final Object this$someEnum = this.getSomeEnum();
			final Object other$someEnum = other.getSomeEnum();
			if (!Objects.equals(this$someEnum, other$someEnum))
				return false;
			final Object this$localDateTime = this.getLocalDateTime();
			final Object other$localDateTime = other.getLocalDateTime();
			if (!Objects.equals(this$localDateTime, other$localDateTime))
				return false;
			final Object this$zonedDateTime = this.getZonedDateTime();
			final Object other$zonedDateTime = other.getZonedDateTime();
			if (!Objects.equals(this$zonedDateTime, other$zonedDateTime))
				return false;
			final Object this$listOfString = this.getListOfString();
			final Object other$listOfString = other.getListOfString();
			if (!Objects.equals(this$listOfString, other$listOfString))
				return false;
			if (!java.util.Arrays.deepEquals(this.getArrayOfString(), other.getArrayOfString()))
				return false;
			final Object this$listOfEntity = this.getListOfEntity();
			final Object other$listOfEntity = other.getListOfEntity();
			if (!Objects.equals(this$listOfEntity, other$listOfEntity))
				return false;
			if (!java.util.Arrays.deepEquals(this.getArrayOfEntity(), other.getArrayOfEntity()))
				return false;
			final Object this$someList = this.getSomeList();
			final Object other$someList = other.getSomeList();
			if (!Objects.equals(this$someList, other$someList))
				return false;
			final Object this$name = this.getName();
			final Object other$name = other.getName();
			if (!Objects.equals(this$name, other$name))
				return false;
			final Object this$spelExpression1 = this.getSpelExpression1();
			final Object other$spelExpression1 = other.getSpelExpression1();
			if (!Objects.equals(this$spelExpression1, other$spelExpression1))
				return false;
			final Object this$littleBobbyTables = this.getLittleBobbyTables();
			final Object other$littleBobbyTables = other.getLittleBobbyTables();
			if (!Objects.equals(this$littleBobbyTables, other$littleBobbyTables))
				return false;
			final Object this$poorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot = this
					.getPoorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot();
			final Object other$poorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot = other
					.getPoorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot();
			if (!Objects.equals(this$poorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot, other$poorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot))
				return false;
			final Object this$embeddableEntity = this.getEmbeddableEntity();
			final Object other$embeddableEntity = other.getEmbeddableEntity();
			if (!Objects.equals(this$embeddableEntity, other$embeddableEntity))
				return false;
			final Object this$prefixedEmbeddableEntity = this.getPrefixedEmbeddableEntity();
			final Object other$prefixedEmbeddableEntity = other.getPrefixedEmbeddableEntity();
			return Objects.equals(this$prefixedEmbeddableEntity, other$prefixedEmbeddableEntity);
		}

		boolean canEqual(final Object other) {
			return other instanceof DummyEntity;
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $id = this.getId();
			result = result * PRIME + ($id == null ? 43 : $id.hashCode());
			final Object $someEnum = this.getSomeEnum();
			result = result * PRIME + ($someEnum == null ? 43 : $someEnum.hashCode());
			final Object $localDateTime = this.getLocalDateTime();
			result = result * PRIME + ($localDateTime == null ? 43 : $localDateTime.hashCode());
			final Object $zonedDateTime = this.getZonedDateTime();
			result = result * PRIME + ($zonedDateTime == null ? 43 : $zonedDateTime.hashCode());
			final Object $listOfString = this.getListOfString();
			result = result * PRIME + ($listOfString == null ? 43 : $listOfString.hashCode());
			result = result * PRIME + java.util.Arrays.deepHashCode(this.getArrayOfString());
			final Object $listOfEntity = this.getListOfEntity();
			result = result * PRIME + ($listOfEntity == null ? 43 : $listOfEntity.hashCode());
			result = result * PRIME + java.util.Arrays.deepHashCode(this.getArrayOfEntity());
			final Object $someList = this.getSomeList();
			result = result * PRIME + ($someList == null ? 43 : $someList.hashCode());
			final Object $name = this.getName();
			result = result * PRIME + ($name == null ? 43 : $name.hashCode());
			final Object $spelExpression1 = this.getSpelExpression1();
			result = result * PRIME + ($spelExpression1 == null ? 43 : $spelExpression1.hashCode());
			final Object $littleBobbyTables = this.getLittleBobbyTables();
			result = result * PRIME + ($littleBobbyTables == null ? 43 : $littleBobbyTables.hashCode());
			final Object $poorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot = this
					.getPoorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot();
			result = result * PRIME + ($poorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot == null ? 43
					: $poorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot.hashCode());
			final Object $embeddableEntity = this.getEmbeddableEntity();
			result = result * PRIME + ($embeddableEntity == null ? 43 : $embeddableEntity.hashCode());
			final Object $prefixedEmbeddableEntity = this.getPrefixedEmbeddableEntity();
			result = result * PRIME + ($prefixedEmbeddableEntity == null ? 43 : $prefixedEmbeddableEntity.hashCode());
			return result;
		}

		public String toString() {
			return "BasicRelationalPersistentPropertyUnitTests.DummyEntity(id=" + this.getId() + ", someEnum="
					+ this.getSomeEnum() + ", localDateTime=" + this.getLocalDateTime() + ", zonedDateTime="
					+ this.getZonedDateTime() + ", listOfString=" + this.getListOfString() + ", arrayOfString="
					+ java.util.Arrays.deepToString(this.getArrayOfString()) + ", listOfEntity=" + this.getListOfEntity()
					+ ", arrayOfEntity=" + java.util.Arrays.deepToString(this.getArrayOfEntity()) + ", someList="
					+ this.getSomeList() + ", name=" + this.getName() + ", spelExpression1=" + this.getSpelExpression1()
					+ ", littleBobbyTables=" + this.getLittleBobbyTables()
					+ ", poorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot="
					+ this.getPoorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot() + ", embeddableEntity="
					+ this.getEmbeddableEntity() + ", prefixedEmbeddableEntity=" + this.getPrefixedEmbeddableEntity() + ")";
		}
	}

	private static class WithMappedCollection {

		@MappedCollection(idColumn = "#{'id_col'}", keyColumn = "#{'key_col'}") private List<Integer> someList;
	}

	@SuppressWarnings("unused")
	private enum SomeEnum {
		ALPHA
	}

	// DATAJDBC-111
		private record EmbeddableEntity(String embeddedTest) {


		public boolean equals(final Object o) {
				if (o == this)
					return true;
				if (!(o instanceof EmbeddableEntity other))
					return false;
				if (!other.canEqual(this))
					return false;
				final Object this$embeddedTest = this.embeddedTest();
				final Object other$embeddedTest = other.embeddedTest();
				return Objects.equals(this$embeddedTest, other$embeddedTest);
			}

			boolean canEqual(final Object other) {
				return other instanceof EmbeddableEntity;
			}

			public int hashCode() {
				final int PRIME = 59;
				int result = 1;
				final Object $embeddedTest = this.embeddedTest();
				result = result * PRIME + ($embeddedTest == null ? 43 : $embeddedTest.hashCode());
				return result;
			}

			public String toString() {
				return "BasicRelationalPersistentPropertyUnitTests.EmbeddableEntity(embeddedTest=" + this.embeddedTest() + ")";
			}
		}

	@SuppressWarnings("unused")
	private static class OtherEntity {}

	@Table("entity_with_sequence")
	static class EntityWithSequence {
		@Id
		@Sequence(sequence = "my_seq") Long id;
	}

	@Table("entity_with_sequence_value_alias")
	static class EntityWithSequenceValueAlias {
		@Id
		@Column("myId")
		@Sequence(value = "my_seq") Long id;
	}

	@Table("entity_with_sequence_and_schema")
	static class EntityWithSequenceAndSchema {
		@Id
		@Column("myId")
		@Sequence(sequence = "my_seq", schema = "public") Long id;
	}
}
