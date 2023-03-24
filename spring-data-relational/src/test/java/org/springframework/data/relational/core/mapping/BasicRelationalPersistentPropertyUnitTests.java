/*
 * Copyright 2017-2023 the original author or authors.
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
import lombok.Data;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.function.BiConsumer;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.Embedded.OnEmpty;

/**
 * Unit tests for the {@link BasicRelationalPersistentProperty}.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Florian Lüdiger
 * @author Bastian Wilhelm
 * @author Kurt Niemi
 */
public class BasicRelationalPersistentPropertyUnitTests {

	RelationalMappingContext context = new RelationalMappingContext();
	RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);

	@Test // DATAJDBC-106
	public void detectsAnnotatedColumnName() {

		assertThat(entity.getRequiredPersistentProperty("name").getColumnName()).isEqualTo(quoted("dummy_name"));
		assertThat(entity.getRequiredPersistentProperty("localDateTime").getColumnName())
				.isEqualTo(quoted("dummy_last_updated_at"));
	}

	@Test // DATAJDBC-218
	public void detectsAnnotatedColumnAndKeyName() {

		RelationalPersistentProperty listProperty = entity.getRequiredPersistentProperty("someList");

		PersistentPropertyPath<RelationalPersistentProperty> path = context
				.findPersistentPropertyPaths(DummyEntity.class, p -> p.getName().equals("someList")).getFirst()
				.orElseThrow(() -> new AssertionFailedError("Couldn't find path for 'someList'"));

		assertThat(listProperty.getReverseColumnName(new PersistentPropertyPathExtension(context, path)))
				.isEqualTo(quoted("dummy_column_name"));
		assertThat(listProperty.getKeyColumn()).isEqualTo(quoted("dummy_key_column_name"));
	}

	@Test // GH-1325
	void testRelationalPersistentEntitySpelExpressions() {

		assertThat(entity.getRequiredPersistentProperty("spelExpression1").getColumnName()).isEqualTo(quoted("THE_FORCE_IS_WITH_YOU"));
		assertThat(entity.getRequiredPersistentProperty("littleBobbyTables").getColumnName())
				.isEqualTo(quoted("DROPALLTABLES"));

		// Test that sanitizer does affect non-spel expressions
		assertThat(entity.getRequiredPersistentProperty(
				"poorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot").getColumnName())
				.isEqualTo(quoted("--; DROP ALL TABLES;--"));
	}

	@Test // DATAJDBC-111
	public void detectsEmbeddedEntity() {

		final RelationalPersistentEntity<?> requiredPersistentEntity = context
				.getRequiredPersistentEntity(DummyEntity.class);

		SoftAssertions softly = new SoftAssertions();

		BiConsumer<String, String> checkEmbedded = (name, prefix) -> {

			RelationalPersistentProperty property = requiredPersistentEntity.getRequiredPersistentProperty(name);

			softly.assertThat(property.isEmbedded()) //
					.describedAs(name + " is embedded") //
					.isEqualTo(prefix != null);

			softly.assertThat(property.getEmbeddedPrefix()) //
					.describedAs(name + " prefix") //
					.isEqualTo(prefix);
		};

		checkEmbedded.accept("someList", null);
		checkEmbedded.accept("id", null);
		checkEmbedded.accept("embeddableEntity", "");
		checkEmbedded.accept("prefixedEmbeddableEntity", "prefix");

		softly.assertAll();
	}

	@Test // DATAJDBC-259
	public void classificationOfCollectionLikeProperties() {

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

	@Data
	@SuppressWarnings("unused")
	private static class DummyEntity {

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
		@Column(value="#{T(org.springframework.data.relational.core.mapping." +
				"BasicRelationalPersistentPropertyUnitTests$DummyEntity" +
				").spelExpression1Value}")
		private String spelExpression1;

		@Column(value="#{T(org.springframework.data.relational.core.mapping." +
				"BasicRelationalPersistentPropertyUnitTests$DummyEntity" +
				").littleBobbyTablesValue}")
		private String littleBobbyTables;

		@Column(value="--; DROP ALL TABLES;--")
		private String poorDeveloperProgrammaticallyAskingToShootThemselvesInTheFoot;

		// DATAJDBC-111
		private @Embedded(onEmpty = OnEmpty.USE_NULL) EmbeddableEntity embeddableEntity;

		// DATAJDBC-111
		private @Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "prefix") EmbeddableEntity prefixedEmbeddableEntity;

		@Column("dummy_last_updated_at")
		public LocalDateTime getLocalDateTime() {
			return localDateTime;
		}

		public void setListSetter(Integer integer) {

		}

		public List<Date> getListGetter() {
			return null;
		}
	}

	@SuppressWarnings("unused")
	private enum SomeEnum {
		ALPHA
	}

	// DATAJDBC-111
	@Data
	private static class EmbeddableEntity {
		private final String embeddedTest;
	}

	@SuppressWarnings("unused")
	private static class OtherEntity {}
}
