/*
 * Copyright 2020 the original author or authors.
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

import lombok.Data;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * Unit tests for {@link BasicJdbcConverter}.
 *
 * @author Mark Paluch
 */
public class BasicJdbcConverterUnitTests {

	JdbcMappingContext context = new JdbcMappingContext();
	BasicJdbcConverter converter = new BasicJdbcConverter(context, (identifier, path) -> {
		throw new UnsupportedOperationException();
	});

	@Test // DATAJDBC-104
	public void enumGetsStoredAsString() {

		RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);

		entity.doWithProperties((PropertyHandler<RelationalPersistentProperty>) p -> {
			switch (p.getName()) {
				case "someEnum":
					assertThat(converter.getColumnType(p)).isEqualTo(String.class);
					break;
				case "localDateTime":
					assertThat(converter.getColumnType(p)).isEqualTo(Date.class);
					break;
				case "zonedDateTime":
					assertThat(converter.getColumnType(p)).isEqualTo(String.class);
					break;
				default:
			}
		});
	}

	@Test // DATAJDBC-104, DATAJDBC-1384
	public void testTargetTypesForPropertyType() {

		RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);

		SoftAssertions softly = new SoftAssertions();

		checkTargetType(softly, entity, "someEnum", String.class);
		checkTargetType(softly, entity, "localDateTime", Date.class);
		checkTargetType(softly, entity, "zonedDateTime", String.class);
		checkTargetType(softly, entity, "uuid", UUID.class);

		softly.assertAll();
	}

	@Test // DATAJDBC-259
	public void classificationOfCollectionLikeProperties() {

		RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);

		RelationalPersistentProperty listOfString = entity.getRequiredPersistentProperty("listOfString");
		RelationalPersistentProperty arrayOfString = entity.getRequiredPersistentProperty("arrayOfString");

		SoftAssertions softly = new SoftAssertions();

		softly.assertThat(converter.getColumnType(arrayOfString)).isEqualTo(String[].class);
		softly.assertThat(converter.getColumnType(listOfString)).isEqualTo(String[].class);

		softly.assertAll();
	}

	@Test // DATAJDBC-221
	public void referencesAreNotEntitiesAndGetStoredAsTheirId() {

		RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);

		SoftAssertions softly = new SoftAssertions();

		RelationalPersistentProperty reference = entity.getRequiredPersistentProperty("reference");

		softly.assertThat(reference.isEntity()).isFalse();
		softly.assertThat(converter.getColumnType(reference)).isEqualTo(Long.class);

		softly.assertAll();
	}

	private void checkTargetType(SoftAssertions softly, RelationalPersistentEntity<?> persistentEntity,
			String propertyName, Class<?> expected) {

		RelationalPersistentProperty property = persistentEntity.getRequiredPersistentProperty(propertyName);

		softly.assertThat(converter.getColumnType(property)).describedAs(propertyName).isEqualTo(expected);
	}

	@Data
	@SuppressWarnings("unused")
	private static class DummyEntity {

		@Id private final Long id;
		private final SomeEnum someEnum;
		private final LocalDateTime localDateTime;
		private final ZonedDateTime zonedDateTime;
		private final AggregateReference<DummyEntity, Long> reference;
		private final UUID uuid;

		// DATAJDBC-259
		private final List<String> listOfString;
		private final String[] arrayOfString;
		private final List<OtherEntity> listOfEntity;
		private final OtherEntity[] arrayOfEntity;

	}

	@SuppressWarnings("unused")
	private enum SomeEnum {
		ALPHA
	}

	@SuppressWarnings("unused")
	private static class OtherEntity {}
}
