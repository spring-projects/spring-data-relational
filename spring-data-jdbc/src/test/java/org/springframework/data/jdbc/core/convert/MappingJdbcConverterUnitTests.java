/*
 * Copyright 2020-2024 the original author or authors.
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
import static org.assertj.core.api.SoftAssertions.*;
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.core.mapping.JdbcValue;
import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.data.relational.core.mapping.MappedCollection;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.RowDocument;
import org.springframework.data.util.TypeInformation;

/**
 * Unit tests for {@link MappingJdbcConverter}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
class MappingJdbcConverterUnitTests {

	private static final UUID UUID = java.util.UUID.fromString("87a48aa8-a071-705e-54a9-e52fe3a012f1");
	private static final byte[] BYTES_REPRESENTING_UUID = { -121, -92, -118, -88, -96, 113, 112, 94, 84, -87, -27, 47,
			-29,
			-96, 18, -15 };

	private JdbcMappingContext context = new JdbcMappingContext();
	private StubbedJdbcTypeFactory typeFactory = new StubbedJdbcTypeFactory();
	private MappingJdbcConverter converter = new MappingJdbcConverter( //
			context, //
			(identifier, path) -> {
				throw new UnsupportedOperationException();
			}, //
			new JdbcCustomConversions(), //
			typeFactory //
	);

	@Test // DATAJDBC-104, DATAJDBC-1384
	void testTargetTypesForPropertyType() {

		RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);

		SoftAssertions softly = new SoftAssertions();

		checkTargetType(softly, entity, "someEnum", String.class);
		checkTargetType(softly, entity, "localDateTime", LocalDateTime.class);
		checkTargetType(softly, entity, "localDate", Timestamp.class);
		checkTargetType(softly, entity, "localTime", Timestamp.class);
		checkTargetType(softly, entity, "zonedDateTime", String.class);
		checkTargetType(softly, entity, "offsetDateTime", OffsetDateTime.class);
		checkTargetType(softly, entity, "instant", Timestamp.class);
		checkTargetType(softly, entity, "date", Date.class);
		checkTargetType(softly, entity, "timestamp", Timestamp.class);
		checkTargetType(softly, entity, "uuid", UUID.class);

		softly.assertAll();
	}

	@Test // DATAJDBC-259
	void classificationOfCollectionLikeProperties() {

		RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);

		RelationalPersistentProperty listOfString = entity.getRequiredPersistentProperty("listOfString");
		RelationalPersistentProperty arrayOfString = entity.getRequiredPersistentProperty("arrayOfString");

		SoftAssertions softly = new SoftAssertions();

		softly.assertThat(converter.getColumnType(arrayOfString)).isEqualTo(String[].class);
		softly.assertThat(converter.getColumnType(listOfString)).isEqualTo(String[].class);

		softly.assertAll();
	}

	@Test // DATAJDBC-221
	void referencesAreNotEntitiesAndGetStoredAsTheirId() {

		RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);

		SoftAssertions softly = new SoftAssertions();

		RelationalPersistentProperty reference = entity.getRequiredPersistentProperty("reference");

		softly.assertThat(reference.isEntity()).isFalse();
		softly.assertThat(converter.getColumnType(reference)).isEqualTo(Long.class);

		softly.assertAll();
	}

	@Test // DATAJDBC-637
	void conversionOfDateLikeValueAndBackYieldsOriginalValue() {

		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(DummyEntity.class);

		assertSoftly(softly -> {
			LocalDateTime testLocalDateTime = LocalDateTime.of(2001, 2, 3, 4, 5, 6, 123456789);
			checkConversionToTimestampAndBack(softly, persistentEntity, "localDateTime", testLocalDateTime);
			checkConversionToTimestampAndBack(softly, persistentEntity, "localDate", LocalDate.of(2001, 2, 3));
			checkConversionToTimestampAndBack(softly, persistentEntity, "localTime", LocalTime.of(1, 2, 3, 123456789));
			checkConversionToTimestampAndBack(softly, persistentEntity, "instant",
					testLocalDateTime.toInstant(ZoneOffset.UTC));
		});

	}

	@Test // GH-945
	void conversionOfPrimitiveArrays() {

		int[] ints = { 1, 2, 3, 4, 5 };
		JdbcValue converted = converter.writeJdbcValue(ints, ints.getClass(), JdbcUtil.targetSqlTypeFor(ints.getClass()));

		assertThat(converted.getValue()).isInstanceOf(Array.class);
		assertThat(typeFactory.arraySource).containsExactly(1, 2, 3, 4, 5);
	}

	@Test // GH-1684
	void accessesCorrectValuesForOneToOneRelationshipWithIdenticallyNamedIdProperties() {

		RowDocument rowdocument = new RowDocument(Map.of("ID", "one", "REFERENCED_ID", 23));

		WithOneToOne result = converter.readAndResolve(WithOneToOne.class, rowdocument);

		assertThat(result).isEqualTo(new WithOneToOne("one", new Referenced(23L)));
	}

	@Test // GH-1750
	void readByteArrayToNestedUuidWithCustomConverter() {

		JdbcMappingContext context = new JdbcMappingContext();
		StubbedJdbcTypeFactory typeFactory = new StubbedJdbcTypeFactory();
		Converter<byte[], UUID> customConverter = new ByteArrayToUuid();
		MappingJdbcConverter converter = new MappingJdbcConverter( //
				context, //
				(identifier, path) -> {
					throw new UnsupportedOperationException();
				}, //
				new JdbcCustomConversions(Collections.singletonList(customConverter)), //
				typeFactory //
		);

		assertSoftly(softly -> {
			checkReadConversion(softly, converter, "uuidRef", AggregateReference.to(UUID));
			checkReadConversion(softly, converter, "uuid", UUID);
			checkReadConversion(softly, converter, "optionalUuid", Optional.of(UUID));
		});

	}

	private static void checkReadConversion(SoftAssertions softly, MappingJdbcConverter converter, String propertyName,
			Object expected) {

		RelationalPersistentProperty property = converter.getMappingContext().getRequiredPersistentEntity(DummyEntity.class)
				.getRequiredPersistentProperty(propertyName);
		Object value = converter.readValue(BYTES_REPRESENTING_UUID, property.getTypeInformation() //
		);

		softly.assertThat(value).isEqualTo(expected);
	}

	private void checkConversionToTimestampAndBack(SoftAssertions softly, RelationalPersistentEntity<?> persistentEntity,
			String propertyName, Object value) {

		RelationalPersistentProperty property = persistentEntity.getRequiredPersistentProperty(propertyName);

		Object converted = converter.writeValue(value, TypeInformation.of(converter.getColumnType(property)));
		Object convertedBack = converter.readValue(converted, property.getTypeInformation());

		softly.assertThat(convertedBack).describedAs(propertyName).isEqualTo(value);
	}

	private void checkTargetType(SoftAssertions softly, RelationalPersistentEntity<?> persistentEntity,
			String propertyName, Class<?> expected) {

		RelationalPersistentProperty property = persistentEntity.getRequiredPersistentProperty(propertyName);

		softly.assertThat(converter.getColumnType(property)).describedAs(propertyName).isEqualTo(expected);
	}

	@SuppressWarnings("unused")
	private static class DummyEntity {

		@Id private final Long id;
		private final SomeEnum someEnum;
		private final LocalDateTime localDateTime;
		private final LocalDate localDate;
		private final LocalTime localTime;
		private final ZonedDateTime zonedDateTime;
		private final OffsetDateTime offsetDateTime;
		private final Instant instant;
		private final Date date;
		private final Timestamp timestamp;
		private final AggregateReference<DummyEntity, Long> reference;
		private final UUID uuid;
		private final AggregateReference<ReferencedByUuid, UUID> uuidRef;
		private final Optional<UUID> optionalUuid;

		// DATAJDBC-259
		private final List<String> listOfString;
		private final String[] arrayOfString;
		private final List<OtherEntity> listOfEntity;
		private final OtherEntity[] arrayOfEntity;

		private DummyEntity(Long id, SomeEnum someEnum, LocalDateTime localDateTime, LocalDate localDate,
							LocalTime localTime, ZonedDateTime zonedDateTime, OffsetDateTime offsetDateTime, Instant instant, Date date,
							Timestamp timestamp, AggregateReference<DummyEntity, Long> reference, UUID uuid,
							AggregateReference<ReferencedByUuid, UUID> uuidRef, Optional<java.util.UUID> optionalUUID, List<String> listOfString, String[] arrayOfString,
							List<OtherEntity> listOfEntity, OtherEntity[] arrayOfEntity) {
			this.id = id;
			this.someEnum = someEnum;
			this.localDateTime = localDateTime;
			this.localDate = localDate;
			this.localTime = localTime;
			this.zonedDateTime = zonedDateTime;
			this.offsetDateTime = offsetDateTime;
			this.instant = instant;
			this.date = date;
			this.timestamp = timestamp;
			this.reference = reference;
			this.uuid = uuid;
			this.uuidRef = uuidRef;
			this.optionalUuid = optionalUUID;
			this.listOfString = listOfString;
			this.arrayOfString = arrayOfString;
			this.listOfEntity = listOfEntity;
			this.arrayOfEntity = arrayOfEntity;
		}

		public Long getId() {
			return this.id;
		}

		public SomeEnum getSomeEnum() {
			return this.someEnum;
		}

		public LocalDateTime getLocalDateTime() {
			return this.localDateTime;
		}

		public LocalDate getLocalDate() {
			return this.localDate;
		}

		public LocalTime getLocalTime() {
			return this.localTime;
		}

		public ZonedDateTime getZonedDateTime() {
			return this.zonedDateTime;
		}

		public OffsetDateTime getOffsetDateTime() {
			return this.offsetDateTime;
		}

		public Instant getInstant() {
			return this.instant;
		}

		public Date getDate() {
			return this.date;
		}

		public Timestamp getTimestamp() {
			return this.timestamp;
		}

		public AggregateReference<DummyEntity, Long> getReference() {
			return this.reference;
		}

		public UUID getUuid() {
			return this.uuid;
		}

		public List<String> getListOfString() {
			return this.listOfString;
		}

		public String[] getArrayOfString() {
			return this.arrayOfString;
		}

		public List<OtherEntity> getListOfEntity() {
			return this.listOfEntity;
		}

		public OtherEntity[] getArrayOfEntity() {
			return this.arrayOfEntity;
		}
	}

	@SuppressWarnings("unused")
	private enum SomeEnum {
		ALPHA
	}

	@SuppressWarnings("unused")
	private static class OtherEntity {}

	private static class StubbedJdbcTypeFactory implements JdbcTypeFactory {
		Object[] arraySource;

		@Override
		public Array createArray(Object[] value) {
			arraySource = value;
			return mock(Array.class);
		}
	}

	private record WithOneToOne(@Id String id, @MappedCollection(idColumn = "renamed") Referenced referenced) {
	}

	private record Referenced(@Id Long id) {
	}

	private record ReferencedByUuid(@Id UUID id) {
	}

	static class ByteArrayToUuid implements Converter<byte[], UUID> {
		@Override
		public UUID convert(byte[] source) {

			ByteBuffer byteBuffer = ByteBuffer.wrap(source);
			long high = byteBuffer.getLong();
			long low = byteBuffer.getLong();
			return new UUID(high, low);
		}
	}
}
