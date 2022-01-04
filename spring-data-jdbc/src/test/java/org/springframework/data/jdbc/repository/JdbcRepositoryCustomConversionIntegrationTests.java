/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.jdbc.repository;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.core.convert.JdbcValue;
import org.springframework.data.jdbc.core.dialect.H2TimestampWithTimeZoneToOffsetDateTimeConverter;
import org.springframework.data.jdbc.core.dialect.H2TimestampWithTimeZoneToZonedDateTimeConverter;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.AssumeFeatureTestExecutionListener;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Optional;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.springframework.test.context.TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS;

/**
 * Tests storing and retrieving data types that get processed by custom conversions.
 *
 * @author Jens Schauder
 * @author Sanghyuk Jung
 * @author Mikhail Polivakha
 */
@ContextConfiguration
@Transactional
@TestExecutionListeners(value = AssumeFeatureTestExecutionListener.class, mergeMode = MERGE_WITH_DEFAULTS)
@ExtendWith(SpringExtension.class)
public class JdbcRepositoryCustomConversionIntegrationTests {

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryCustomConversionIntegrationTests.class;
		}

		@Bean
		EntityWithBooleanRepository entityWithBooleanRepository() {
			return factory.getRepository(EntityWithBooleanRepository.class);
		}

		@Bean
		EntityWithZonedDateTimeRepository entityWithZonedDateTimeRepository() {
			return factory.getRepository(EntityWithZonedDateTimeRepository.class);
		}

		@Bean
		EntityWithOffsetDateTimeRepository entityWithOffsetDateTimeRepository() {
			return factory.getRepository(EntityWithOffsetDateTimeRepository.class);
		}

		@Bean
		JdbcCustomConversions jdbcCustomConversions() {
			return new JdbcCustomConversions(
					asList(
							StringToBigDecimalConverter.INSTANCE,
							BigDecimalToString.INSTANCE,
							CustomIdReadingConverter.INSTANCE,
							CustomIdWritingConverter.INSTANCE,
							TimestampToOffsetDateTimeConverter.INSTANCE,
							TimestampToZonedDateTimeConverter.INSTANCE,
							H2TimestampWithTimeZoneToZonedDateTimeConverter.INSTANCE,
							H2TimestampWithTimeZoneToOffsetDateTimeConverter.INSTANCE
					)
			);
		}
	}

	@Autowired EntityWithBooleanRepository repository;

	@Autowired EntityWithZonedDateTimeRepository entityWithZonedDateTimeRepository;

	@Autowired EntityWithOffsetDateTimeRepository entityWithOffsetDateTimeRepository;

	/**
	 * In PostgreSQL this fails if a simple converter like the following is used.
	 *
	 * <pre class="code">
	 *
	 * &#64;WritingConverter
	 * enum PlainStringToBigDecimalConverter implements Converter<String, BigDecimal> {
	 *
	 * 	INSTANCE;
	 *
	 * 	&#64;Override
	 * 	&#64;Nullable
	 * 	public BigDecimal convert(String source) {
	 *
	 * 		return source == new BigDecimal(source);
	 * 	}
	 * }
	 * </pre>
	 */
	@Test // DATAJDBC-327
	public void saveAndLoadAnEntity() {

		EntityWithStringyBigDecimal entity = new EntityWithStringyBigDecimal();
		entity.stringyNumber = "123456.78912";

		repository.save(entity);

		EntityWithStringyBigDecimal reloaded = repository.findById(entity.id).get();

		// loading the number from the database might result in additional zeros at the end.
		String stringyNumber = reloaded.stringyNumber;
		assertThat(stringyNumber).startsWith(entity.stringyNumber);
		assertThat(stringyNumber.substring(entity.stringyNumber.length())).matches("0*");
	}

	@Test // DATAJDBC-412
	public void saveAndLoadAnEntityWithReference() {

		EntityWithStringyBigDecimal entity = new EntityWithStringyBigDecimal();
		entity.stringyNumber = "123456.78912";
		entity.reference = new OtherEntity();
		entity.reference.created = new Date();

		repository.save(entity);

		EntityWithStringyBigDecimal reloaded = repository.findById(entity.id).get();

		// loading the number from the database might result in additional zeros at the end.
		assertSoftly(softly -> {
			String stringyNumber = reloaded.stringyNumber;
			softly.assertThat(stringyNumber).startsWith(entity.stringyNumber);
			softly.assertThat(stringyNumber.substring(entity.stringyNumber.length())).matches("0*");

			softly.assertThat(entity.id.value).isNotNull();
			softly.assertThat(reloaded.id.value).isEqualTo(entity.id.value);

			softly.assertThat(entity.reference.id.value).isNotNull();
			softly.assertThat(reloaded.reference.id.value).isEqualTo(entity.reference.id.value);
		});
	}

	/**
	 * DATAJDBC-1089
	 */
	@Test
	public void testZonedDateTimeToTimestampConversion() {
		EntityWithZonedDateTime entity = new EntityWithZonedDateTime();
		entity.createdAt = ZonedDateTime.now(ZoneOffset.ofHours(3));

		final EntityWithZonedDateTime persistedEntity = entityWithZonedDateTimeRepository.save(entity);
		final Optional<EntityWithZonedDateTime> foundEntity = entityWithZonedDateTimeRepository.findById(persistedEntity.id);

		assertThat(foundEntity).isPresent();
		assertThat(persistedEntity.createdAt.toEpochSecond()).isEqualTo(foundEntity.get().createdAt.toEpochSecond());
	}

	/**
	 * DATAJDBC-1089
	 */
	@Test
	public void testOffsetDateTimeToTimestampConversion() {
		EntityWithOffsetDateTime entity = new EntityWithOffsetDateTime();
		entity.createdAt = OffsetDateTime.now(ZoneOffset.ofHours(3));

		final EntityWithOffsetDateTime persistedEntity = entityWithOffsetDateTimeRepository.save(entity);
		final Optional<EntityWithOffsetDateTime> foundEntity = entityWithOffsetDateTimeRepository.findById(persistedEntity.id);

		assertThat(foundEntity).isPresent();
		assertThat(persistedEntity.createdAt.toEpochSecond()).isEqualTo(foundEntity.get().createdAt.toEpochSecond());
	}

	interface EntityWithBooleanRepository extends CrudRepository<EntityWithStringyBigDecimal, CustomId> {}

	interface EntityWithZonedDateTimeRepository extends CrudRepository<EntityWithZonedDateTime, Long> {};

	interface EntityWithOffsetDateTimeRepository extends CrudRepository<EntityWithOffsetDateTime, Long> {};

	private static class EntityWithStringyBigDecimal {
		@Id CustomId id;
		String stringyNumber;
		OtherEntity reference;
	}

	private static class EntityWithZonedDateTime {
		@Id private Long id;
		private ZonedDateTime createdAt;
	}

	private static class EntityWithOffsetDateTime {
		@Id private Long id;
		private OffsetDateTime createdAt;
	}

	private static class CustomId {

		private final Long value;

		CustomId(Long value) {
			this.value = value;
		}
	}

	private static class OtherEntity {

		@Id CustomId id;
		Date created;
	}

	@WritingConverter
	enum StringToBigDecimalConverter implements Converter<String, JdbcValue> {

		INSTANCE;

		@Override
		public JdbcValue convert(String source) {

			Object value = new BigDecimal(source);
			return JdbcValue.of(value, JDBCType.DECIMAL);
		}
	}

	@ReadingConverter
	enum BigDecimalToString implements Converter<BigDecimal, String> {

		INSTANCE;

		@Override
		public String convert(BigDecimal source) {

			return source.toString();
		}
	}

	@WritingConverter
	enum CustomIdWritingConverter implements Converter<CustomId, Number> {

		INSTANCE;

		@Override
		public Number convert(CustomId source) {
			return source.value.intValue();
		}
	}

	@ReadingConverter
	enum CustomIdReadingConverter implements Converter<Number, CustomId> {

		INSTANCE;

		@Override
		public CustomId convert(Number source) {
			return new CustomId(source.longValue());
		}
	}

	@ReadingConverter
	public enum TimestampToZonedDateTimeConverter implements Converter<Timestamp, ZonedDateTime> {

		INSTANCE;

		@Override
		public ZonedDateTime convert(Timestamp source) {
			return ZonedDateTime.ofInstant(source.toInstant(), ZoneOffset.ofHours(3));
		}
	}

	@ReadingConverter
	public enum TimestampToOffsetDateTimeConverter implements Converter<Timestamp, OffsetDateTime> {

		INSTANCE;

		@Override
		public OffsetDateTime convert(Timestamp source) {
			return OffsetDateTime.ofInstant(source.toInstant(), ZoneOffset.ofHours(3));
		}
	}
}