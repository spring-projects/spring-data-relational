/*
 * Copyright 2019-2023 the original author or authors.
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

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.SoftAssertions.*;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.core.mapping.JdbcValue;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.CrudRepository;

/**
 * Tests storing and retrieving data types that get processed by custom conversions.
 *
 * @author Jens Schauder
 * @author Sanghyuk Jung
 * @author Chirag Tailor
 */
@IntegrationTest
public class JdbcRepositoryCustomConversionIntegrationTests {

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Bean
		EntityWithStringyBigDecimalRepository repository(JdbcRepositoryFactory factory) {
			return factory.getRepository(EntityWithStringyBigDecimalRepository.class);
		}

		@Bean
		JdbcCustomConversions jdbcCustomConversions() {
			return new JdbcCustomConversions(asList(StringToBigDecimalConverter.INSTANCE, BigDecimalToString.INSTANCE,
					CustomIdReadingConverter.INSTANCE, CustomIdWritingConverter.INSTANCE, DirectionToIntegerConverter.INSTANCE,
					NumberToDirectionConverter.INSTANCE, IntegerToDirectionConverter.INSTANCE));
		}
	}

	@Autowired EntityWithStringyBigDecimalRepository repository;

	/**
	 * In PostrgreSQL this fails if a simple converter like the following is used.
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

	@Test // GH-1212
	void queryByEnumTypeIn() {

		EntityWithStringyBigDecimal entityA = new EntityWithStringyBigDecimal();
		entityA.direction = Direction.LEFT;
		EntityWithStringyBigDecimal entityB = new EntityWithStringyBigDecimal();
		entityB.direction = Direction.CENTER;
		EntityWithStringyBigDecimal entityC = new EntityWithStringyBigDecimal();
		entityC.direction = Direction.RIGHT;
		repository.saveAll(asList(entityA, entityB, entityC));

		assertThat(repository.findByEnumTypeIn(Set.of(Direction.LEFT, Direction.RIGHT)))
				.extracting(entity -> entity.direction).containsExactlyInAnyOrder(Direction.LEFT, Direction.RIGHT);
	}

	@Test // GH-1212
	void queryByEnumTypeEqual() {

		EntityWithStringyBigDecimal entityA = new EntityWithStringyBigDecimal();
		entityA.direction = Direction.LEFT;
		EntityWithStringyBigDecimal entityB = new EntityWithStringyBigDecimal();
		entityB.direction = Direction.CENTER;
		EntityWithStringyBigDecimal entityC = new EntityWithStringyBigDecimal();
		entityC.direction = Direction.RIGHT;
		repository.saveAll(asList(entityA, entityB, entityC));

		assertThat(repository.findByEnumTypeIn(Set.of(Direction.CENTER))).extracting(entity -> entity.direction)
				.containsExactly(Direction.CENTER);
	}

	interface EntityWithStringyBigDecimalRepository extends CrudRepository<EntityWithStringyBigDecimal, CustomId> {

		@Query("SELECT * FROM ENTITY_WITH_STRINGY_BIG_DECIMAL WHERE DIRECTION IN (:types)")
		List<EntityWithStringyBigDecimal> findByEnumTypeIn(Set<Direction> types);

		@Query("SELECT * FROM ENTITY_WITH_STRINGY_BIG_DECIMAL WHERE DIRECTION = :type")
		List<EntityWithStringyBigDecimal> findByEnumType(Direction type);
	}

	private static class EntityWithStringyBigDecimal {

		@Id CustomId id;
		String stringyNumber = "1.0";
		OtherEntity reference;
		Direction direction = Direction.CENTER;
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

	enum Direction {
		LEFT, CENTER, RIGHT
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

	@WritingConverter
	enum DirectionToIntegerConverter implements Converter<Direction, JdbcValue> {

		INSTANCE;

		@Override
		public JdbcValue convert(Direction source) {

			int integer = switch (source) {
				case LEFT -> -1;
				case CENTER -> 0;
				case RIGHT -> 1;
			};
			return JdbcValue.of(integer, JDBCType.INTEGER);
		}
	}

	@ReadingConverter // Needed for Oracle since the JDBC driver returns BigDecimal on read
	enum NumberToDirectionConverter implements Converter<Number, Direction> {

		INSTANCE;

		@Override
		public Direction convert(Number source) {
			int sourceAsInt = source.intValue();
			if (sourceAsInt == 0) {
				return Direction.CENTER;
			} else if (sourceAsInt < 0) {
				return Direction.LEFT;
			} else {
				return Direction.RIGHT;
			}
		}
	}

	@ReadingConverter
	enum IntegerToDirectionConverter implements Converter<Integer, Direction> {

		INSTANCE;

		@Override
		public Direction convert(Integer source) {
			if (source == 0) {
				return Direction.CENTER;
			} else if (source < 0) {
				return Direction.LEFT;
			} else {
				return Direction.RIGHT;
			}
		}
	}
}
