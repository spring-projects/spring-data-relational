/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.util.Optional;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

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
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.lang.Nullable;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.transaction.annotation.Transactional;

/**
 * Tests storing and retrieving data types that get processed by custom conversions.
 *
 * @author Jens Schauder
 */
@ContextConfiguration
@Transactional
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
		EntityWithBooleanRepository repository() {
			return factory.getRepository(EntityWithBooleanRepository.class);
		}

		@Bean
		JdbcCustomConversions jdbcCustomConversions() {
			return new JdbcCustomConversions(asList(BigDecimalToString.INSTANCE, StringToBigDecimalConverter.INSTANCE));
		}
	}

	@ClassRule public static final SpringClassRule classRule = new SpringClassRule();
	@Rule public SpringMethodRule methodRule = new SpringMethodRule();

	@Autowired EntityWithBooleanRepository repository;

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
	 * 	public BigDecimal convert(@Nullable String source) {
	 *
	 * 		return source == null ? null : new BigDecimal(source);
	 * 	}
	 * }
	 * </pre>
	 */
	@Test // DATAJDBC-327
	public void saveAndLoadAnEntity() {

		EntityWithStringyBigDecimal entity = new EntityWithStringyBigDecimal();
		entity.stringyNumber = "123456.78910";

		repository.save(entity);

		Optional<EntityWithStringyBigDecimal> reloaded = repository.findById(entity.id);

		// loading the number from the database might result in additional zeros at the end.
		String stringyNumber = reloaded.get().stringyNumber;
		assertThat(stringyNumber).startsWith(entity.stringyNumber);
		assertThat(stringyNumber.substring(entity.stringyNumber.length())).matches("0*");
	}

	interface EntityWithBooleanRepository extends CrudRepository<EntityWithStringyBigDecimal, Long> {}

	private static class EntityWithStringyBigDecimal {

		@Id Long id;
		String stringyNumber;
	}

	@WritingConverter
	enum StringToBigDecimalConverter implements Converter<String, JdbcValue> {

		INSTANCE;

		@Override
		public JdbcValue convert(@Nullable String source) {

			Object value = source == null ? null : new BigDecimal(source);
			return JdbcValue.of(value, JDBCType.DECIMAL);
		}

	}

	@ReadingConverter
	enum BigDecimalToString implements Converter<BigDecimal, String> {

		INSTANCE;

		@Override
		public String convert(@Nullable BigDecimal source) {

			if (source == null) {
				return null;
			}

			return source.toString();
		}
	}
}
