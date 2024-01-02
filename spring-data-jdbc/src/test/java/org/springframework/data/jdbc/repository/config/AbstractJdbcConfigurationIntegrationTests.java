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
package org.springframework.data.jdbc.repository.config;

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.jdbc.core.JdbcAggregateTemplate;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.RelationalManagedTypes;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.LimitClause;
import org.springframework.data.relational.core.dialect.LockClause;
import org.springframework.data.relational.core.sql.render.SelectRenderContext;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Integration tests for {@link AbstractJdbcConfiguration}.
 *
 * @author Oliver Drotbohm
 * @author Mark Paluch
 */
class AbstractJdbcConfigurationIntegrationTests {

	@Test // DATAJDBC-395
	void configuresInfrastructureComponents() {

		assertApplicationContext(context -> {

			List<Class<?>> expectedBeanTypes = asList(DataAccessStrategy.class, //
					JdbcMappingContext.class, //
					JdbcConverter.class, //
					JdbcCustomConversions.class, //
					JdbcAggregateTemplate.class);

			expectedBeanTypes.stream() //
					.map(context::getBean) //
					.forEach(it -> assertThat(it).isNotNull());

		}, AbstractJdbcConfigurationUnderTest.class, Infrastructure.class);
	}

	@Test // GH-975
	void registersSimpleTypesFromCustomConversions() {

		assertApplicationContext(context -> {

			JdbcMappingContext mappingContext = context.getBean(JdbcMappingContext.class);
			assertThat( //
					mappingContext.getPersistentEntity(AbstractJdbcConfigurationUnderTest.Blah.class) //
			).describedAs("Blah should not be an entity, since there is a WritingConversion configured for it") //
					.isNull();

		}, AbstractJdbcConfigurationUnderTest.class, Infrastructure.class);
	}

	@Test // GH-908
	void userProvidedConversionsOverwriteDialectSpecificConversions() {

		assertApplicationContext(applicationContext -> {

			Optional<Class<?>> customWriteTarget = applicationContext.getBean(JdbcCustomConversions.class)
					.getCustomWriteTarget(Boolean.class);

			assertThat(customWriteTarget).contains(String.class);

		}, AbstractJdbcConfigurationUnderTest.class, Infrastructure.class);
	}

	@Test // GH-1269
	void detectsInitialEntities() {

		assertApplicationContext(context -> {

			JdbcMappingContext mappingContext = context.getBean(JdbcMappingContext.class);
			RelationalManagedTypes managedTypes = (RelationalManagedTypes) ReflectionTestUtils.getField(mappingContext,
					"managedTypes");

			assertThat(managedTypes.toList()).contains(JdbcRepositoryConfigExtensionUnitTests.Sample.class,
					TopLevelEntity.class);

		}, AbstractJdbcConfigurationUnderTest.class, Infrastructure.class);
	}

	static void assertApplicationContext(Consumer<ConfigurableApplicationContext> verification,
			Class<?>... configurationClasses) {

		try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {

			context.register(configurationClasses);
			context.refresh();

			verification.accept(context);
		}
	}

	@Configuration
	static class Infrastructure {

		@Bean
		public NamedParameterJdbcOperations jdbcOperations() {

			JdbcOperations jdbcOperations = mock(JdbcOperations.class);
			return new NamedParameterJdbcTemplate(jdbcOperations);
		}
	}

	static class AbstractJdbcConfigurationUnderTest extends AbstractJdbcConfiguration {

		@Override
		@Bean
		public Dialect jdbcDialect(NamedParameterJdbcOperations operations) {
			return new DummyDialect();
		}

		@Override
		protected List<?> userConverters() {
			return asList(Blah2BlubbConverter.INSTANCE, BooleanToYnConverter.INSTANCE);
		}

		@WritingConverter
		enum Blah2BlubbConverter implements Converter<Blah, Blubb> {
			INSTANCE;

			@Override
			public Blubb convert(Blah blah) {
				return new Blubb();
			}
		}

		private static class Blah {}

		private static class Blubb {}

		private static class DummyDialect implements Dialect {
			@Override
			public LimitClause limit() {
				return null;
			}

			@Override
			public LockClause lock() {
				return null;
			}

			@Override
			public SelectRenderContext getSelectContext() {
				return null;
			}

			@Override
			public Collection<Object> getConverters() {
				return asList(BooleanToNumberConverter.INSTANCE, NumberToBooleanConverter.INSTANCE);
			}
		}

		@WritingConverter
		enum BooleanToNumberConverter implements Converter<Boolean, Number> {
			INSTANCE;

			@Override
			public Number convert(Boolean source) {
				return source ? 1 : 0;
			}
		}

		@ReadingConverter
		enum NumberToBooleanConverter implements Converter<Number, Boolean> {
			INSTANCE;

			@Override
			public Boolean convert(Number source) {
				return source.intValue() == 0;
			}
		}

		@WritingConverter
		enum BooleanToYnConverter implements Converter<Boolean, String> {
			INSTANCE;

			@Override
			public String convert(Boolean source) {
				return source ? "Y" : "N";
			}
		}

	}

}
