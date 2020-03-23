/*
 * Copyright 2019-2020 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.junit.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jdbc.core.JdbcAggregateTemplate;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.HsqlDbDialect;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * Integration tests for {@link AbstractJdbcConfiguration}.
 *
 * @author Oliver Drotbohm
 */
public class AbstractJdbcConfigurationIntegrationTests {

	@Test // DATAJDBC-395
	public void configuresInfrastructureComponents() {

		assertApplicationContext(context -> {

			List<Class<?>> expectedBeanTypes = Arrays.asList(DataAccessStrategy.class, //
					JdbcMappingContext.class, //
					JdbcConverter.class, //
					JdbcCustomConversions.class, //
					JdbcAggregateTemplate.class);

			expectedBeanTypes.stream() //
					.map(context::getBean) //
					.forEach(it -> assertThat(it).isNotNull());

		}, AbstractJdbcConfigurationUnderTest.class, Infrastructure.class);
	}

	protected static void assertApplicationContext(Consumer<ConfigurableApplicationContext> verification,
			Class<?>... configurationClasses) {

		try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {

			context.register(configurationClasses);
			context.refresh();

			verification.accept(context);
		}
	}

	@Configuration
	static class        Infrastructure {

		@Bean
		public NamedParameterJdbcOperations jdbcOperations() {

			JdbcOperations jdbcOperations = mock(JdbcOperations.class);
			return new NamedParameterJdbcTemplate(jdbcOperations);
		}
	}

	static class AbstractJdbcConfigurationUnderTest extends AbstractJdbcConfiguration {

		@Override
		@Bean
		public Dialect dialect(NamedParameterJdbcOperations template) {
			return HsqlDbDialect.INSTANCE;
		}
	}

}
