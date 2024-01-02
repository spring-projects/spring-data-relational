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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;

import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jdbc.core.convert.CascadingDataAccessStrategy;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.mybatis.MyBatisDataAccessStrategy;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.HsqlDbDialect;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Integration tests for {@link MyBatisJdbcConfiguration}.
 *
 * @author Oliver Drotbohm
 */
public class MyBatisJdbcConfigurationIntegrationTests extends AbstractJdbcConfigurationIntegrationTests {

	@Test // DATAJDBC-395
	public void bootstrapsMyBatisDataAccessStrategy() {

		assertApplicationContext(context -> {

			assertThat(context.getBean(DataAccessStrategy.class)) //
					.isInstanceOfSatisfying(CascadingDataAccessStrategy.class, it -> {

						List<?> strategies = (List<?>) ReflectionTestUtils.getField(it, "strategies");

						assertThat(strategies).hasSize(2);
						assertThat(strategies.get(0)).isInstanceOf(MyBatisDataAccessStrategy.class);
					});

		}, MyBatisJdbcConfigurationUnderTest.class, MyBatisInfrastructure.class);
	}

	@Configuration
	static class MyBatisInfrastructure extends Infrastructure {

		@Bean
		public SqlSession session() {
			return mock(SqlSession.class);
		}
	}

	public static class MyBatisJdbcConfigurationUnderTest extends MyBatisJdbcConfiguration {

		@Override
		@Bean
		public Dialect jdbcDialect(NamedParameterJdbcOperations operations) {
			return HsqlDbDialect.INSTANCE;
		}
	}
}
