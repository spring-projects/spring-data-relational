/*
 * Copyright 2021-2024 the original author or authors.
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
package org.springframework.data.r2dbc.repository;

import io.r2dbc.spi.ConnectionFactory;

import java.util.Optional;

import javax.sql.DataSource;

import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.convert.R2dbcCustomConversions;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.data.r2dbc.testing.EnabledOnClass;
import org.springframework.data.r2dbc.testing.ExternalDatabase;
import org.springframework.data.r2dbc.testing.OracleTestSupport;
import org.springframework.data.relational.RelationalManagedTypes;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration tests for {@link LegoSetRepository} with table and column names that contain upper and lower case
 * characters against Oracle.
 *
 * @author Jens Schauder
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
@EnabledOnClass("oracle.r2dbc.impl.OracleConnectionFactoryProviderImpl")
@DisabledOnOs(architectures = "aarch64")
public class OracleR2dbcRepositoryWithMixedCaseNamesIntegrationTests
		extends AbstractR2dbcRepositoryWithMixedCaseNamesIntegrationTests {

	@RegisterExtension public static final ExternalDatabase database = OracleTestSupport.database();

	@Configuration
	@EnableR2dbcRepositories(considerNestedRepositories = true,
			includeFilters = @Filter(classes = { LegoSetRepository.class }, type = FilterType.ASSIGNABLE_TYPE))
	static class IntegrationTestConfiguration extends AbstractR2dbcConfiguration {

		@Bean
		@Override
		public ConnectionFactory connectionFactory() {
			return OracleTestSupport.createConnectionFactory(database);
		}

		@Override
		public R2dbcMappingContext r2dbcMappingContext(Optional<NamingStrategy> namingStrategy,
				R2dbcCustomConversions r2dbcCustomConversions, RelationalManagedTypes r2dbcManagedTypes) {

			R2dbcMappingContext r2dbcMappingContext = super.r2dbcMappingContext(namingStrategy, r2dbcCustomConversions,
					r2dbcManagedTypes);
			r2dbcMappingContext.setForceQuote(true);

			return r2dbcMappingContext;
		}
	}

	@Override
	protected DataSource createDataSource() {
		return OracleTestSupport.createDataSource(database);
	}

	@Override
	protected ConnectionFactory createConnectionFactory() {
		return OracleTestSupport.createConnectionFactory(database);
	}

	@Override
	protected String getCreateTableStatement() {
		return OracleTestSupport.CREATE_TABLE_LEGOSET_WITH_MIXED_CASE_NAMES;
	}

	@Override
	protected String getDropTableStatement() {
		return OracleTestSupport.DROP_TABLE_LEGOSET_WITH_MIXED_CASE_NAMES;
	}
}
