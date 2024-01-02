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

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.convert.R2dbcCustomConversions;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.data.r2dbc.testing.H2TestSupport;
import org.springframework.data.relational.RelationalManagedTypes;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration tests for {@link LegoSetRepository} with table and column names that contain upper and lower case
 * characters against H2.
 *
 * @author Jens Schauder
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class H2R2dbcRepositoryWithMixedCaseNamesIntegrationTests
		extends AbstractR2dbcRepositoryWithMixedCaseNamesIntegrationTests {

	@Configuration
	@EnableR2dbcRepositories(considerNestedRepositories = true,
			includeFilters = @Filter(classes = { LegoSetRepository.class }, type = FilterType.ASSIGNABLE_TYPE))
	static class IntegrationTestConfiguration extends AbstractR2dbcConfiguration {

		@Bean
		@Override
		public ConnectionFactory connectionFactory() {
			return H2TestSupport.createConnectionFactory();
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
		return H2TestSupport.createDataSource();
	}

	@Override
	protected ConnectionFactory createConnectionFactory() {
		return H2TestSupport.createConnectionFactory();
	}

	@Override
	protected String getCreateTableStatement() {
		return H2TestSupport.CREATE_TABLE_LEGOSET_WITH_MIXED_CASE_NAMES;
	}

	@Override
	protected String getDropTableStatement() {
		return H2TestSupport.DROP_TABLE_LEGOSET_WITH_MIXED_CASE_NAMES;
	}
}
