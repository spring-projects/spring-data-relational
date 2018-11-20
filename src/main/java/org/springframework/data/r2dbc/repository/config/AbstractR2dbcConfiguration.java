/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.r2dbc.repository.config;

import java.util.Optional;

import io.r2dbc.spi.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.function.DatabaseClient;
import org.springframework.data.r2dbc.function.DefaultReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.support.R2dbcExceptionTranslator;
import org.springframework.data.r2dbc.support.SqlErrorCodeR2dbcExceptionTranslator;
import org.springframework.data.relational.core.conversion.BasicRelationalConverter;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.util.Assert;

/**
 * Base class for Spring Data R2DBC configuration containing bean declarations that must be registered for Spring Data
 * R2DBC to work.
 *
 * @author Mark Paluch
 * @see ConnectionFactory
 * @see DatabaseClient
 * @see EnableR2dbcRepositories
 */
@Configuration
public abstract class AbstractR2dbcConfiguration {

	/**
	 * Return a R2DBC {@link ConnectionFactory}. Annotate with {@link Bean} in case you want to expose a
	 * {@link ConnectionFactory} instance to the {@link org.springframework.context.ApplicationContext}.
	 *
	 * @return the configured {@link ConnectionFactory}.
	 */
	public abstract ConnectionFactory connectionFactory();

	/**
	 * Register a {@link DatabaseClient} using {@link #connectionFactory()} and {@link RelationalMappingContext}.
	 *
	 * @return must not be {@literal null}.
	 * @throws IllegalArgumentException if any of the required args is {@literal null}.
	 */
	@Bean
	public DatabaseClient databaseClient(ReactiveDataAccessStrategy dataAccessStrategy,
			R2dbcExceptionTranslator exceptionTranslator) {

		Assert.notNull(dataAccessStrategy, "DataAccessStrategy must not be null!");
		Assert.notNull(exceptionTranslator, "ExceptionTranslator must not be null!");

		return DatabaseClient.builder() //
				.connectionFactory(connectionFactory()) //
				.dataAccessStrategy(dataAccessStrategy) //
				.exceptionTranslator(exceptionTranslator) //
				.build();
	}

	/**
	 * Register a {@link RelationalMappingContext} and apply an optional {@link NamingStrategy}.
	 *
	 * @param namingStrategy optional {@link NamingStrategy}. Use {@link NamingStrategy#INSTANCE} as fallback.
	 * @return must not be {@literal null}.
	 * @throws IllegalArgumentException if any of the required args is {@literal null}.
	 */
	@Bean
	public RelationalMappingContext r2dbcMappingContext(Optional<NamingStrategy> namingStrategy) {

		Assert.notNull(namingStrategy, "NamingStrategy must not be null!");

		return new RelationalMappingContext(namingStrategy.orElse(NamingStrategy.INSTANCE));
	}

	/**
	 * Creates a {@link ReactiveDataAccessStrategy} using the configured {@link #r2dbcMappingContext(Optional) RelationalMappingContext}.
	 *
	 * @param mappingContext the configured {@link RelationalMappingContext}.
	 * @return must not be {@literal null}.
	 * @see #r2dbcMappingContext(Optional)
	 * @throws IllegalArgumentException if any of the {@literal mappingContext} is {@literal null}.
	 */
	@Bean
	public ReactiveDataAccessStrategy reactiveDataAccessStrategy(RelationalMappingContext mappingContext) {

		Assert.notNull(mappingContext, "MappingContext must not be null!");
		return new DefaultReactiveDataAccessStrategy(new BasicRelationalConverter(mappingContext));
	}

	/**
	 * Creates a {@link R2dbcExceptionTranslator} using the configured {@link #connectionFactory() ConnectionFactory}.
	 *
	 * @return must not be {@literal null}.
	 * @see #connectionFactory()
	 */
	@Bean
	public R2dbcExceptionTranslator exceptionTranslator() {
		return new SqlErrorCodeR2dbcExceptionTranslator(connectionFactory());
	}
}
