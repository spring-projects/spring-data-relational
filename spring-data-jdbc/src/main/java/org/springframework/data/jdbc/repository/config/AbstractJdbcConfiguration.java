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

import java.util.Optional;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.JdbcAggregateTemplate;
import org.springframework.data.jdbc.core.convert.BasicJdbcConverter;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.DefaultDataAccessStrategy;
import org.springframework.data.jdbc.core.convert.DefaultJdbcTypeFactory;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.core.convert.RelationResolver;
import org.springframework.data.jdbc.core.convert.SqlGeneratorSource;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * Beans that must be registered for Spring Data JDBC to work.
 *
 * @author Greg Turnquist
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Michael Simons
 * @author Christoph Strobl
 * @since 1.1
 */
@Configuration(proxyBeanMethods = false)
public class AbstractJdbcConfiguration {

	/**
	 * Register a {@link JdbcMappingContext} and apply an optional {@link NamingStrategy}.
	 *
	 * @param namingStrategy optional {@link NamingStrategy}. Use {@link NamingStrategy#INSTANCE} as fallback.
	 * @param customConversions see {@link #jdbcCustomConversions()}.
	 * @return must not be {@literal null}.
	 */
	@Bean
	public JdbcMappingContext jdbcMappingContext(Optional<NamingStrategy> namingStrategy,
			JdbcCustomConversions customConversions) {

		JdbcMappingContext mappingContext = new JdbcMappingContext(namingStrategy.orElse(NamingStrategy.INSTANCE));
		mappingContext.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());

		return mappingContext;
	}

	/**
	 * Creates a {@link RelationalConverter} using the configured
	 * {@link #jdbcMappingContext(Optional, JdbcCustomConversions)}. Will get {@link #jdbcCustomConversions()} applied.
	 *
	 * @see #jdbcMappingContext(Optional, JdbcCustomConversions)
	 * @see #jdbcCustomConversions()
	 * @return must not be {@literal null}.
	 */
	@Bean
	public JdbcConverter jdbcConverter(JdbcMappingContext mappingContext, NamedParameterJdbcOperations operations,
			@Lazy RelationResolver relationResolver, JdbcCustomConversions conversions) {

		DefaultJdbcTypeFactory jdbcTypeFactory = new DefaultJdbcTypeFactory(operations.getJdbcOperations());

		return new BasicJdbcConverter(mappingContext, relationResolver, conversions, jdbcTypeFactory);
	}

	/**
	 * Register custom {@link Converter}s in a {@link JdbcCustomConversions} object if required. These
	 * {@link JdbcCustomConversions} will be registered with the
	 * {@link #jdbcConverter(JdbcMappingContext, NamedParameterJdbcOperations, RelationResolver, JdbcCustomConversions)}.
	 * Returns an empty {@link JdbcCustomConversions} instance by default.
	 *
	 * @return will never be {@literal null}.
	 */
	@Bean
	public JdbcCustomConversions jdbcCustomConversions() {
		return new JdbcCustomConversions();
	}

	/**
	 * Register a {@link JdbcAggregateTemplate} as a bean for easy use in applications that need a lower level of
	 * abstraction than the normal repository abstraction.
	 *
	 * @param applicationContext for publishing events. Must not be {@literal null}.
	 * @param mappingContext the mapping context to be used. Must not be {@literal null}.
	 * @param converter the conversions used when reading and writing from/to the database. Must not be {@literal null}.
	 * @return a {@link JdbcAggregateTemplate}. Will never be {@literal null}.
	 */
	@Bean
	public JdbcAggregateTemplate jdbcAggregateTemplate(ApplicationContext applicationContext,
			JdbcMappingContext mappingContext, JdbcConverter converter, DataAccessStrategy dataAccessStrategy) {

		return new JdbcAggregateTemplate(applicationContext, mappingContext, converter, dataAccessStrategy);
	}

	/**
	 * Create a {@link DataAccessStrategy} for reuse in the {@link JdbcAggregateOperations} and the {@link JdbcConverter}.
	 * Override this method to register a bean of type {@link DataAccessStrategy} if your use case requires a more
	 * specialized {@link DataAccessStrategy}.
	 *
	 * @return will never be {@literal null}.
	 */
	@Bean
	public DataAccessStrategy dataAccessStrategyBean(NamedParameterJdbcOperations operations, JdbcConverter jdbcConverter,
			JdbcMappingContext context, Dialect dialect) {
		return new DefaultDataAccessStrategy(new SqlGeneratorSource(context, jdbcConverter, dialect), context,
				jdbcConverter, operations);
	}

	@Bean
	public Dialect dialect(NamedParameterJdbcOperations template) {
		return JdbcDialectResolver.getDialect(template);
	}
}
