/*
 * Copyright 2017-2020 the original author or authors.
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

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.JdbcAggregateTemplate;
import org.springframework.data.jdbc.core.convert.BasicJdbcConverter;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.DefaultDataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.core.convert.JdbcTypeFactory;
import org.springframework.data.jdbc.core.convert.RelationResolver;
import org.springframework.data.jdbc.core.convert.SqlGeneratorSource;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.HsqlDbDialect;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * Beans that must be registered for Spring Data JDBC to work.
 *
 * @author Greg Turnquist
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Michael Simons
 * @author Christoph Strobl
 * @author Myeonghyeon Lee
 * @deprecated Use {@link AbstractJdbcConfiguration} instead.
 */
@Configuration
@Deprecated
public class JdbcConfiguration {

	/**
	 * Register a {@link RelationalMappingContext} and apply an optional {@link NamingStrategy}.
	 *
	 * @param namingStrategy optional {@link NamingStrategy}. Use {@link NamingStrategy#INSTANCE} as fallback.
	 * @return must not be {@literal null}.
	 */
	@Bean
	public RelationalMappingContext jdbcMappingContext(Optional<NamingStrategy> namingStrategy) {

		JdbcMappingContext mappingContext = new JdbcMappingContext(namingStrategy.orElse(NamingStrategy.INSTANCE));
		mappingContext.setSimpleTypeHolder(jdbcCustomConversions().getSimpleTypeHolder());

		return mappingContext;
	}

	/**
	 * Creates a {@link RelationalConverter} using the configured {@link #jdbcMappingContext(Optional)}. Will get
	 * {@link #jdbcCustomConversions()} applied.
	 *
	 * @see #jdbcMappingContext(Optional)
	 * @see #jdbcCustomConversions()
	 * @return must not be {@literal null}.
	 */
	@Bean
	public RelationalConverter relationalConverter(RelationalMappingContext mappingContext,
			@Lazy RelationResolver relationalResolver, Dialect dialect) {

		return new BasicJdbcConverter(mappingContext, relationalResolver, jdbcCustomConversions(),
				JdbcTypeFactory.unsupported(), dialect.getIdentifierProcessing());
	}

	/**
	 * Register custom {@link Converter}s in a {@link JdbcCustomConversions} object if required. These
	 * {@link JdbcCustomConversions} will be registered with the {@link #relationalConverter(RelationalMappingContext)}.
	 * Returns an empty {@link JdbcCustomConversions} instance by default.
	 *
	 * @return must not be {@literal null}.
	 */
	@Bean
	public JdbcCustomConversions jdbcCustomConversions() {
		return new JdbcCustomConversions();
	}

	/**
	 * Register a {@link JdbcAggregateTemplate} as a bean for easy use in applications that need a lower level of
	 * abstraction than the normal repository abstraction.
	 *
	 * @param publisher
	 * @param context
	 * @param converter
	 * @param operations
	 * @param dialect
	 * @return
	 */
	@Bean
	public JdbcAggregateOperations jdbcAggregateOperations(ApplicationEventPublisher publisher,
			RelationalMappingContext context, JdbcConverter converter, NamedParameterJdbcOperations operations,
			Dialect dialect) {
		return new JdbcAggregateTemplate(publisher, context, converter,
				dataAccessStrategy(context, converter, operations, dialect));
	}

	/**
	 * Register a {@link DataAccessStrategy} as a bean for reuse in the {@link JdbcAggregateOperations} and the
	 * {@link RelationalConverter}.
	 *
	 * @param context
	 * @param converter
	 * @param operations
	 * @param dialect
	 * @return
	 */
	@Bean
	public DataAccessStrategy dataAccessStrategy(RelationalMappingContext context, JdbcConverter converter,
			NamedParameterJdbcOperations operations, Dialect dialect) {
		return new DefaultDataAccessStrategy(new SqlGeneratorSource(context, converter, dialect), context, converter,
				operations);
	}

	@Bean
	public Dialect dialect() {
		return HsqlDbDialect.INSTANCE;
	}
}
