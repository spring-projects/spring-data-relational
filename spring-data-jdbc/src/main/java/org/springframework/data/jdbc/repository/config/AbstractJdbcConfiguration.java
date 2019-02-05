/*
 * Copyright 2017-2019 the original author or authors.
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
package org.springframework.data.jdbc.repository.config;

import java.util.Optional;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.jdbc.core.DataAccessStrategy;
import org.springframework.data.jdbc.core.DefaultDataAccessStrategy;
import org.springframework.data.jdbc.core.JdbcAggregateTemplate;
import org.springframework.data.jdbc.core.SqlGeneratorSource;
import org.springframework.data.jdbc.core.convert.BasicJdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.conversion.RelationalConverter;
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
 * @since 1.1
 */
@Configuration
public abstract class AbstractJdbcConfiguration {

	/**
	 * Register a {@link RelationalMappingContext} and apply an optional {@link NamingStrategy}.
	 *
	 * @param namingStrategy optional {@link NamingStrategy}. Use {@link NamingStrategy#INSTANCE} as fallback.
	 * @return must not be {@literal null}.
	 */
	@Bean
	public JdbcMappingContext jdbcMappingContext(Optional<NamingStrategy> namingStrategy) {

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
	public JdbcConverter relationalConverter(RelationalMappingContext mappingContext) {
		return new BasicJdbcConverter(mappingContext, jdbcCustomConversions());
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
	 * @param publisher for publishing events. Must not be {@literal null}.
	 * @param context the mapping context to be used. Must not be {@literal null}.
	 * @param converter the conversions used when reading and writing from/to the database. Must not be {@literal null}.
	 * @param operations {@link NamedParameterJdbcOperations} used for accessing the database. Must not be
	 *          {@literal null}.
	 * @return a {@link JdbcAggregateTemplate}. Guaranteed to be not {@literal null}.
	 */
	@Bean
	public JdbcAggregateTemplate jdbcAggregateTemplate(ApplicationEventPublisher publisher,
			RelationalMappingContext context, RelationalConverter converter, NamedParameterJdbcOperations operations) {

		DataAccessStrategy dataAccessStrategy = new DefaultDataAccessStrategy(new SqlGeneratorSource(context), context,
				converter, operations);
		return new JdbcAggregateTemplate(publisher, context, converter, dataAccessStrategy);
	}
}
