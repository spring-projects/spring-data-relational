/*
 * Copyright 2019-2022 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.JdbcAggregateTemplate;
import org.springframework.data.jdbc.core.convert.*;
import org.springframework.data.jdbc.core.dialect.JdbcArrayColumns;
import org.springframework.data.jdbc.core.dialect.JdbcDialect;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.core.mapping.JdbcSimpleTypes;
import org.springframework.data.mapping.model.SimpleTypeHolder;
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
 * @author Myeonghyeon Lee
 * @author Chirag Tailor
 * @since 1.1
 */
@Configuration(proxyBeanMethods = false)
public class AbstractJdbcConfiguration implements ApplicationContextAware {

	private static final Log LOG = LogFactory.getLog(AbstractJdbcConfiguration.class);

	private ApplicationContext applicationContext;

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
	 * {@link #jdbcMappingContext(Optional, JdbcCustomConversions)}. Will get {@link #jdbcCustomConversions()} ()}
	 * applied.
	 *
	 * @see #jdbcMappingContext(Optional, JdbcCustomConversions)
	 * @see #jdbcCustomConversions()
	 * @return must not be {@literal null}.
	 */
	@Bean
	public JdbcConverter jdbcConverter(JdbcMappingContext mappingContext, NamedParameterJdbcOperations operations,
			@Lazy RelationResolver relationResolver, JdbcCustomConversions conversions, Dialect dialect) {

		JdbcArrayColumns arrayColumns = dialect instanceof JdbcDialect ? ((JdbcDialect) dialect).getArraySupport()
				: JdbcArrayColumns.DefaultSupport.INSTANCE;
		DefaultJdbcTypeFactory jdbcTypeFactory = new DefaultJdbcTypeFactory(operations.getJdbcOperations(),
				arrayColumns);

		return new BasicJdbcConverter(mappingContext, relationResolver, conversions, jdbcTypeFactory,
				dialect.getIdentifierProcessing());
	}

	/**
	 * Register custom {@link Converter}s in a {@link JdbcCustomConversions} object if required. These
	 * {@link JdbcCustomConversions} will be registered with the
	 * {@link #jdbcConverter(JdbcMappingContext, NamedParameterJdbcOperations, RelationResolver, JdbcCustomConversions, Dialect)}.
	 * Returns an empty {@link JdbcCustomConversions} instance by default.
	 *
	 * @return will never be {@literal null}.
	 */
	@Bean
	public JdbcCustomConversions jdbcCustomConversions() {

		try {

			Dialect dialect = applicationContext.getBean(Dialect.class);
			SimpleTypeHolder simpleTypeHolder = dialect.simpleTypes().isEmpty() ? JdbcSimpleTypes.HOLDER
					: new SimpleTypeHolder(dialect.simpleTypes(), JdbcSimpleTypes.HOLDER);

			return new JdbcCustomConversions(
					CustomConversions.StoreConversions.of(simpleTypeHolder, storeConverters(dialect)), userConverters());

		} catch (NoSuchBeanDefinitionException exception) {

			LOG.warn("No dialect found. CustomConversions will be configured without dialect specific conversions.");

			return new JdbcCustomConversions();
		}
	}

	protected List<?> userConverters() {
		return Collections.emptyList();
	}

	private List<Object> storeConverters(Dialect dialect) {

		List<Object> converters = new ArrayList<>();
		converters.addAll(dialect.getConverters());
		converters.addAll(JdbcCustomConversions.storeConverters());
		return converters;
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
				jdbcConverter, operations, new SqlParametersFactory(context, jdbcConverter, dialect),
				new InsertStrategyFactory(operations, new BatchJdbcOperations(operations.getJdbcOperations()), dialect));
	}

	/**
	 * Resolves a {@link Dialect JDBC dialect} by inspecting {@link NamedParameterJdbcOperations}.
	 *
	 * @param operations the {@link NamedParameterJdbcOperations} allowing access to a {@link java.sql.Connection}.
	 * @return the {@link Dialect} to be used.
	 * @since 2.0
	 * @throws org.springframework.data.jdbc.repository.config.DialectResolver.NoDialectException if the {@link Dialect}
	 *           cannot be determined.
	 */
	@Bean
	public Dialect jdbcDialect(NamedParameterJdbcOperations operations) {
		return DialectResolver.getDialect(operations.getJdbcOperations());
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}
}
