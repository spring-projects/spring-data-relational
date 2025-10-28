/*
 * Copyright 2019-2025 the original author or authors.
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.JdbcAggregateTemplate;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.IdGeneratingEntityCallback;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.core.convert.QueryMappingConfiguration;
import org.springframework.data.jdbc.core.convert.RelationResolver;
import org.springframework.data.jdbc.core.dialect.DialectResolver;
import org.springframework.data.jdbc.core.dialect.JdbcDialect;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.RelationalManagedTypes;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.Table;
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
 * @author Mikhail Polivakha
 * @since 1.1
 */
@Configuration(proxyBeanMethods = false)
public class AbstractJdbcConfiguration implements ApplicationContextAware {

	@SuppressWarnings("NullAway.Init") private ApplicationContext applicationContext;

	private QueryMappingConfiguration queryMappingConfiguration = QueryMappingConfiguration.EMPTY;

	/**
	 * Returns the base packages to scan for JDBC mapped entities at startup. Returns the package name of the
	 * configuration class' (the concrete class, not this one here) by default. So if you have a
	 * {@code com.acme.AppConfig} extending {@link AbstractJdbcConfiguration} the base package will be considered
	 * {@code com.acme} unless the method is overridden to implement alternate behavior.
	 *
	 * @return the base packages to scan for mapped {@link Table} classes or an empty collection to not enable scanning
	 *         for entities.
	 * @since 3.0
	 */
	protected Collection<String> getMappingBasePackages() {

		Package mappingBasePackage = getClass().getPackage();
		return mappingBasePackage == null ? List.of() : List.of(mappingBasePackage.getName());
	}

	/**
	 * Returns the a {@link RelationalManagedTypes} object holding the initial entity set.
	 *
	 * @return new instance of {@link RelationalManagedTypes}.
	 * @throws ClassNotFoundException
	 * @since 3.0
	 */
	@Bean
	public RelationalManagedTypes jdbcManagedTypes() throws ClassNotFoundException {
		return RelationalManagedTypes.fromIterable(getInitialEntitySet());
	}

	/**
	 * Register a {@link JdbcMappingContext} and apply an optional {@link NamingStrategy}.
	 *
	 * @param namingStrategy optional {@link NamingStrategy}. Use
	 *          {@link org.springframework.data.relational.core.mapping.DefaultNamingStrategy#INSTANCE} as fallback.
	 * @param customConversions see {@link #jdbcCustomConversions()}.
	 * @param jdbcManagedTypes JDBC managed types, typically discovered through {@link #jdbcManagedTypes() an entity
	 *          scan}.
	 * @return must not be {@literal null}.
	 */
	@Bean
	public JdbcMappingContext jdbcMappingContext(Optional<NamingStrategy> namingStrategy,
			JdbcCustomConversions customConversions, RelationalManagedTypes jdbcManagedTypes) {
		return JdbcConfiguration.createMappingContext(jdbcManagedTypes, customConversions, namingStrategy.orElse(null));
	}

	/**
	 * Creates a {@link IdGeneratingEntityCallback} bean using the configured
	 * {@link #jdbcMappingContext(Optional, JdbcCustomConversions, RelationalManagedTypes)} and
	 * {@link #jdbcDialect(NamedParameterJdbcOperations)}.
	 *
	 * @return must not be {@literal null}.
	 * @since 3.5
	 */
	@Bean
	public IdGeneratingEntityCallback idGeneratingBeforeSaveCallback(JdbcMappingContext mappingContext,
			NamedParameterJdbcOperations operations, JdbcDialect dialect) {
		return new IdGeneratingEntityCallback(mappingContext, dialect, operations);
	}

	/**
	 * Creates a {@link RelationalConverter} using the configured
	 * {@link #jdbcMappingContext(Optional, JdbcCustomConversions, RelationalManagedTypes)}.
	 *
	 * @see #jdbcMappingContext(Optional, JdbcCustomConversions, RelationalManagedTypes)
	 * @see #jdbcCustomConversions()
	 * @return must not be {@literal null}.
	 */
	@Bean
	public JdbcConverter jdbcConverter(JdbcMappingContext mappingContext, NamedParameterJdbcOperations operations,
			@Lazy RelationResolver relationResolver, JdbcCustomConversions conversions, JdbcDialect dialect) {
		return JdbcConfiguration.createConverter(mappingContext, operations, relationResolver, conversions, dialect);
	}

	/**
	 * Register custom {@link Converter}s in a {@link JdbcCustomConversions} object if required. These
	 * {@link JdbcCustomConversions} will be registered with the
	 * {@link #jdbcConverter(JdbcMappingContext, NamedParameterJdbcOperations, RelationResolver, JdbcCustomConversions, JdbcDialect)}.
	 * Returns an empty {@link JdbcCustomConversions} instance by default.
	 *
	 * @return will never be {@literal null}.
	 */
	@Bean
	public JdbcCustomConversions jdbcCustomConversions() {

		JdbcDialect dialect = applicationContext.getBean(JdbcDialect.class);
		return JdbcConfiguration.createCustomConversions(dialect, userConverters());
	}

	protected List<?> userConverters() {
		return Collections.emptyList();
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
			JdbcMappingContext context, JdbcDialect dialect) {
		return JdbcConfiguration.createDataAccessStrategy(operations, jdbcConverter, queryMappingConfiguration, dialect);
	}

	/**
	 * Resolves a {@link Dialect JDBC dialect} by inspecting {@link NamedParameterJdbcOperations}.
	 *
	 * @param operations the {@link NamedParameterJdbcOperations} allowing access to a {@link java.sql.Connection}.
	 * @return the {@link Dialect} to be used.
	 * @since 2.0
	 * @throws DialectResolver.NoDialectException if the {@link Dialect} cannot be determined.
	 */
	@Bean
	public JdbcDialect jdbcDialect(NamedParameterJdbcOperations operations) {
		return DialectResolver.getDialect(operations.getJdbcOperations());
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public void setQueryMappingConfiguration(Optional<QueryMappingConfiguration> queryMappingConfiguration)
			throws BeansException {
		this.queryMappingConfiguration = queryMappingConfiguration.orElse(QueryMappingConfiguration.EMPTY);
	}

	/**
	 * Scans the mapping base package for classes annotated with {@link Table}. By default, it scans for entities in all
	 * packages returned by {@link #getMappingBasePackages()}.
	 *
	 * @see #getMappingBasePackages()
	 * @return
	 * @throws ClassNotFoundException
	 * @since 3.0
	 */
	protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {

		Set<Class<?>> initialEntitySet = new HashSet<>();

		for (String basePackage : getMappingBasePackages()) {
			initialEntitySet.addAll(scanForEntities(basePackage));
		}

		return initialEntitySet;
	}

	/**
	 * Scans the given base package for entities, i.e. JDBC-specific types annotated with {@link Table}.
	 *
	 * @param basePackage must not be {@literal null}.
	 * @return a set of classes identified as entities.
	 * @since 3.0
	 */
	protected Set<Class<?>> scanForEntities(String basePackage) {
		return JdbcConfiguration.scanForEntities(basePackage);
	}
}
