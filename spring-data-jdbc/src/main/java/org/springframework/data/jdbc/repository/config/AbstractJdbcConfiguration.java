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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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
import org.springframework.data.jdbc.core.dialect.JdbcDialect;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.core.mapping.JdbcSimpleTypes;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.RelationalManagedTypes;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.DefaultNamingStrategy;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.util.TypeScanner;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.util.StringUtils;

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
		return Collections.singleton(mappingBasePackage == null ? null : mappingBasePackage.getName());
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

		JdbcMappingContext mappingContext = new JdbcMappingContext(namingStrategy.orElse(DefaultNamingStrategy.INSTANCE));
		mappingContext.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());
		mappingContext.setManagedTypes(jdbcManagedTypes);

		return mappingContext;
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
			@Lazy RelationResolver relationResolver, JdbcCustomConversions conversions, Dialect dialect) {

		JdbcArrayColumns arrayColumns = dialect instanceof JdbcDialect ? ((JdbcDialect) dialect).getArraySupport()
				: JdbcArrayColumns.DefaultSupport.INSTANCE;
		DefaultJdbcTypeFactory jdbcTypeFactory = new DefaultJdbcTypeFactory(operations.getJdbcOperations(), arrayColumns);

		return new MappingJdbcConverter(mappingContext, relationResolver, conversions, jdbcTypeFactory);
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

			LOG.warn("No dialect found; CustomConversions will be configured without dialect specific conversions");

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

		SqlGeneratorSource sqlGeneratorSource = new SqlGeneratorSource(context, jdbcConverter, dialect);
		DataAccessStrategyFactory factory = new DataAccessStrategyFactory(sqlGeneratorSource, jdbcConverter, operations,
				new SqlParametersFactory(context, jdbcConverter),
				new InsertStrategyFactory(operations, dialect));

		return factory.create();
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
	@SuppressWarnings("unchecked")
	protected Set<Class<?>> scanForEntities(String basePackage) {

		if (!StringUtils.hasText(basePackage)) {
			return Collections.emptySet();
		}

		return TypeScanner.typeScanner(AbstractJdbcConfiguration.class.getClassLoader()) //
				.forTypesAnnotatedWith(Table.class) //
				.scanPackages(basePackage) //
				.collectAsSet();
	}
}
