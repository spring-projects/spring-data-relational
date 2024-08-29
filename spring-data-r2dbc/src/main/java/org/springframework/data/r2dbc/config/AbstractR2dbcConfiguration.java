/*
 * Copyright 2018-2024 the original author or authors.
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
package org.springframework.data.r2dbc.config;

import io.r2dbc.spi.ConnectionFactory;

import java.util.ArrayList;
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
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.CustomConversions.StoreConversions;
import org.springframework.data.r2dbc.convert.MappingR2dbcConverter;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.convert.R2dbcCustomConversions;
import org.springframework.data.r2dbc.core.DefaultReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.dialect.DialectResolver;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.relational.RelationalManagedTypes;
import org.springframework.data.relational.core.mapping.DefaultNamingStrategy;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.util.TypeScanner;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Base class for Spring Data R2DBC configuration containing bean declarations that must be registered for Spring Data
 * R2DBC to work.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @see ConnectionFactory
 * @see DatabaseClient
 * @see org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories
 */
@Configuration(proxyBeanMethods = false)
public abstract class AbstractR2dbcConfiguration implements ApplicationContextAware {

	private static final String CONNECTION_FACTORY_BEAN_NAME = "connectionFactory";

	private @Nullable ApplicationContext context;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.context = applicationContext;
	}

	/**
	 * Return a R2DBC {@link ConnectionFactory}. Annotate with {@link Bean} in case you want to expose a
	 * {@link ConnectionFactory} instance to the {@link org.springframework.context.ApplicationContext}.
	 *
	 * @return the configured {@link ConnectionFactory}.
	 */
	public abstract ConnectionFactory connectionFactory();

	/**
	 * Returns the base packages to scan for R2DBC mapped entities at startup. Returns the package name of the
	 * configuration class' (the concrete class, not this one here) by default. So if you have a
	 * {@code com.acme.AppConfig} extending {@link AbstractR2dbcConfiguration} the base package will be considered
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
	public RelationalManagedTypes r2dbcManagedTypes() throws ClassNotFoundException {
		return RelationalManagedTypes.fromIterable(getInitialEntitySet());
	}

	/**
	 * Return a {@link R2dbcDialect} for the given {@link ConnectionFactory}. This method attempts to resolve a
	 * {@link R2dbcDialect} from {@link io.r2dbc.spi.ConnectionFactoryMetadata}. Override this method to specify a dialect
	 * instead of attempting to resolve one.
	 *
	 * @param connectionFactory the configured {@link ConnectionFactory}.
	 * @return the resolved {@link R2dbcDialect}.
	 * @throws org.springframework.data.r2dbc.dialect.DialectResolver.NoDialectException if the {@link R2dbcDialect}
	 *           cannot be determined.
	 */
	public R2dbcDialect getDialect(ConnectionFactory connectionFactory) {
		return DialectResolver.getDialect(connectionFactory);
	}

	/**
	 * Register a {@link DatabaseClient} using {@link #connectionFactory()} and {@link ReactiveDataAccessStrategy}.
	 *
	 * @return must not be {@literal null}.
	 * @throws IllegalArgumentException if any of the required args is {@literal null}.
	 */
	@Bean({ "r2dbcDatabaseClient", "databaseClient" })
	public DatabaseClient databaseClient() {

		ConnectionFactory connectionFactory = lookupConnectionFactory();

		return DatabaseClient.builder() //
				.connectionFactory(connectionFactory) //
				.bindMarkers(getDialect(connectionFactory).getBindMarkersFactory()) //
				.build();
	}

	/**
	 * Register {@link R2dbcEntityTemplate} using {@link #databaseClient()} and {@link #connectionFactory()}.
	 *
	 * @param databaseClient must not be {@literal null}.
	 * @param dataAccessStrategy must not be {@literal null}.
	 * @return
	 * @since 1.2
	 */
	@Bean
	public R2dbcEntityTemplate r2dbcEntityTemplate(DatabaseClient databaseClient,
			ReactiveDataAccessStrategy dataAccessStrategy) {

		Assert.notNull(databaseClient, "DatabaseClient must not be null");
		Assert.notNull(dataAccessStrategy, "ReactiveDataAccessStrategy must not be null");

		return new R2dbcEntityTemplate(databaseClient, dataAccessStrategy);
	}

	/**
	 * Register a {@link R2dbcMappingContext} and apply an optional {@link NamingStrategy}.
	 *
	 * @param namingStrategy optional {@link NamingStrategy}. Use {@link DefaultNamingStrategy#INSTANCE} as fallback.
	 * @param r2dbcCustomConversions customized R2DBC conversions.
	 * @param r2dbcManagedTypes R2DBC managed types, typically discovered through {@link #r2dbcManagedTypes() an entity
	 *          scan}.
	 * @return must not be {@literal null}.
	 * @throws IllegalArgumentException if any of the required args is {@literal null}.
	 */
	@Bean
	public R2dbcMappingContext r2dbcMappingContext(Optional<NamingStrategy> namingStrategy,
			R2dbcCustomConversions r2dbcCustomConversions, RelationalManagedTypes r2dbcManagedTypes) {

		Assert.notNull(namingStrategy, "NamingStrategy must not be null");

		R2dbcMappingContext context = new R2dbcMappingContext(namingStrategy.orElse(DefaultNamingStrategy.INSTANCE));
		context.setSimpleTypeHolder(r2dbcCustomConversions.getSimpleTypeHolder());
		context.setManagedTypes(r2dbcManagedTypes);

		return context;
	}

	/**
	 * Creates a {@link ReactiveDataAccessStrategy} using the configured
	 * {@link #r2dbcConverter(R2dbcMappingContext, R2dbcCustomConversions) R2dbcConverter}.
	 *
	 * @param converter the configured {@link R2dbcConverter}.
	 * @return must not be {@literal null}.
	 * @see #r2dbcConverter(R2dbcMappingContext, R2dbcCustomConversions)
	 * @see #getDialect(ConnectionFactory)
	 * @throws IllegalArgumentException if any of the {@literal mappingContext} is {@literal null}.
	 */
	@Bean
	public ReactiveDataAccessStrategy reactiveDataAccessStrategy(R2dbcConverter converter) {

		Assert.notNull(converter, "MappingContext must not be null");

		return new DefaultReactiveDataAccessStrategy(getDialect(lookupConnectionFactory()), converter);
	}

	/**
	 * Creates a {@link org.springframework.data.r2dbc.convert.R2dbcConverter} using the configured
	 * {@link #r2dbcMappingContext(Optional, R2dbcCustomConversions, RelationalManagedTypes)} R2dbcMappingContext}.
	 *
	 * @param mappingContext the configured {@link R2dbcMappingContext}.
	 * @param r2dbcCustomConversions customized R2DBC conversions.
	 * @return must not be {@literal null}.
	 * @see #r2dbcMappingContext(Optional, R2dbcCustomConversions, RelationalManagedTypes)
	 * @see #getDialect(ConnectionFactory)
	 * @throws IllegalArgumentException if any of the {@literal mappingContext} is {@literal null}.
	 * @since 1.2
	 */
	@Bean
	public MappingR2dbcConverter r2dbcConverter(R2dbcMappingContext mappingContext,
			R2dbcCustomConversions r2dbcCustomConversions) {

		Assert.notNull(mappingContext, "MappingContext must not be null");

		return new MappingR2dbcConverter(mappingContext, r2dbcCustomConversions);
	}

	/**
	 * Register custom {@link Converter}s in a {@link CustomConversions} object if required. These
	 * {@link CustomConversions} will be registered with the {@link MappingR2dbcConverter} and
	 * {@link #r2dbcMappingContext(Optional, R2dbcCustomConversions, RelationalManagedTypes)}. Returns an empty
	 * {@link R2dbcCustomConversions} instance by default. Override {@link #getCustomConverters()} to supply custom
	 * converters.
	 *
	 * @return must not be {@literal null}.
	 * @see #getCustomConverters()
	 */
	@Bean
	public R2dbcCustomConversions r2dbcCustomConversions() {
		return new R2dbcCustomConversions(getStoreConversions(), getCustomConverters());
	}

	/**
	 * Customization hook to return custom converters.
	 *
	 * @return return custom converters.
	 */
	protected List<Object> getCustomConverters() {
		return Collections.emptyList();
	}

	/**
	 * Returns the {@link R2dbcDialect}-specific {@link StoreConversions}.
	 *
	 * @return the {@link R2dbcDialect}-specific {@link StoreConversions}.
	 */
	protected StoreConversions getStoreConversions() {

		R2dbcDialect dialect = getDialect(lookupConnectionFactory());

		List<Object> converters = new ArrayList<>(dialect.getConverters());
		converters.addAll(R2dbcCustomConversions.STORE_CONVERTERS);

		return StoreConversions.of(dialect.getSimpleTypeHolder(), converters);
	}

	ConnectionFactory lookupConnectionFactory() {

		ApplicationContext context = this.context;
		Assert.notNull(context, "ApplicationContext is not yet initialized");

		String[] beanNamesForType = context.getBeanNamesForType(ConnectionFactory.class);

		for (String beanName : beanNamesForType) {

			if (beanName.equals(CONNECTION_FACTORY_BEAN_NAME)) {
				return context.getBean(CONNECTION_FACTORY_BEAN_NAME, ConnectionFactory.class);
			}
		}

		return connectionFactory();
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
	 * Scans the given base package for entities, i.e. R2DBC-specific types annotated with {@link Table}.
	 *
	 * @param basePackage must not be {@literal null}.
	 * @return a set of classes identified as entities.
	 * @since 3.0
	 */
	protected Set<Class<?>> scanForEntities(String basePackage) {

		if (!StringUtils.hasText(basePackage)) {
			return Collections.emptySet();
		}

		return TypeScanner.typeScanner(AbstractR2dbcConfiguration.class.getClassLoader()) //
				.forTypesAnnotatedWith(Table.class) //
				.scanPackages(basePackage) //
				.collectAsSet();
	}
}
