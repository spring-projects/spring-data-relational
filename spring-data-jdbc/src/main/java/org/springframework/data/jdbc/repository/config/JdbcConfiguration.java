/*
 * Copyright 2025 the original author or authors.
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
import java.util.Set;

import org.jspecify.annotations.Nullable;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.DataAccessStrategyFactory;
import org.springframework.data.jdbc.core.convert.DefaultJdbcTypeFactory;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.core.convert.MappingJdbcConverter;
import org.springframework.data.jdbc.core.convert.QueryMappingConfiguration;
import org.springframework.data.jdbc.core.convert.RelationResolver;
import org.springframework.data.jdbc.core.dialect.JdbcArrayColumns;
import org.springframework.data.jdbc.core.dialect.JdbcDialect;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.core.mapping.JdbcSimpleTypes;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.RelationalManagedTypes;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.DefaultNamingStrategy;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.util.TypeScanner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.util.StringUtils;

/**
 * Utility class to providing factory methods for JDBC infrastructure components.
 * <p>
 * Mainly for use within the framework or for configuration arrangements that require customization of configuration.
 *
 * @author Mark Paluch
 * @since 4.0
 */
public final class JdbcConfiguration {

	private JdbcConfiguration() {}

	/**
	 * Register custom {@link Converter}s in a {@link JdbcCustomConversions} object if required.
	 *
	 * @param dialect the JDBC dialect in use.
	 * @param userConverters list of user converters, must not be {@literal null}.
	 * @return will never be {@literal null}.
	 */
	public static JdbcCustomConversions createCustomConversions(JdbcDialect dialect, List<?> userConverters) {

		SimpleTypeHolder simpleTypeHolder = new SimpleTypeHolder(dialect.simpleTypes(), JdbcSimpleTypes.HOLDER);

		return new JdbcCustomConversions(CustomConversions.StoreConversions.of(simpleTypeHolder, storeConverters(dialect)),
				userConverters);
	}

	private static List<Object> storeConverters(Dialect dialect) {

		List<Object> converters = new ArrayList<>();
		converters.addAll(dialect.getConverters());
		converters.addAll(JdbcCustomConversions.storeConverters());
		return converters;
	}

	/**
	 * Register a {@link JdbcMappingContext} and apply an optional {@link NamingStrategy}.
	 *
	 * @param jdbcManagedTypes JDBC managed types.
	 * @param customConversions the custom conversions.
	 * @param namingStrategy optional {@link NamingStrategy}. Use {@link DefaultNamingStrategy#INSTANCE} as fallback.
	 * @return must not be {@literal null}.
	 */
	public static JdbcMappingContext createMappingContext(RelationalManagedTypes jdbcManagedTypes,
			JdbcCustomConversions customConversions, @Nullable NamingStrategy namingStrategy) {

		JdbcMappingContext mappingContext = JdbcMappingContext
				.forQuotedIdentifiers(namingStrategy != null ? namingStrategy : DefaultNamingStrategy.INSTANCE);
		mappingContext.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());
		mappingContext.setManagedTypes(jdbcManagedTypes);

		return mappingContext;
	}

	/**
	 * Creates a {@link JdbcConverter}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param relationResolver must not be {@literal null}.
	 * @param conversions must not be {@literal null}.
	 * @param dialect the JDBC dialect in use.
	 * @return must not be {@literal null}.
	 */
	public static JdbcConverter createConverter(JdbcMappingContext mappingContext,
			NamedParameterJdbcOperations operations, RelationResolver relationResolver, JdbcCustomConversions conversions,
			JdbcDialect dialect) {

		JdbcArrayColumns arrayColumns = JdbcDialect.getArraySupport(dialect);
		DefaultJdbcTypeFactory jdbcTypeFactory = new DefaultJdbcTypeFactory(operations.getJdbcOperations(), arrayColumns);

		MappingJdbcConverter mappingJdbcConverter = new MappingJdbcConverter(mappingContext, relationResolver, conversions,
				jdbcTypeFactory);

		if (operations.getJdbcOperations() instanceof JdbcTemplate jdbcTemplate) {
			mappingJdbcConverter.setExceptionTranslator(jdbcTemplate.getExceptionTranslator());
		}

		return mappingJdbcConverter;
	}

	/**
	 * Create a {@link DataAccessStrategy} for reuse in the {@link JdbcAggregateOperations} and the {@link JdbcConverter}.
	 * Override this method to register a bean of type {@link DataAccessStrategy} if your use case requires a more
	 * specialized {@link DataAccessStrategy}.
	 *
	 * @param operations must not be {@literal null}.
	 * @param jdbcConverter must not be {@literal null}.
	 * @param mappingConfiguration mapping configuration, can be {@literal null}.
	 * @param dialect the JDBC dialect in use.
	 * @return will never be {@literal null}.
	 */
	public static DataAccessStrategy createDataAccessStrategy(NamedParameterJdbcOperations operations,
			JdbcConverter jdbcConverter, @Nullable QueryMappingConfiguration mappingConfiguration, JdbcDialect dialect) {
		return new DataAccessStrategyFactory(jdbcConverter, operations, dialect,
				mappingConfiguration == null ? QueryMappingConfiguration.EMPTY : mappingConfiguration).create();
	}

	/**
	 * Scans the given base package for entities, i.e. JDBC-specific types annotated with {@link Table}.
	 *
	 * @param basePackage must not be {@literal null}.
	 * @return a set of classes identified as entities.
	 */
	@SuppressWarnings("unchecked")
	public static Set<Class<?>> scanForEntities(String basePackage) {

		if (!StringUtils.hasText(basePackage)) {
			return Collections.emptySet();
		}

		return TypeScanner.typeScanner(JdbcConfiguration.class.getClassLoader()) //
				.forTypesAnnotatedWith(Table.class) //
				.scanPackages(basePackage) //
				.collectAsSet();
	}

}
