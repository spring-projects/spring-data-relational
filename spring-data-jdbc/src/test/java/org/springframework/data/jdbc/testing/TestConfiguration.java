/*
 * Copyright 2017-2024 the original author or authors.
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
package org.springframework.data.jdbc.testing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.jdbc.core.convert.*;
import org.springframework.data.jdbc.core.dialect.JdbcDialect;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.core.mapping.JdbcSimpleTypes;
import org.springframework.data.jdbc.repository.config.DialectResolver;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.DefaultNamingStrategy;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.query.ExtensionAwareQueryMethodEvaluationContextProvider;
import org.springframework.data.spel.spi.EvaluationContextExtension;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Infrastructure configuration for integration tests.
 *
 * @author Oliver Gierke
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Fei Dong
 * @author Myeonghyeon Lee
 * @author Christoph Strobl
 * @author Chirag Tailor
 * @author Christopher Klein
 */
@Configuration
@ComponentScan // To pick up configuration classes (per activated profile)
public class TestConfiguration {

	public static final String PROFILE_SINGLE_QUERY_LOADING = "singleQueryLoading";
	public static final String PROFILE_NO_SINGLE_QUERY_LOADING = "!" + PROFILE_SINGLE_QUERY_LOADING;

	@Autowired DataSource dataSource;
	@Autowired BeanFactory beanFactory;
	@Autowired ApplicationEventPublisher publisher;
	@Autowired(required = false) SqlSessionFactory sqlSessionFactory;

	@Bean
	JdbcRepositoryFactory jdbcRepositoryFactory(
			@Qualifier("defaultDataAccessStrategy") DataAccessStrategy dataAccessStrategy, RelationalMappingContext context,
			Dialect dialect, JdbcConverter converter, Optional<List<NamedQueries>> namedQueries,
			List<EvaluationContextExtension> evaulationContextExtensions) {

		JdbcRepositoryFactory factory = new JdbcRepositoryFactory(dataAccessStrategy, context, converter, dialect,
				publisher, namedParameterJdbcTemplate());
		namedQueries.map(it -> it.iterator().next()).ifPresent(factory::setNamedQueries);

		factory.setEvaluationContextProvider(
				new ExtensionAwareQueryMethodEvaluationContextProvider(evaulationContextExtensions));
		return factory;
	}

	@Bean
	@Primary
	NamedParameterJdbcOperations namedParameterJdbcTemplate() {
		return new NamedParameterJdbcTemplate(dataSource);
	}

	@Bean
	PlatformTransactionManager transactionManager() {
		return new DataSourceTransactionManager(dataSource);
	}

	@Bean
	DataAccessStrategy defaultDataAccessStrategy(
			@Qualifier("namedParameterJdbcTemplate") NamedParameterJdbcOperations template, RelationalMappingContext context,
			JdbcConverter converter, Dialect dialect) {

		return new DataAccessStrategyFactory(new SqlGeneratorSource(context, converter, dialect), converter,
				template, new SqlParametersFactory(context, converter),
				new InsertStrategyFactory(template, dialect)).create();
	}

	@Bean("jdbcMappingContext")
	@Profile(PROFILE_NO_SINGLE_QUERY_LOADING)
	JdbcMappingContext jdbcMappingContextWithOutSingleQueryLoading(Optional<NamingStrategy> namingStrategy, CustomConversions conversions) {

		JdbcMappingContext mappingContext = new JdbcMappingContext(namingStrategy.orElse(DefaultNamingStrategy.INSTANCE));
		mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		return mappingContext;
	}
	@Bean("jdbcMappingContext")
	@Profile(PROFILE_SINGLE_QUERY_LOADING)
	JdbcMappingContext jdbcMappingContextWithSingleQueryLoading(Optional<NamingStrategy> namingStrategy, CustomConversions conversions) {

		JdbcMappingContext mappingContext = new JdbcMappingContext(namingStrategy.orElse(DefaultNamingStrategy.INSTANCE));
		mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		mappingContext.setSingleQueryLoadingEnabled(true);
		return mappingContext;
	}

	@Bean
	CustomConversions jdbcCustomConversions(Dialect dialect) {

		SimpleTypeHolder simpleTypeHolder = dialect.simpleTypes().isEmpty() ? JdbcSimpleTypes.HOLDER
				: new SimpleTypeHolder(dialect.simpleTypes(), JdbcSimpleTypes.HOLDER);

		return new JdbcCustomConversions(CustomConversions.StoreConversions.of(simpleTypeHolder, storeConverters(dialect)),
				Collections.emptyList());
	}

	private List<Object> storeConverters(Dialect dialect) {

		List<Object> converters = new ArrayList<>();
		converters.addAll(dialect.getConverters());
		converters.addAll(JdbcCustomConversions.storeConverters());
		return converters;
	}

	@Bean
	JdbcConverter relationalConverter(RelationalMappingContext mappingContext, @Lazy RelationResolver relationResolver,
			CustomConversions conversions, @Qualifier("namedParameterJdbcTemplate") NamedParameterJdbcOperations template,
			Dialect dialect) {

		JdbcArrayColumns arrayColumns = dialect instanceof JdbcDialect ? ((JdbcDialect) dialect).getArraySupport()
				: JdbcArrayColumns.DefaultSupport.INSTANCE;

		return new MappingJdbcConverter( //
				mappingContext, //
				relationResolver, //
				conversions, //
				new DefaultJdbcTypeFactory(template.getJdbcOperations(), arrayColumns));
	}

	@Bean
	Dialect jdbcDialect(NamedParameterJdbcOperations operations) {
		return DialectResolver.getDialect(operations.getJdbcOperations());
	}

	@Lazy
	@Bean
	TestDatabaseFeatures features(NamedParameterJdbcOperations operations) {
		return new TestDatabaseFeatures(operations.getJdbcOperations());
	}
}
