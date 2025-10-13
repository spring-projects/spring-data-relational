/*
 * Copyright 2017-2025 the original author or authors.
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

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.aot.BeanRegistrationAotProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionValidationException;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.core.dialect.JdbcDialect;
import org.springframework.data.jdbc.core.dialect.JdbcH2Dialect;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.repository.aot.JdbcRepositoryContributor;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactoryBean;
import org.springframework.data.jdbc.repository.support.SimpleJdbcRepository;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.DefaultNamingStrategy;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.aot.generate.RepositoryContributor;
import org.springframework.data.repository.config.AotRepositoryContext;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;
import org.springframework.data.repository.config.RepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryRegistrationAotProcessor;
import org.springframework.util.StringUtils;

/**
 * {@link org.springframework.data.repository.config.RepositoryConfigurationExtension} extending the repository
 * registration process by registering JDBC repositories.
 *
 * @author Jens Schauder
 * @author Fei Dong
 * @author Mark Paluch
 * @author Antoine Sauray
 * @author Tomohiko Ozawa
 */
public class JdbcRepositoryConfigExtension extends RepositoryConfigurationExtensionSupport {

	private static final String DEFAULT_TRANSACTION_MANAGER_BEAN_NAME = "transactionManager";
	private static final String ENABLE_DEFAULT_TRANSACTIONS_ATTRIBUTE = "enableDefaultTransactions";

	@Override
	public String getModuleName() {
		return "JDBC";
	}

	@Override
	public String getRepositoryBaseClassName() {
		return SimpleJdbcRepository.class.getName();
	}

	@Override
	public String getRepositoryFactoryBeanClassName() {
		return JdbcRepositoryFactoryBean.class.getName();
	}

	@Override
	protected String getModulePrefix() {
		return getModuleName().toLowerCase(Locale.US);
	}

	@Override
	public String getModuleIdentifier() {
		return getModulePrefix();
	}

	@Override
	public void postProcess(BeanDefinitionBuilder builder, RepositoryConfigurationSource source) {

		source.getAttribute(ENABLE_DEFAULT_TRANSACTIONS_ATTRIBUTE, Boolean.class)
				.ifPresent(it -> builder.addPropertyValue(ENABLE_DEFAULT_TRANSACTIONS_ATTRIBUTE, it));

		Optional<String> transactionManagerRef = source.getAttribute("transactionManagerRef");
		builder.addPropertyValue("transactionManager", transactionManagerRef.orElse(DEFAULT_TRANSACTION_MANAGER_BEAN_NAME));

		Optional<String> jdbcAggregateOperationsRef = source.getAttribute("jdbcAggregateOperationsRef")
				.filter(StringUtils::hasText);

		Optional<String> jdbcOperationsRef = source.getAttribute("jdbcOperationsRef") //
				.filter(StringUtils::hasText);
		Optional<String> dataAccessStrategyRef = source.getAttribute("dataAccessStrategyRef") //
				.filter(StringUtils::hasText);

		if (jdbcAggregateOperationsRef.isPresent()) {

			if (jdbcOperationsRef.isPresent() || dataAccessStrategyRef.isPresent()) {
				throw new BeanDefinitionValidationException(
						"Cannot set both 'jdbcAggregateOperations' and 'jdbcOperations' or 'dataAccessStrategy' in '@EnableJdbcRepositories' at '"
								+ source.getSource() + "'");
			}

			builder.addPropertyReference("jdbcAggregateOperations", jdbcAggregateOperationsRef.get());

		} else if (jdbcOperationsRef.isPresent() || dataAccessStrategyRef.isPresent()) {

			jdbcOperationsRef.ifPresent(s -> builder.addPropertyReference("jdbcOperations", s));
			dataAccessStrategyRef.ifPresent(s -> builder.addPropertyReference("dataAccessStrategy", s));

			builder.addPropertyValue("mappingContext", new RuntimeBeanReference(JdbcMappingContext.class));
			builder.addPropertyValue("dialect", new RuntimeBeanReference(Dialect.class));
			builder.addPropertyValue("converter", new RuntimeBeanReference(JdbcConverter.class));
		} else {
			builder.addPropertyValue("jdbcAggregateOperations", new RuntimeBeanReference(JdbcAggregateOperations.class));
		}
	}

	@Override
	public Class<? extends BeanRegistrationAotProcessor> getRepositoryAotProcessor() {
		return JdbcRepositoryRegistrationAotProcessor.class;
	}

	/**
	 * In strict mode only domain types having a {@link Table} annotation get a repository.
	 */
	@Override
	protected Collection<Class<? extends Annotation>> getIdentifyingAnnotations() {
		return Collections.singleton(Table.class);
	}

	/**
	 * A {@link RepositoryRegistrationAotProcessor} implementation that maintains aot repository setup.
	 *
	 * @since 3.0
	 */
	public static class JdbcRepositoryRegistrationAotProcessor extends RepositoryRegistrationAotProcessor {

		private static final String MODULE_NAME = "jdbc";

		@Override
		protected @Nullable RepositoryContributor contributeAotRepository(AotRepositoryContext repositoryContext) {

			if (!repositoryContext.isGeneratedRepositoriesEnabled(MODULE_NAME)) {
				return null;
			}

			ConfigurableListableBeanFactory beanFactory = repositoryContext.getBeanFactory();
			JdbcDialect dialect = beanFactory.getBeanProvider(JdbcDialect.class).getIfAvailable(() -> JdbcH2Dialect.INSTANCE);
			RelationalMappingContext mappingContext = beanFactory.getBeanProvider(RelationalMappingContext.class)
					.getIfAvailable(() -> {

						JdbcCustomConversions customConversions = beanFactory.getBeanProvider(JdbcCustomConversions.class)
								.getIfAvailable(() -> JdbcCustomConversions.of(dialect, Collections.emptyList()));

						NamingStrategy namingStrategy = beanFactory.getBeanProvider(NamingStrategy.class)
								.getIfAvailable(DefaultNamingStrategy::new);

						JdbcMappingContext context = new JdbcMappingContext(namingStrategy);
						context.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());

						return context;
					});

			return new JdbcRepositoryContributor(repositoryContext, dialect, mappingContext);
		}

	}

}
