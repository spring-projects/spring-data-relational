/*
 * Copyright 2017-2018 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Supplier;

import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.data.jdbc.core.DataAccessStrategy;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactoryBean;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;
import org.springframework.data.repository.config.RepositoryConfigurationSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * {@link org.springframework.data.repository.config.RepositoryConfigurationExtension} extending the repository
 * registration process by registering JDBC repositories.
 *
 * @author Jens Schauder
 * @author Fei Dong
 */
public class JdbcRepositoryConfigExtension extends RepositoryConfigurationExtensionSupport {

	private ListableBeanFactory beanFactory;

	/*
	* (non-Javadoc)
	* @see org.springframework.data.repository.config.RepositoryConfigurationExtension#getModuleName()
	 */
	@Override
	public String getModuleName() {
		return "JDBC";
	}

	/*
	* (non-Javadoc)
	* @see org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport#getRepositoryFactoryBeanClassName()
	 */
	@Override
	public String getRepositoryFactoryBeanClassName() {
		return JdbcRepositoryFactoryBean.class.getName();
	}

	/*
	* (non-Javadoc)
	* @see org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport#getModulePrefix()
	 */
	@Override
	protected String getModulePrefix() {
		return getModuleName().toLowerCase(Locale.US);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.config.RepositoryConfigurationExtension#registerBeansForRoot(org.springframework.beans.factory.support.BeanDefinitionRegistry, org.springframework.data.repository.config.RepositoryConfigurationSource)
	 */
	public void registerBeansForRoot(BeanDefinitionRegistry registry, RepositoryConfigurationSource configurationSource) {

		if (registry instanceof ListableBeanFactory) {
			this.beanFactory = (ListableBeanFactory) registry;
		}
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport#postProcess(org.springframework.beans.factory.support.BeanDefinitionBuilder, org.springframework.data.repository.config.RepositoryConfigurationSource)
	 */
	@Override
	public void postProcess(BeanDefinitionBuilder builder, RepositoryConfigurationSource source) {

		resolveReference(builder, source, "jdbcOperationsRef", "jdbcOperations", NamedParameterJdbcOperations.class, true);
		resolveReference(builder, source, "dataAccessStrategyRef", "dataAccessStrategy", DataAccessStrategy.class, false);
	}

	private void resolveReference(BeanDefinitionBuilder builder, RepositoryConfigurationSource source,
			String attributeName, String propertyName, Class<?> classRef, boolean required) {

		Optional<String> beanNameRef = source.getAttribute(attributeName).filter(StringUtils::hasText);

		String beanName = beanNameRef.orElseGet(() -> determineMatchingBeanName(propertyName, classRef, required));

		if (beanName != null) {
			builder.addPropertyReference(propertyName, beanName);
		} else {
			Assert.isTrue(!required,
					"The beanName must not be null when requested as 'required'. Please report this as a bug.");
		}

	}

	@Nullable
	private String determineMatchingBeanName(String propertyName, Class<?> classRef, boolean required) {

		if (this.beanFactory == null) {
			return nullOrThrowException(required,
					() -> new NoSuchBeanDefinitionException(classRef, "No BeanFactory available."));
		}

		List<String> beanNames = Arrays.asList(beanFactory.getBeanNamesForType(classRef));

		if (beanNames.isEmpty()) {
			return nullOrThrowException(required,
					() -> new NoSuchBeanDefinitionException(classRef, String.format("No bean of type %s available", classRef)));
		}

		if (beanNames.size() == 1) {
			return beanNames.get(0);
		}

		if (!(beanFactory instanceof ConfigurableListableBeanFactory)) {

			return nullOrThrowException(required,
					() -> new NoSuchBeanDefinitionException(String.format(
							"BeanFactory does not implement ConfigurableListableBeanFactory when trying to find bean of type %s.",
							classRef)));
		}

		List<String> primaryBeanNames = getPrimaryBeanDefinitions(beanNames, (ConfigurableListableBeanFactory) beanFactory);

		if (primaryBeanNames.size() == 1) {
			return primaryBeanNames.get(0);
		}

		if (primaryBeanNames.size() > 1) {
			throw new NoUniqueBeanDefinitionException(classRef, primaryBeanNames.size(),
					"more than one 'primary' bean found among candidates: " + primaryBeanNames);
		}

		for (String beanName : beanNames) {

			if (propertyName.equals(beanName)
					|| ObjectUtils.containsElement(beanFactory.getAliases(beanName), propertyName)) {
				return beanName;
			}
		}

		return nullOrThrowException(required,
				() -> new NoSuchBeanDefinitionException(String.format("No bean of name %s found.", propertyName)));
	}

	private static List<String> getPrimaryBeanDefinitions(List<String> beanNames,
			ConfigurableListableBeanFactory beanFactory) {

		ArrayList<String> primaryBeanNames = new ArrayList<>();
		for (String name : beanNames) {

			if (beanFactory.getBeanDefinition(name).isPrimary()) {
				primaryBeanNames.add(name);
			}
		}
		return primaryBeanNames;
	}

	@Nullable
	private static String nullOrThrowException(boolean required, Supplier<RuntimeException> exception) {

		if (required) {
			throw exception.get();
		}
		return null;
	}

}
