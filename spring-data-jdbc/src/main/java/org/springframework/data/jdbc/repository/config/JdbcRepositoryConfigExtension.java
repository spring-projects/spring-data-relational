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

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.data.jdbc.core.DataAccessStrategy;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactoryBean;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;
import org.springframework.data.repository.config.RepositoryConfigurationSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
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

	private ConfigurableListableBeanFactory listableBeanFactory;
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
	public void registerBeansForRoot(BeanDefinitionRegistry registry,
			RepositoryConfigurationSource configurationSource) {
		if (registry instanceof ConfigurableListableBeanFactory) {
			this.listableBeanFactory =   (ConfigurableListableBeanFactory) registry;
		}
	}


	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport#postProcess(org.springframework.beans.factory.support.BeanDefinitionBuilder, org.springframework.data.repository.config.RepositoryConfigurationSource)
	 */
	@Override
	public void postProcess(BeanDefinitionBuilder builder, RepositoryConfigurationSource source) {
		resolveReference(builder, source, "jdbcOperationsRef", "jdbcOperations", NamedParameterJdbcOperations.class,
				true);
		resolveReference(builder, source, "dataAccessStrategyRef", "dataAccessStrategy", DataAccessStrategy.class,
				false);
	}

	private void resolveReference(BeanDefinitionBuilder builder, RepositoryConfigurationSource source,
			String attributeName, String propertyName, Class<?> classRef, boolean required) {
		Optional<String> beanNameRef = source.getAttribute(attributeName).filter(StringUtils::hasText);

		String beanName = beanNameRef.orElseGet(() -> {
			if (this.listableBeanFactory != null) {
				List<String> beanNames = Arrays.asList(listableBeanFactory.getBeanNamesForType(classRef));
				Map<String, BeanDefinition> bdMap = beanNames.stream()
						.collect(Collectors.toMap(Function.identity(), listableBeanFactory::getBeanDefinition));

				if (beanNames.size() > 1) {
					// determine primary

					Map<String, BeanDefinition> primaryBdMap = bdMap.entrySet().stream()
							.filter(e -> e.getValue().isPrimary())
							.collect(Collectors.toMap(Entry::getKey, Entry::getValue));

					Optional<String> primaryBeanName = getSingleBeanName(primaryBdMap.keySet(), classRef,
							() -> "more than one 'primary' bean found among candidates: " + primaryBdMap.keySet());

					// In Java 11 should use Optional.or()
					if (primaryBeanName.isPresent()) {
						return primaryBeanName.get();
					}

					// determine matchesBeanName

					Optional<String> matchesBeanName = beanNames.stream()
							.filter(name -> propertyName.equals(name)
									|| ObjectUtils.containsElement(listableBeanFactory.getAliases(name), propertyName))
							.findFirst();

					if (matchesBeanName.isPresent()) {
						return matchesBeanName.get();
					}

				}

				if (beanNames.size() == 1) {
					return beanNames.get(0);
				}
			}
			return null;
		});
		if (beanName != null) {
			builder.addPropertyReference(propertyName, beanName);
		} else if (required) {
			throw new NoSuchBeanDefinitionException(classRef);
		}
	}

	private Optional<String> getSingleBeanName(Collection<String> beanNames, Class<?> classRef,
			Supplier<String> errorMessage) {
		if (beanNames.size() > 1) {
			throw new NoUniqueBeanDefinitionException(classRef, beanNames.size(), errorMessage.get());
		}

		return beanNames.stream().findFirst();
	}
}
