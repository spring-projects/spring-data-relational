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
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.data.jdbc.core.DataAccessStrategy;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactoryBean;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;
import org.springframework.data.repository.config.RepositoryConfigurationSource;
import org.springframework.util.StringUtils;

/**
 * {@link org.springframework.data.repository.config.RepositoryConfigurationExtension} extending the repository
 * registration process by registering JDBC repositories.
 *
 * @author Jens Schauder
 */
public class JdbcRepositoryConfigExtension extends RepositoryConfigurationExtensionSupport {

	private static final String DEFAULT_JDBC_OPERATION_BEAN_NAME = "namedParameterJdbcTemplate";

	
	private ListableBeanFactory listableBeanFactory;
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
		if (registry instanceof ListableBeanFactory) {
			this.listableBeanFactory =   (ListableBeanFactory) registry;
		}
	}


	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport#postProcess(org.springframework.beans.factory.support.BeanDefinitionBuilder, org.springframework.data.repository.config.RepositoryConfigurationSource)
	 */
	@Override
	public void postProcess(BeanDefinitionBuilder builder, RepositoryConfigurationSource source) {

		Optional<String> jdbcOperationRef = source.getAttribute("jdbcOperationsRef");
		builder.addPropertyReference("jdbcOperations", jdbcOperationRef.orElse(DEFAULT_JDBC_OPERATION_BEAN_NAME));
		Optional<String> dataAccessStrategyRef = source.getAttribute("dataAccessStrategyRef").filter(StringUtils::hasText);
		if (dataAccessStrategyRef.isPresent()) {
			builder.addPropertyReference("dataAccessStrategy", dataAccessStrategyRef.get());
		} else if(this.listableBeanFactory != null) {
			List<String> beanNames=Arrays.asList(listableBeanFactory.getBeanNamesForType(DataAccessStrategy.class));

			if (beanNames.size() > 1) {
				throw new NoUniqueBeanDefinitionException(DataAccessStrategy.class, beanNames);
			}

			if (!beanNames.isEmpty()) {
				builder.addPropertyReference("dataAccessStrategy", beanNames.get(0));
			}

		}
	}

}
