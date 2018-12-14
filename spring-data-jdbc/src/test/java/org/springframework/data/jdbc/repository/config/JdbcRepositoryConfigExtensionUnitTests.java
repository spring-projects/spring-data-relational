/*
 * Copyright 2018 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.repository.config.RepositoryConfigurationSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * @author Jens Schauder
 */
public class JdbcRepositoryConfigExtensionUnitTests {

	BeanDefinitionBuilder definitionBuilder = BeanDefinitionBuilder.genericBeanDefinition();
	RepositoryConfigurationSource configSource = mock(RepositoryConfigurationSource.class);
	DefaultListableBeanFactory listableBeanFactory = new DefaultListableBeanFactory();

	JdbcRepositoryConfigExtension extension = new JdbcRepositoryConfigExtension();

	@Test // DATAJDBC-293
	public void exceptionIsThrownOnPostProcessIfNoBeanFactoryIsAvailable() {

		assertThatThrownBy( //
				() -> extension.postProcess(definitionBuilder, configSource)) //
						.isInstanceOf(NoSuchBeanDefinitionException.class) //
						.hasMessageContaining("No BeanFactory");

	}

	@Test // DATAJDBC-293
	public void exceptionIsThrownOnPostProcessIfNoJdbcOperationsBeanIsAvailable() {

		extension.registerBeansForRoot(listableBeanFactory, null);

		assertThatThrownBy( //
				() -> extension.postProcess(definitionBuilder, configSource)) //
						.isInstanceOf(NoSuchBeanDefinitionException.class) //
						.hasMessageContaining("NamedParameterJdbcOperations"); //

	}

	@Test // DATAJDBC-293
	public void exceptionIsThrownOnPostProcessIfMultipleJdbcOperationsBeansAreAvailableAndNoConfigurableBeanFactoryAvailable() {

		GenericApplicationContext applicationContext = new GenericApplicationContext();

		applicationContext.registerBean( //
				"one", //
				NamedParameterJdbcOperations.class, //
				() -> mock(NamedParameterJdbcOperations.class));
		applicationContext.registerBean( //
				"two", //
				NamedParameterJdbcOperations.class, //
				() -> mock(NamedParameterJdbcOperations.class));

		applicationContext.refresh();

		extension.registerBeansForRoot(applicationContext, null);

		assertThatThrownBy( //
				() -> extension.postProcess(definitionBuilder, configSource)) //
						.isInstanceOf(NoSuchBeanDefinitionException.class) //
						.hasMessageContaining("NamedParameterJdbcOperations"); //

	}

	@Test // DATAJDBC-293
	public void exceptionIsThrownOnPostProcessIfMultiplePrimaryNoJdbcOperationsBeansAreAvailable() {

		registerJdbcOperations("one", true);
		registerJdbcOperations("two", true);

		extension.registerBeansForRoot(listableBeanFactory, null);

		assertThatThrownBy( //
				() -> extension.postProcess(definitionBuilder, configSource)) //
						.isInstanceOf(NoSuchBeanDefinitionException.class) //
						.hasMessageContaining("NamedParameterJdbcOperations"); //

	}

	@Test // DATAJDBC-293
	public void uniquePrimaryBeanIsUsedOfNamedParameterJdbcOperations() {

		registerJdbcOperations("one", false);
		registerJdbcOperations("two", true);

		extension.registerBeansForRoot(listableBeanFactory, null);

		extension.postProcess(definitionBuilder, configSource);

		Object jdbcOperations = definitionBuilder.getBeanDefinition().getPropertyValues().get("jdbcOperations");

		assertThat(jdbcOperations) //
				.isInstanceOf(RuntimeBeanReference.class) //
				.extracting(rbr -> ((RuntimeBeanReference) rbr).getBeanName()).contains("two");

		System.out.println(jdbcOperations);
	}

	@Test // DATAJDBC-293
	public void matchesByNameAsLastResort() {

		registerJdbcOperations("jdbcOperations", false);
		registerJdbcOperations("two", false);

		extension.registerBeansForRoot(listableBeanFactory, null);

		extension.postProcess(definitionBuilder, configSource);

		Object jdbcOperations = definitionBuilder.getBeanDefinition().getPropertyValues().get("jdbcOperations");

		assertThat(jdbcOperations) //
				.isInstanceOf(RuntimeBeanReference.class) //
				.extracting(rbr -> ((RuntimeBeanReference) rbr).getBeanName()).contains("jdbcOperations");

	}

	private void registerJdbcOperations(String name, boolean primary) {

		listableBeanFactory.registerBeanDefinition(name, BeanDefinitionBuilder.genericBeanDefinition( //
				NamedParameterJdbcOperations.class, //
				() -> mock(NamedParameterJdbcOperations.class)) //
				.applyCustomizers(bd -> bd.setPrimary(primary)) //
				.getBeanDefinition());
	}
}
