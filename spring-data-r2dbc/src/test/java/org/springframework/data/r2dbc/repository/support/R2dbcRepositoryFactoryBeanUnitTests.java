/*
 * Copyright 2021-2024 the original author or authors.
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
package org.springframework.data.r2dbc.repository.support;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.dialect.H2Dialect;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.spel.EvaluationContextProvider;
import org.springframework.data.spel.ReactiveExtensionAwareEvaluationContextProvider;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Unit tests for {@link R2dbcRepositoryFactoryBean}.
 *
 * @author Mark Paluch
 */
class R2dbcRepositoryFactoryBeanUnitTests {

	@Test // #658
	void shouldConfigureReactiveExtensionAwareQueryMethodEvaluationContextProvider() {

		R2dbcRepositoryFactoryBean<PersonRepository, Person, String> factoryBean = new R2dbcRepositoryFactoryBean<>(
				PersonRepository.class);
		factoryBean.setBeanFactory(mock(ListableBeanFactory.class));
		R2dbcEntityTemplate operations = new R2dbcEntityTemplate(mock(DatabaseClient.class), H2Dialect.INSTANCE);
		factoryBean.setEntityOperations(operations);
		factoryBean.setLazyInit(true);
		factoryBean.afterPropertiesSet();

		Object factory = ReflectionTestUtils.getField(factoryBean, "factory");
		Object evaluationContextProvider = ReflectionTestUtils.getField(factory, "evaluationContextProvider");

		assertThat(evaluationContextProvider).isInstanceOf(ReactiveExtensionAwareEvaluationContextProvider.class)
				.isNotEqualTo(EvaluationContextProvider.DEFAULT);
	}

	static class Person {}

	interface PersonRepository extends R2dbcRepository<Person, String>

	{}
}
