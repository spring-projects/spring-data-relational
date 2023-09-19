/*
 * Copyright 2017-2023 the original author or authors.
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
package org.springframework.data.jdbc.repository.support;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.DefaultDataAccessStrategy;
import org.springframework.data.jdbc.core.convert.MappingJdbcConverter;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.repository.QueryMappingConfiguration;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Tests the dependency injection for {@link JdbcRepositoryFactoryBean}.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Evgeni Dimitrov
 */
@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
public class JdbcRepositoryFactoryBeanUnitTests {

	JdbcRepositoryFactoryBean<DummyEntityRepository, DummyEntity, Long> factoryBean;

	@Mock DataAccessStrategy dataAccessStrategy;
	@Mock ApplicationEventPublisher publisher;
	@Mock(answer = Answers.RETURNS_DEEP_STUBS) ListableBeanFactory beanFactory;
	@Mock Dialect dialect;

	RelationalMappingContext mappingContext;

	@BeforeEach
	public void setUp() {

		this.mappingContext = new JdbcMappingContext();

		// Setup standard configuration
		factoryBean = new JdbcRepositoryFactoryBean<>(DummyEntityRepository.class);

		when(beanFactory.getBean(NamedParameterJdbcOperations.class)).thenReturn(mock(NamedParameterJdbcOperations.class));

		ObjectProvider<DataAccessStrategy> provider = mock(ObjectProvider.class);
		when(beanFactory.getBeanProvider(DataAccessStrategy.class)).thenReturn(provider);
		when(provider.getIfAvailable(any()))
				.then((Answer<?>) invocation -> ((Supplier<?>) invocation.getArgument(0)).get());
	}

	@Test // DATAJDBC-151
	public void setsUpBasicInstanceCorrectly() {

		factoryBean.setDataAccessStrategy(dataAccessStrategy);
		factoryBean.setMappingContext(mappingContext);
		factoryBean.setConverter(new MappingJdbcConverter(mappingContext, dataAccessStrategy));
		factoryBean.setApplicationEventPublisher(publisher);
		factoryBean.setBeanFactory(beanFactory);
		factoryBean.setDialect(dialect);
		factoryBean.afterPropertiesSet();

		assertThat(factoryBean.getObject()).isNotNull();
	}

	@Test // DATAJDBC-151
	public void requiresListableBeanFactory() {

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> factoryBean.setBeanFactory(mock(BeanFactory.class)));
	}

	@Test // DATAJDBC-155
	public void afterPropertiesThrowsExceptionWhenNoMappingContextSet() {

		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> factoryBean.setMappingContext(null));
	}

	@Test // DATAJDBC-155
	public void afterPropertiesSetDefaultsNullablePropertiesCorrectly() {

		factoryBean.setMappingContext(mappingContext);
		factoryBean.setConverter(new MappingJdbcConverter(mappingContext, dataAccessStrategy));
		factoryBean.setApplicationEventPublisher(publisher);
		factoryBean.setBeanFactory(beanFactory);
		factoryBean.setDialect(dialect);
		factoryBean.afterPropertiesSet();

		assertThat(factoryBean.getObject()).isNotNull();
		assertThat(ReflectionTestUtils.getField(factoryBean, "dataAccessStrategy"))
				.isInstanceOf(DefaultDataAccessStrategy.class);
		assertThat(ReflectionTestUtils.getField(factoryBean, "queryMappingConfiguration"))
				.isEqualTo(QueryMappingConfiguration.EMPTY);
	}

	private static class DummyEntity {

		@Id private Long id;
	}

	private interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {}
}
