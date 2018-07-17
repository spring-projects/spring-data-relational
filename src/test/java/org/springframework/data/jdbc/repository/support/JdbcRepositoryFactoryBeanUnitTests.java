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
package org.springframework.data.jdbc.repository.support;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.DataAccessStrategy;
import org.springframework.data.jdbc.core.DefaultDataAccessStrategy;
import org.springframework.data.jdbc.repository.RowMapperMap;
import org.springframework.data.relational.core.conversion.BasicRelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Tests the dependency injection for {@link JdbcRepositoryFactoryBean}.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class JdbcRepositoryFactoryBeanUnitTests {

	JdbcRepositoryFactoryBean<DummyEntityRepository, DummyEntity, Long> factoryBean;

	@Mock DataAccessStrategy dataAccessStrategy;
	@Mock ApplicationEventPublisher publisher;

	RelationalMappingContext mappingContext;

	@Before
	public void setUp() {

		this.mappingContext = new RelationalMappingContext();

		// Setup standard configuration
		factoryBean = new JdbcRepositoryFactoryBean<>(DummyEntityRepository.class);
	}

	@Test
	public void setsUpBasicInstanceCorrectly() {

		factoryBean.setDataAccessStrategy(dataAccessStrategy);
		factoryBean.setMappingContext(mappingContext);
		factoryBean.setConverter(new BasicRelationalConverter(mappingContext));
		factoryBean.setApplicationEventPublisher(publisher);
		factoryBean.afterPropertiesSet();

		assertThat(factoryBean.getObject()).isNotNull();
	}

	@Test(expected = IllegalArgumentException.class)
	public void requiresListableBeanFactory() {

		factoryBean.setBeanFactory(mock(BeanFactory.class));
	}

	@Test(expected = IllegalStateException.class) // DATAJDBC-155
	public void afterPropertiesThowsExceptionWhenNoMappingContextSet() {

		factoryBean.setMappingContext(null);
		factoryBean.setApplicationEventPublisher(publisher);
		factoryBean.afterPropertiesSet();
	}

	@Test // DATAJDBC-155
	public void afterPropertiesSetDefaultsNullablePropertiesCorrectly() {

		factoryBean.setMappingContext(mappingContext);
		factoryBean.setConverter(new BasicRelationalConverter(mappingContext));
		factoryBean.setApplicationEventPublisher(publisher);
		factoryBean.afterPropertiesSet();

		assertThat(factoryBean.getObject()).isNotNull();
		assertThat(ReflectionTestUtils.getField(factoryBean, "dataAccessStrategy"))
				.isInstanceOf(DefaultDataAccessStrategy.class);
		assertThat(ReflectionTestUtils.getField(factoryBean, "rowMapperMap")).isEqualTo(RowMapperMap.EMPTY);
	}

	private static class DummyEntity {
		@Id private Long id;
	}

	private interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {}
}
