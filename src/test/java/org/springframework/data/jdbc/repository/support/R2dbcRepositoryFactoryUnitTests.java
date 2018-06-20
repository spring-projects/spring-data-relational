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
package org.springframework.data.jdbc.repository.support;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.jdbc.core.function.DatabaseClient;
import org.springframework.data.jdbc.core.mapping.JdbcPersistentEntity;
import org.springframework.data.jdbc.repository.query.JdbcEntityInformation;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.Repository;

/**
 * Unit test for {@link R2dbcRepositoryFactory}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class R2dbcRepositoryFactoryUnitTests {

	@Mock DatabaseClient databaseClient;
	@Mock @SuppressWarnings("rawtypes") MappingContext mappingContext;
	@Mock @SuppressWarnings("rawtypes") JdbcPersistentEntity entity;

	@Before
	@SuppressWarnings("unchecked")
	public void before() {
		when(mappingContext.getRequiredPersistentEntity(Person.class)).thenReturn(entity);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void usesMappingJdbcEntityInformationIfMappingContextSet() {

		R2dbcRepositoryFactory factory = new R2dbcRepositoryFactory(databaseClient, mappingContext);
		JdbcEntityInformation<Person, Long> entityInformation = factory.getEntityInformation(Person.class);

		assertThat(entityInformation).isInstanceOf(MappingJdbcEntityInformation.class);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void createsRepositoryWithIdTypeLong() {

		R2dbcRepositoryFactory factory = new R2dbcRepositoryFactory(databaseClient, mappingContext);
		MyPersonRepository repository = factory.getRepository(MyPersonRepository.class);

		assertThat(repository).isNotNull();
	}

	interface MyPersonRepository extends Repository<Person, Long> {}

	static class Person {}
}
