/*
 * Copyright 2018-2024 the original author or authors.
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.convert.MappingR2dbcConverter;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.relational.core.dialect.AnsiDialect;
import org.springframework.data.relational.repository.query.RelationalEntityInformation;
import org.springframework.data.relational.repository.support.MappingRelationalEntityInformation;
import org.springframework.data.repository.Repository;
import org.springframework.r2dbc.core.DatabaseClient;

/**
 * Unit test for {@link R2dbcRepositoryFactory}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
@ExtendWith(MockitoExtension.class)
public class R2dbcRepositoryFactoryUnitTests {

	R2dbcConverter r2dbcConverter = new MappingR2dbcConverter(new R2dbcMappingContext());

	@Mock DatabaseClient databaseClient;
	@Mock ReactiveDataAccessStrategy dataAccessStrategy;

	@BeforeEach
	@SuppressWarnings("unchecked")
	public void before() {

		when(dataAccessStrategy.getConverter()).thenReturn(r2dbcConverter);
	}

	@Test
	public void usesMappingRelationalEntityInformationIfMappingContextSet() {

		R2dbcRepositoryFactory factory = new R2dbcRepositoryFactory(databaseClient, dataAccessStrategy);
		RelationalEntityInformation<Person, Long> entityInformation = factory.getEntityInformation(Person.class);

		assertThat(entityInformation).isInstanceOf(MappingRelationalEntityInformation.class);
	}

	@Test
	public void createsRepositoryWithIdTypeLong() {

		when(dataAccessStrategy.getDialect()).thenReturn(AnsiDialect.INSTANCE);

		R2dbcRepositoryFactory factory = new R2dbcRepositoryFactory(databaseClient, dataAccessStrategy);
		MyPersonRepository repository = factory.getRepository(MyPersonRepository.class);

		assertThat(repository).isNotNull();
	}

	interface MyPersonRepository extends Repository<Person, Long> {}

	static class Person {
		@Id long id;
	}
}
