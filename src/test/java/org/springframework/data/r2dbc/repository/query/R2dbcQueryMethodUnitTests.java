/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.r2dbc.repository.query;

import static org.assertj.core.api.Assertions.*;

import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;

/**
 * Unit test for {@link R2dbcQueryMethod}.
 *
 * @author Mark Paluch
 */
public class R2dbcQueryMethodUnitTests {

	RelationalMappingContext context;

	@Before
	public void setUp() {
		this.context = new RelationalMappingContext();
	}

	@Test
	public void detectsCollectionFromReturnTypeIfReturnTypeAssignable() throws Exception {

		R2dbcQueryMethod queryMethod = queryMethod(SampleRepository.class, "method");
		RelationalEntityMetadata<?> metadata = queryMethod.getEntityInformation();

		assertThat(metadata.getJavaType()).isAssignableFrom(Contact.class);
		assertThat(metadata.getTableName()).isEqualTo("contact");
	}

	@Test
	public void detectsTableNameFromRepoTypeIfReturnTypeNotAssignable() throws Exception {

		R2dbcQueryMethod queryMethod = queryMethod(SampleRepository.class, "differentTable");
		RelationalEntityMetadata<?> metadata = queryMethod.getEntityInformation();

		assertThat(metadata.getJavaType()).isAssignableFrom(Address.class);
		assertThat(metadata.getTableName()).isEqualTo("contact");
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullMappingContext() throws Exception {

		Method method = PersonRepository.class.getMethod("findMonoByLastname", String.class, Pageable.class);

		new R2dbcQueryMethod(method, new DefaultRepositoryMetadata(PersonRepository.class),
				new SpelAwareProxyProjectionFactory(), null);
	}

	@Test(expected = IllegalStateException.class)
	public void rejectsMonoPageableResult() throws Exception {
		queryMethod(PersonRepository.class, "findMonoByLastname", String.class, Pageable.class);
	}

	@Test
	public void createsQueryMethodObjectForMethodReturningAnInterface() throws Exception {
		queryMethod(SampleRepository.class, "methodReturningAnInterface");
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void throwsExceptionOnWrappedPage() throws Exception {
		queryMethod(PersonRepository.class, "findMonoPageByLastname", String.class, Pageable.class);
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void throwsExceptionOnWrappedSlice() throws Exception {
		queryMethod(PersonRepository.class, "findMonoSliceByLastname", String.class, Pageable.class);
	}

	@Test
	public void fallsBackToRepositoryDomainTypeIfMethodDoesNotReturnADomainType() throws Exception {

		R2dbcQueryMethod method = queryMethod(PersonRepository.class, "deleteByUserName", String.class);

		assertThat(method.getEntityInformation().getJavaType()).isAssignableFrom(Contact.class);
	}

	private R2dbcQueryMethod queryMethod(Class<?> repository, String name, Class<?>... parameters) throws Exception {

		Method method = repository.getMethod(name, parameters);
		ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
		return new R2dbcQueryMethod(method, new DefaultRepositoryMetadata(repository), factory, context);
	}

	interface PersonRepository extends Repository<Contact, Long> {

		Mono<Contact> findMonoByLastname(String lastname, Pageable pageRequest);

		Mono<Page<Contact>> findMonoPageByLastname(String lastname, Pageable pageRequest);

		Mono<Slice<Contact>> findMonoSliceByLastname(String lastname, Pageable pageRequest);

		void deleteByUserName(String userName);
	}

	interface SampleRepository extends Repository<Contact, Long> {

		List<Contact> method();

		List<Address> differentTable();

		Customer methodReturningAnInterface();
	}

	interface Customer {}

	static class Contact {}

	static class Address {}
}
