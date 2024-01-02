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
package org.springframework.data.r2dbc.repository.query;

import static org.assertj.core.api.Assertions.*;

import kotlin.Unit;
import org.springframework.data.relational.repository.Lock;
import org.springframework.data.relational.core.sql.LockMode;
import reactor.core.publisher.Mono;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;

/**
 * Unit test for {@link R2dbcQueryMethod}.
 *
 * @author Mark Paluch
 * @author Stephen Cohen
 * @author Diego Krupitza
 */
class R2dbcQueryMethodUnitTests {

	private RelationalMappingContext context;

	@BeforeEach
	void setUp() {
		this.context = new R2dbcMappingContext();
	}

	@Test
	void detectsCollectionFromReturnTypeIfReturnTypeAssignable() throws Exception {

		R2dbcQueryMethod queryMethod = queryMethod(SampleRepository.class, "method");
		RelationalEntityMetadata<?> metadata = queryMethod.getEntityInformation();

		assertThat(metadata.getJavaType()).isAssignableFrom(Contact.class);
		assertThat(metadata.getTableName()).isEqualTo(SqlIdentifier.unquoted("contact"));
	}

	@Test // gh-235
	void detectsModifyingQuery() throws Exception {

		R2dbcQueryMethod queryMethod = queryMethod(SampleRepository.class, "method");

		assertThat(queryMethod.isModifyingQuery()).isTrue();
	}

	@Test // gh-235
	void detectsNotModifyingQuery() throws Exception {

		R2dbcQueryMethod queryMethod = queryMethod(SampleRepository.class, "differentTable");

		assertThat(queryMethod.isModifyingQuery()).isFalse();
	}

	@Test
	void detectsTableNameFromRepoTypeIfReturnTypeNotAssignable() throws Exception {

		R2dbcQueryMethod queryMethod = queryMethod(SampleRepository.class, "differentTable");
		RelationalEntityMetadata<?> metadata = queryMethod.getEntityInformation();

		assertThat(metadata.getJavaType()).isAssignableFrom(Address.class);
		assertThat(metadata.getTableName()).isEqualTo(SqlIdentifier.unquoted("contact"));
	}

	@Test
	void rejectsNullMappingContext() throws Exception {

		Method method = PersonRepository.class.getMethod("findMonoByLastname", String.class, Pageable.class);

		assertThatIllegalArgumentException().isThrownBy(() -> new R2dbcQueryMethod(method,
				new DefaultRepositoryMetadata(PersonRepository.class), new SpelAwareProxyProjectionFactory(), null));
	}

	@Test
	void rejectsMonoPageableResult() {
		assertThatIllegalStateException()
				.isThrownBy(() -> queryMethod(PersonRepository.class, "findMonoByLastname", String.class, Pageable.class));
	}

	@Test
	void createsQueryMethodObjectForMethodReturningAnInterface() throws Exception {
		queryMethod(SampleRepository.class, "methodReturningAnInterface");
	}

	@Test
	void throwsExceptionOnWrappedPage() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> queryMethod(PersonRepository.class, "findMonoPageByLastname", String.class, Pageable.class));
	}

	@Test
	void throwsExceptionOnWrappedSlice() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> queryMethod(PersonRepository.class, "findMonoSliceByLastname", String.class, Pageable.class));
	}

	@Test
	void fallsBackToRepositoryDomainTypeIfMethodDoesNotReturnADomainType() throws Exception {

		R2dbcQueryMethod method = queryMethod(PersonRepository.class, "deleteByUserName", String.class);

		assertThat(method.getEntityInformation().getJavaType()).isAssignableFrom(Contact.class);
	}

	@Test // gh-421
	void fallsBackToRepositoryDomainTypeIfMethodReturnsKotlinUnit() throws Exception {

		R2dbcQueryMethod method = queryMethod(PersonRepository.class, "deleteByFirstname", String.class);

		assertThat(method.getEntityInformation().getJavaType()).isAssignableFrom(Contact.class);
	}

	@Test // GH-1041
	void returnsQueryMethodWithCorrectLockTypeWriteLock() throws Exception {

		R2dbcQueryMethod queryMethodWithWriteLock = queryMethod(PersonRepository.class, "queryMethodWithWriteLock");

		assertThat(queryMethodWithWriteLock.getLock()).isPresent();
		assertThat(queryMethodWithWriteLock.getLock().get().value()).isEqualTo(LockMode.PESSIMISTIC_WRITE);
	}

	@Test // GH-1041
	void returnsQueryMethodWithCorrectLockTypeReadLock() throws Exception {

		R2dbcQueryMethod queryMethodWithReadLock = queryMethod(PersonRepository.class, "queryMethodWithReadLock");

		assertThat(queryMethodWithReadLock.getLock()).isPresent();
		assertThat(queryMethodWithReadLock.getLock().get().value()).isEqualTo(LockMode.PESSIMISTIC_READ);
	}

	@Test // GH-1041
	void returnsQueryMethodWithCorrectLockTypeNoLock() throws Exception {

		R2dbcQueryMethod queryMethodWithWriteLock = queryMethod(SampleRepository.class, "methodReturningAnInterface");

		assertThat(queryMethodWithWriteLock.getLock()).isEmpty();
	}

	private R2dbcQueryMethod queryMethod(Class<?> repository, String name, Class<?>... parameters) throws Exception {

		Method method = repository.getMethod(name, parameters);
		ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
		return new R2dbcQueryMethod(method, new DefaultRepositoryMetadata(repository), factory, context);
	}

	interface PersonRepository extends Repository<Contact, Long> {

		@Lock(LockMode.PESSIMISTIC_WRITE)
		Mono<Contact> queryMethodWithWriteLock();

		@Lock(LockMode.PESSIMISTIC_READ)
		Mono<Contact> queryMethodWithReadLock();

		Mono<Contact> findMonoByLastname(String lastname, Pageable pageRequest);

		Mono<Page<Contact>> findMonoPageByLastname(String lastname, Pageable pageRequest);

		Mono<Slice<Contact>> findMonoSliceByLastname(String lastname, Pageable pageRequest);

		void deleteByUserName(String userName);

		Unit deleteByFirstname(String firstname);
	}

	interface SampleRepository extends Repository<Contact, Long> {

		@MyModifyingAnnotation
		List<Contact> method();

		List<Address> differentTable();

		Customer methodReturningAnInterface();
	}

	interface Customer {}

	static class Contact {}

	private static class Address {}

	@Retention(RetentionPolicy.RUNTIME)
	@Modifying
	@interface MyModifyingAnnotation {
	}

}
