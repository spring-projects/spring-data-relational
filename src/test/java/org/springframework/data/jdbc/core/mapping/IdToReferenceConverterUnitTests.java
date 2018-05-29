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
package org.springframework.data.jdbc.core.mapping;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Proxy;

import org.junit.Test;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.Repository;

/**
 * @author Jens Schauder
 */
public class IdToReferenceConverterUnitTests {

	private AggregateReference<DummyEntity, Long> aggregateReference;

	@Test
	public void extractEntityTypeFromRepositoryClass() {

		assertThat(IdToReferenceConverter.getEntityClass(DummyEntityRepository.class)).isEqualTo(DummyEntity.class);
	}

	@Test
	public void extractEntityTypeFromCrudRepositoryClass() {

		assertThat(IdToReferenceConverter.getEntityClass(DummyEntityCrudRepository.class)).isEqualTo(DummyEntity.class);
	}

	@Test
	public void extractEntityTypeFromCrudRepositoryWithIntermediateClass() {

		assertThat(IdToReferenceConverter.getEntityClass(DummyEntityWithIntermediateRepository.class)).isEqualTo(DummyEntity.class);
	}

	@Test
	public void extractEntityTypeFromRepositoryClassProxy() {

		final Class<?> proxyClass = Proxy.getProxyClass(this.getClass().getClassLoader(), DummyEntityCrudRepository.class);

		assertThat(IdToReferenceConverter.getEntityClass(proxyClass)).isEqualTo(DummyEntity.class);
	}

	@Test
	public void extractTargetTypeFromReferenceTypeDefinition() throws NoSuchFieldException {

		final TypeDescriptor entityReferenceTypeDescriptor = new TypeDescriptor(IdToReferenceConverterUnitTests.class.getDeclaredField("aggregateReference"));
		assertThat(IdToReferenceConverter.getReferenceTarget(entityReferenceTypeDescriptor)).isEqualTo(DummyEntity.class);
	}

	private interface DummyEntityCrudRepository extends CrudRepository<DummyEntity, Long> {}
	private interface DummyEntityRepository extends Repository<DummyEntity, Long> {}

	private interface IntermediateRepInterface<X, T, ID> extends CrudRepository<T, ID>{}
	private interface DummyEntityWithIntermediateRepository extends IntermediateRepInterface<String, DummyEntity, Long>{}

	private static class DummyEntity {

	}
}
