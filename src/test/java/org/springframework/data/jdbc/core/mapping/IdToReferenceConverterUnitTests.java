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
import static org.mockito.Mockito.*;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.junit.Test;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.data.repository.Repository;

/**
 * @author Jens Schauder
 */
public class IdToReferenceConverterUnitTests {

	private Reference<DummyEntity, Long> entityReference;

	// TODO: interesting question: what should happen when there are specific Reference Types inherited from Reference<x,y> like `interface DummyReference extends Reference<DummyEntity, Long>
	// TODO: similar but not with an interface, but with a class.

	@Test
	public void test() {

		ListableBeanFactory beans = mock(ListableBeanFactory.class);
		IdToReferenceConverter converter = new IdToReferenceConverter(beans);

	}

	@Test
	public void extractEntityTypeFromRepositoryClass() {

		assertThat(IdToReferenceConverter.getEntityClass(DummyEntityRepository.class)).isEqualTo(DummyEntity.class);
	}


	@Test
	public void extractTargetTypeFromReferenceTypeDefinition() throws NoSuchFieldException {

		final TypeDescriptor entityReferenceTypeDescriptor = new TypeDescriptor(IdToReferenceConverterUnitTests.class.getDeclaredField("entityReference"));
		assertThat(IdToReferenceConverter.getReferenceTarget(entityReferenceTypeDescriptor)).isEqualTo(DummyEntity.class);
	}


	private interface DummyEntityRepository extends Repository<DummyEntity, Long> {}

	private static class DummyEntity {

	}
}
