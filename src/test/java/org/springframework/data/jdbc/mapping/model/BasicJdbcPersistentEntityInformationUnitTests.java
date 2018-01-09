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
package org.springframework.data.jdbc.mapping.model;

import static org.assertj.core.api.Java6Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Persistable;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;

/**
 * @author Jens Schauder
 */
public class BasicJdbcPersistentEntityInformationUnitTests {

	JdbcMappingContext context = new JdbcMappingContext(new DefaultNamingStrategy(), mock(NamedParameterJdbcOperations.class), cs -> {});
	private DummyEntity dummyEntity = new DummyEntity();
	private PersistableDummyEntity persistableDummyEntity = new PersistableDummyEntity();

	@Test // DATAJDBC-158
	public void idIsBasedOnIdAnnotatedProperty() {

		dummyEntity.id = 42L;
		assertThat(context.getRequiredPersistentEntityInformation(DummyEntity.class).getRequiredId(dummyEntity))
				.isEqualTo(42L);
	}

	@Test // DATAJDBC-158
	public void idIsBasedOnPersistableGetId() {

		assertThat( //
				context.getRequiredPersistentEntityInformation(PersistableDummyEntity.class)
						.getRequiredId(persistableDummyEntity) //
		).isEqualTo(23L);
	}

	@Test // DATAJDBC-158
	public void isNewIsBasedOnIdAnnotatedPropertyBeingNull() {

		assertThat(context.getRequiredPersistentEntityInformation(DummyEntity.class).isNew(dummyEntity)).isTrue();
		dummyEntity.id = 42L;
		assertThat(context.getRequiredPersistentEntityInformation(DummyEntity.class).isNew(dummyEntity)).isFalse();
	}

	@Test // DATAJDBC-158
	public void isNewIsBasedOnPersistableIsNew() {

		persistableDummyEntity.isNewFlag = true;
		assertThat(
				context.getRequiredPersistentEntityInformation(PersistableDummyEntity.class).isNew(persistableDummyEntity))
						.isTrue();

		persistableDummyEntity.isNewFlag = false;
		assertThat(
				context.getRequiredPersistentEntityInformation(PersistableDummyEntity.class).isNew(persistableDummyEntity))
						.isFalse();
	}

	private static class DummyEntity {
		@Id Long id;
	}

	private static class PersistableDummyEntity implements Persistable<Long> {
		boolean isNewFlag;

		@Nullable
		@Override
		public Long getId() {
			return 23L;
		}

		@Override
		public boolean isNew() {
			return isNewFlag;
		}
	}
}
