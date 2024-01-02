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
package org.springframework.data.relational.core.mapping;

import static org.assertj.core.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.BasicRelationalPersistentEntityUnitTests.DummySubEntity;

/**
 * Unit tests for the {@link NamingStrategy}.
 *
 * @author Kazuki Shimizu
 * @author Oliver Gierke
 * @author Jens Schauder
 */
public class DefaultNamingStrategyUnitTests {

	private final NamingStrategy target = DefaultNamingStrategy.INSTANCE;
	private final RelationalMappingContext context = new RelationalMappingContext(target);
	private final RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(DummyEntity.class);

	@Test
	public void getTableName() {

		assertThat(target.getTableName(persistentEntity.getType())).isEqualTo("dummy_entity");
		assertThat(target.getTableName(DummySubEntity.class)).isEqualTo("dummy_sub_entity");
	}

	@Test
	public void getColumnName() {

		assertThat(target.getColumnName(persistentEntity.getPersistentProperty("id"))).isEqualTo("id");
		assertThat(target.getColumnName(persistentEntity.getPersistentProperty("createdAt"))).isEqualTo("created_at");
		assertThat(target.getColumnName(persistentEntity.getPersistentProperty("dummySubEntities")))
				.isEqualTo("dummy_sub_entities");
	}

	@Test
	public void getReverseColumnName() {

		assertThat(target.getReverseColumnName(persistentEntity.getPersistentProperty("dummySubEntities")))
				.isEqualTo("dummy_entity");
	}

	@Test
	public void getKeyColumn() {

		assertThat(target.getKeyColumn(persistentEntity.getPersistentProperty("dummySubEntities")))
				.isEqualTo("dummy_entity_key");
	}

	@Test
	public void getSchema() {
		assertThat(target.getSchema()).isEqualTo("");
	}

	static class DummyEntity {

		@Id int id;
		LocalDateTime createdAt, lastUpdatedAt;
		List<DummySubEntity> dummySubEntities;
	}
}
