/*
 * Copyright 2018-2019 the original author or authors.
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
import static org.springframework.data.relational.domain.SqlIdentifier.*;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntityImplUnitTests.DummySubEntity;
import org.springframework.data.relational.domain.SqlIdentifier;

/**
 * Unit tests for the {@link NamingStrategy}.
 *
 * @author Kazuki Shimizu
 * @author Oliver Gierke
 * @author Jens Schauder
 */
public class NamingStrategyUnitTests {

	private final NamingStrategy target = NamingStrategy.INSTANCE;
	private final RelationalMappingContext context = new RelationalMappingContext(target);
	private final RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(DummyEntity.class);

	@Test
	public void getTableName() {

		assertThat(target.getTableName(persistentEntity.getType())).isEqualTo(quoted("dummy_entity"));
		assertThat(target.getTableName(DummySubEntity.class)).isEqualTo(quoted("dummy_sub_entity"));
	}

	@Test
	public void getColumnName() {

		assertThat(target.getColumnName(persistentEntity.getPersistentProperty("id"))).isEqualTo(quoted("id"));
		assertThat(target.getColumnName(persistentEntity.getPersistentProperty("createdAt")))
				.isEqualTo(quoted("created_at"));
		assertThat(target.getColumnName(persistentEntity.getPersistentProperty("dummySubEntities")))
				.isEqualTo(quoted("dummy_sub_entities"));
	}

	@Test
	public void getReverseColumnName() {

		assertThat(target.getReverseColumnName(persistentEntity.getPersistentProperty("dummySubEntities")))
				.isEqualTo(quoted("dummy_entity"));
	}

	@Test
	public void getKeyColumn() {

		assertThat(target.getKeyColumn(persistentEntity.getPersistentProperty("dummySubEntities")))
				.isEqualTo(quoted("dummy_entity_key"));
	}

	@Test
	public void getSchema() {
		assertThat(target.getSchema()).isEqualTo(SqlIdentifier.EMPTY);
	}

	@Test
	public void getQualifiedTableName() {

		assertThat(target.getQualifiedTableName(persistentEntity.getType())).isEqualTo(quoted("dummy_entity"));

		NamingStrategy strategy = new NamingStrategy() {
			@Override
			public SqlIdentifier getSchema() {
				return quoted("schema");
			}
		};

		assertThat(strategy.getQualifiedTableName(persistentEntity.getType()))
				.isEqualTo(quoted("schema").concat(quoted("dummy_entity")));
	}

	static class DummyEntity {

		@Id int id;
		LocalDateTime createdAt, lastUpdatedAt;
		List<DummySubEntity> dummySubEntities;
	}
}
