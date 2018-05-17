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
package org.springframework.data.jdbc.mapping.model;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * Unit tests for the {@link NamingStrategy}.
 *
 * @author Kazuki Shimizu
 */
public class NamingStrategyUnitTests {

	private final NamingStrategy target = NamingStrategy.INSTANCE;
	private final JdbcMappingContext context = new JdbcMappingContext(
			target,
			mock(NamedParameterJdbcOperations.class),
			mock(ConversionCustomizer.class));
	private final JdbcPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(DummyEntity.class);

	@Test
	public void getTableName() {

		assertThat(target.getTableName(persistentEntity.getType())).isEqualTo("DummyEntity");
		assertThat(target.getTableName(DummySubEntity.class)).isEqualTo("dummy_sub_entity"); // DATAJDBC-106
	}

	@Test // DATAJDBC-106
	public void getTableNameWithTableAnnotation() {

		assertThat(target.getTableName(DummySubEntity.class)).isEqualTo("dummy_sub_entity");
	}

	@Test
	public void getColumnName() {

		assertThat(target.getColumnName(persistentEntity.getPersistentProperty("id"))).isEqualTo("id");
		assertThat(target.getColumnName(persistentEntity.getPersistentProperty("createdAt"))).isEqualTo("createdAt");
		assertThat(target.getColumnName(persistentEntity.getPersistentProperty("dummySubEntities")))
				.isEqualTo("dummySubEntities");
	}

	@Test // DATAJDBC-106
	public void getColumnNameWithColumnAnnotation() {

		assertThat(target.getColumnName(persistentEntity.getPersistentProperty("name"))).isEqualTo("dummy_name");
		assertThat(target.getColumnName(persistentEntity.getPersistentProperty("lastUpdatedAt")))
				.isEqualTo("dummy_last_updated_at");
	}

	@Test
	public void getReverseColumnName() {

		assertThat(target.getReverseColumnName(persistentEntity.getPersistentProperty("dummySubEntities")))
				.isEqualTo("DummyEntity");
	}

	@Test
	public void getKeyColumn() {

		assertThat(target.getKeyColumn(persistentEntity.getPersistentProperty("dummySubEntities")))
				.isEqualTo("DummyEntity_key");
	}

	@Test
	public void getSchema() {
		assertThat(target.getSchema()).isEmpty();
	}

	@Test
	public void getQualifiedTableName() {

		assertThat(target.getQualifiedTableName(persistentEntity.getType())).isEqualTo("DummyEntity");

		NamingStrategy strategy = new NamingStrategy() {
			@Override
			public String getSchema() {
				return "schema";
			}
		};

		assertThat(strategy.getQualifiedTableName(persistentEntity.getType())).isEqualTo("schema.DummyEntity");
	}

	private static class DummyEntity {

		@Id private int id;
		@Column("dummy_name") private String name;
		private LocalDateTime createdAt;
		private LocalDateTime lastUpdatedAt;
		private List<DummySubEntity> dummySubEntities;

		@Column("dummy_last_updated_at")
		public LocalDateTime getLastUpdatedAt() {
			return LocalDateTime.now();
		}
	}

	@Table("dummy_sub_entity")
	private static class DummySubEntity {}
}
