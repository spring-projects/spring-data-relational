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

import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.ConversionCustomizer;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.core.mapping.JdbcPersistentEntity;
import org.springframework.data.jdbc.core.mapping.NamingStrategy;

/**
 * Unit tests for the default {@link NamingStrategy}.
 *
 * @author Kazuki Shimizu
 * @author Jens Schauder
 */
public class NamingStrategyUnitTests {

	private final NamingStrategy target = NamingStrategy.INSTANCE;

	private final JdbcPersistentEntity<?> persistentEntity = //
			new JdbcMappingContext(target, mock(ConversionCustomizer.class)).getRequiredPersistentEntity(DummyEntity.class);

	@Test // DATAJDBC-184
	public void getTableName() {
		assertThat(target.getTableName(persistentEntity.getType())).isEqualTo("dummy_entity");
	}

	@Test // DATAJDBC-184
	public void getColumnName() {

		assertThat(target.getColumnName(persistentEntity.getPersistentProperty("id"))) //
				.isEqualTo("id");
		assertThat(target.getColumnName(persistentEntity.getPersistentProperty("createdAt"))) //
				.isEqualTo("created_at");
		assertThat(target.getColumnName(persistentEntity.getPersistentProperty("dummySubEntities"))) //
				.isEqualTo("dummy_sub_entities");
	}

	@Test // DATAJDBC-184
	public void getReverseColumnName() {
		assertThat(target.getReverseColumnName(persistentEntity.getPersistentProperty("dummySubEntities")))
				.isEqualTo("dummy_entity");
	}

	@Test // DATAJDBC-184
	public void getKeyColumn() {

		assertThat(target.getKeyColumn(persistentEntity.getPersistentProperty("dummySubEntities"))) //
				.isEqualTo("dummy_entity_key");
	}

	@Test // DATAJDBC-184
	public void getSchema() {
		assertThat(target.getSchema()).isEmpty();
	}

	@Test // DATAJDBC-184
	public void getQualifiedTableName() {

		assertThat(target.getQualifiedTableName(persistentEntity.getType())).isEqualTo("dummy_entity");
	}

	@Data
	private static class DummyEntity {

		@Id private int id;
		private LocalDateTime createdAt;
		private List<DummySubEntity> dummySubEntities;
	}

	@Data
	private static class DummySubEntity {

		@Id private int id;
		private LocalDateTime createdAt;
	}

}
