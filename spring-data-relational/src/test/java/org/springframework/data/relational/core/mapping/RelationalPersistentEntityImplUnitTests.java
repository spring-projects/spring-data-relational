/*
 * Copyright 2018-2022 the original author or authors.
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
import static org.springframework.data.relational.core.sql.SqlIdentifier.*;

import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * Unit tests for {@link RelationalPersistentEntityImpl}.
 *
 * @author Oliver Gierke
 * @author Kazuki Shimizu
 * @author Bastian Wilhelm
 * @author Mark Paluch
 * @author Mikhail Polivakha
 */
public class RelationalPersistentEntityImplUnitTests {

	RelationalMappingContext mappingContext = new RelationalMappingContext();

	@Test // DATAJDBC-106
	public void discoversAnnotatedTableName() {

		RelationalPersistentEntity<?> entity = mappingContext.getPersistentEntity(DummySubEntity.class);

		assertThat(entity.getTableName()).isEqualTo(quoted("dummy_sub_entity"));
	}

	@Test // DATAJDBC-294
	public void considerIdColumnName() {

		RelationalPersistentEntity<?> entity = mappingContext.getPersistentEntity(DummySubEntity.class);

		assertThat(entity.getIdColumn()).isEqualTo(quoted("renamedId"));
	}

	@Test // DATAJDBC-296
	public void emptyTableAnnotationFallsBackToNamingStrategy() {

		RelationalPersistentEntity<?> entity = mappingContext.getPersistentEntity(DummyEntityWithEmptyAnnotation.class);

		assertThat(entity.getTableName()).isEqualTo(quoted("DUMMY_ENTITY_WITH_EMPTY_ANNOTATION"));
	}

	@Test // DATAJDBC-491
	public void namingStrategyWithSchemaReturnsCompositeTableName() {

		mappingContext = new RelationalMappingContext(NamingStrategyWithSchema.INSTANCE);
		RelationalPersistentEntity<?> entity = mappingContext.getPersistentEntity(DummyEntityWithEmptyAnnotation.class);

		assertThat(entity.getTableName())
				.isEqualTo(SqlIdentifier.from(quoted("MY_SCHEMA"), quoted("DUMMY_ENTITY_WITH_EMPTY_ANNOTATION")));
		assertThat(entity.getTableName().toSql(IdentifierProcessing.ANSI))
				.isEqualTo("\"MY_SCHEMA\".\"DUMMY_ENTITY_WITH_EMPTY_ANNOTATION\"");
	}

	@Test // GH-1099
	void testRelationalPersistentEntitySchemaNameChoice() {

		mappingContext = new RelationalMappingContext(NamingStrategyWithSchema.INSTANCE);
		RelationalPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(EntityWithSchemaAndName.class);

		SqlIdentifier tableName = persistentEntity.getTableName();

		assertThat(tableName).isEqualTo(SqlIdentifier.from(SqlIdentifier.quoted("DART_VADER"), quoted("I_AM_THE_SENATE")));
	}

	@Test // GH-1099
	void specifiedSchemaGetsCombinedWithNameFromNamingStrategy() {

		RelationalPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(EntityWithSchema.class);

		SqlIdentifier tableName = persistentEntity.getTableName();

		assertThat(tableName).isEqualTo(SqlIdentifier.from(quoted("ANAKYN_SKYWALKER"), quoted("ENTITY_WITH_SCHEMA")));
	}

	@Table(schema = "ANAKYN_SKYWALKER")
	static class EntityWithSchema {
		@Id private Long id;
	}

	@Table(schema = "DART_VADER", name = "I_AM_THE_SENATE")
	static class EntityWithSchemaAndName {
		@Id private Long id;
	}

	@Table("dummy_sub_entity")
	static class DummySubEntity {
		@Id @Column("renamedId") Long id;
	}

	@Table()
	static class DummyEntityWithEmptyAnnotation {
		@Id @Column() Long id;
	}

	enum NamingStrategyWithSchema implements NamingStrategy {
		INSTANCE;

		@Override
		public String getSchema() {
			return "my_schema";
		}
	}
}
