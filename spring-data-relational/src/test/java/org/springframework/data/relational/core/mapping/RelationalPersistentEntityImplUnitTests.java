/*
 * Copyright 2018-2023 the original author or authors.
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
 * @author Kurt Niemi
 */
class RelationalPersistentEntityImplUnitTests {

	private RelationalMappingContext mappingContext = new RelationalMappingContext();

	@Test // DATAJDBC-106
	void discoversAnnotatedTableName() {

		RelationalPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(DummySubEntity.class);

		assertThat(entity.getTableName()).isEqualTo(quoted("dummy_sub_entity"));
		assertThat(entity.getQualifiedTableName()).isEqualTo(quoted("dummy_sub_entity"));
		assertThat(entity.getTableName()).isEqualTo(quoted("dummy_sub_entity"));
	}

	@Test // DATAJDBC-294
	void considerIdColumnName() {

		RelationalPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(DummySubEntity.class);

		assertThat(entity.getIdColumn()).isEqualTo(quoted("renamedId"));
	}

	@Test // DATAJDBC-296
	void emptyTableAnnotationFallsBackToNamingStrategy() {

		RelationalPersistentEntity<?> entity = mappingContext
				.getRequiredPersistentEntity(DummyEntityWithEmptyAnnotation.class);

		assertThat(entity.getTableName()).isEqualTo(quoted("DUMMY_ENTITY_WITH_EMPTY_ANNOTATION"));
		assertThat(entity.getQualifiedTableName()).isEqualTo(quoted("DUMMY_ENTITY_WITH_EMPTY_ANNOTATION"));
		assertThat(entity.getTableName()).isEqualTo(quoted("DUMMY_ENTITY_WITH_EMPTY_ANNOTATION"));
	}

	@Test // DATAJDBC-491
	void namingStrategyWithSchemaReturnsCompositeTableName() {

		mappingContext = new RelationalMappingContext(NamingStrategyWithSchema.INSTANCE);
		RelationalPersistentEntity<?> entity = mappingContext
				.getRequiredPersistentEntity(DummyEntityWithEmptyAnnotation.class);

		SqlIdentifier simpleExpected = quoted("DUMMY_ENTITY_WITH_EMPTY_ANNOTATION");
		SqlIdentifier fullExpected = SqlIdentifier.from(quoted("MY_SCHEMA"), simpleExpected);

		assertThat(entity.getQualifiedTableName())
				.isEqualTo(fullExpected);
		assertThat(entity.getTableName())
				.isEqualTo(simpleExpected);

		assertThat(entity.getQualifiedTableName().toSql(IdentifierProcessing.ANSI))
				.isEqualTo("\"MY_SCHEMA\".\"DUMMY_ENTITY_WITH_EMPTY_ANNOTATION\"");
	}

	@Test // GH-1099
	void testRelationalPersistentEntitySchemaNameChoice() {

		mappingContext = new RelationalMappingContext(NamingStrategyWithSchema.INSTANCE);
		RelationalPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(EntityWithSchemaAndName.class);

		SqlIdentifier simpleExpected = quoted("I_AM_THE_SENATE");
		SqlIdentifier expected = SqlIdentifier.from(quoted("DART_VADER"), simpleExpected);
		assertThat(entity.getQualifiedTableName()).isEqualTo(expected);
		assertThat(entity.getTableName()).isEqualTo(simpleExpected);
	}

	@Test // GH-1325
	void testRelationalPersistentEntitySpelExpression() {

		mappingContext = new RelationalMappingContext(NamingStrategyWithSchema.INSTANCE);
		RelationalPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(EntityWithSchemaAndTableSpelExpression.class);

		SqlIdentifier simpleExpected = quoted("USE_THE_FORCE");
		SqlIdentifier expected = SqlIdentifier.from(quoted("HELP_ME_OBI_WON"), simpleExpected);
		assertThat(entity.getQualifiedTableName()).isEqualTo(expected);
		assertThat(entity.getTableName()).isEqualTo(simpleExpected);
	}
	@Test // GH-1325
	void testRelationalPersistentEntitySpelExpression_Sanitized() {

		mappingContext = new RelationalMappingContext(NamingStrategyWithSchema.INSTANCE);
		RelationalPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(LittleBobbyTables.class);

		SqlIdentifier simpleExpected = quoted("RobertDROPTABLEstudents");
		SqlIdentifier expected = SqlIdentifier.from(quoted("LITTLE_BOBBY_TABLES"), simpleExpected);
		assertThat(entity.getQualifiedTableName()).isEqualTo(expected);
		assertThat(entity.getTableName()).isEqualTo(simpleExpected);
	}

	@Test // GH-1325
	void testRelationalPersistentEntitySpelExpression_NonSpelExpression() {

		mappingContext = new RelationalMappingContext(NamingStrategyWithSchema.INSTANCE);
		RelationalPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(EntityWithSchemaAndName.class);

		SqlIdentifier simpleExpected = quoted("I_AM_THE_SENATE");
		SqlIdentifier expected = SqlIdentifier.from(quoted("DART_VADER"), simpleExpected);
		assertThat(entity.getQualifiedTableName()).isEqualTo(expected);
		assertThat(entity.getTableName()).isEqualTo(simpleExpected);
	}

	@Test // GH-1099
	void specifiedSchemaGetsCombinedWithNameFromNamingStrategy() {

		RelationalPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(EntityWithSchema.class);

		SqlIdentifier simpleExpected = quoted("ENTITY_WITH_SCHEMA");
		SqlIdentifier expected = SqlIdentifier.from(quoted("ANAKYN_SKYWALKER"), simpleExpected);
		assertThat(entity.getQualifiedTableName()).isEqualTo(expected);
		assertThat(entity.getTableName()).isEqualTo(simpleExpected);
	}

	@Table(schema = "ANAKYN_SKYWALKER")
	private static class EntityWithSchema {
		@Id private Long id;
	}

	@Table(schema = "DART_VADER", name = "I_AM_THE_SENATE")
	private static class EntityWithSchemaAndName {
		@Id private Long id;
	}

	@Table(schema = "HELP_ME_OBI_WON",
			name="#{T(org.springframework.data.relational.core.mapping." +
					"RelationalPersistentEntityImplUnitTests$EntityWithSchemaAndTableSpelExpression" +
					").desiredTableName}")
	private static class EntityWithSchemaAndTableSpelExpression {
		@Id private Long id;
		public static String desiredTableName = "USE_THE_FORCE";
	}

	@Table(schema = "LITTLE_BOBBY_TABLES",
			name="#{T(org.springframework.data.relational.core.mapping." +
					"RelationalPersistentEntityImplUnitTests$LittleBobbyTables" +
					").desiredTableName}")
	private static class LittleBobbyTables {
		@Id private Long id;
		public static String desiredTableName = "Robert'); DROP TABLE students;--";
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
