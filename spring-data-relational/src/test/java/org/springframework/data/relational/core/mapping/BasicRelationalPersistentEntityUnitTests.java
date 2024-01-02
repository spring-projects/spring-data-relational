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
import static org.springframework.data.relational.core.sql.SqlIdentifier.*;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.BasicRelationalPersistentEntityUnitTests.MyConfig;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.spel.spi.EvaluationContextExtension;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Unit tests for {@link BasicRelationalPersistentEntity}.
 *
 * @author Oliver Gierke
 * @author Kazuki Shimizu
 * @author Bastian Wilhelm
 * @author Mark Paluch
 * @author Mikhail Polivakha
 * @author Kurt Niemi
 */
@SpringJUnitConfig(classes = MyConfig.class)
class BasicRelationalPersistentEntityUnitTests {

	@Autowired ApplicationContext applicationContext;
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

		assertThat(entity.getQualifiedTableName()).isEqualTo(fullExpected);
		assertThat(entity.getTableName()).isEqualTo(simpleExpected);

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
		RelationalPersistentEntity<?> entity = mappingContext
				.getRequiredPersistentEntity(EntityWithSchemaAndTableSpelExpression.class);

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
		SqlIdentifier expected = SqlIdentifier.from(quoted("RandomSQLToExecute"), simpleExpected);
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

	@Test // GH-1325
	void considersSpelExtensions() {

		mappingContext.setApplicationContext(applicationContext);
		RelationalPersistentEntity<?> entity = mappingContext
				.getRequiredPersistentEntity(WithConfiguredSqlIdentifiers.class);

		assertThat(entity.getTableName()).isEqualTo(SqlIdentifier.quoted("my_table"));
		assertThat(entity.getIdColumn()).isEqualTo(SqlIdentifier.quoted("my_column"));
	}

	@Table(schema = "ANAKYN_SKYWALKER")
	private static class EntityWithSchema {
		@Id private Long id;
	}

	@Table(schema = "DART_VADER", name = "I_AM_THE_SENATE")
	private static class EntityWithSchemaAndName {
		@Id private Long id;
	}

	@Table(schema = "#{T(org.springframework.data.relational.core.mapping."
			+ "BasicRelationalPersistentEntityUnitTests$EntityWithSchemaAndTableSpelExpression).desiredSchemaName}",
			name = "#{T(org.springframework.data.relational.core.mapping."
					+ "BasicRelationalPersistentEntityUnitTests$EntityWithSchemaAndTableSpelExpression).desiredTableName}")
	private static class EntityWithSchemaAndTableSpelExpression {
		@Id private Long id;
		public static String desiredTableName = "USE_THE_FORCE";
		public static String desiredSchemaName = "HELP_ME_OBI_WON";
	}

	@Table(schema = "#{T(org.springframework.data.relational.core.mapping."
			+ "BasicRelationalPersistentEntityUnitTests$LittleBobbyTables).desiredSchemaName}",
			name = "#{T(org.springframework.data.relational.core.mapping."
			+ "BasicRelationalPersistentEntityUnitTests$LittleBobbyTables).desiredTableName}")
	private static class LittleBobbyTables {
		@Id private Long id;
		public static String desiredTableName = "Robert'); DROP TABLE students;--";
		public static String desiredSchemaName = "Random SQL To Execute;";
	}

	@Table("dummy_sub_entity")
	static class DummySubEntity {
		@Id
		@Column("renamedId") Long id;
	}

	@Table()
	static class DummyEntityWithEmptyAnnotation {
		@Id
		@Column() Long id;
	}

	enum NamingStrategyWithSchema implements NamingStrategy {
		INSTANCE;

		@Override
		public String getSchema() {
			return "my_schema";
		}
	}

	@Table("#{myExtension.getTableName()}")
	static class WithConfiguredSqlIdentifiers {
		@Id
		@Column("#{myExtension.getColumnName()}") Long id;
	}

	@Configuration
	public static class MyConfig {

		@Bean
		public MyExtension extension() {
			return new MyExtension();
		}

	}

	public static class MyExtension implements EvaluationContextExtension {

		@Override
		public String getExtensionId() {
			return "my";
		}

		public String getTableName() {
			return "my_table";
		}

		public String getColumnName() {
			return "my_column";
		}

		@Override
		public Map<String, Object> getProperties() {
			return Map.of("myExtension", this);
		}

	}
}
