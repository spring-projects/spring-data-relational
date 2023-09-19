/*
 * Copyright 2017-2023 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import static org.assertj.core.api.Assertions.*;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.PersistentPropertyPathTestUtils;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.dialect.AnsiDialect;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * Unit tests the {@link SqlGenerator} with a fixed {@link NamingStrategy} implementation containing a hard wired
 * schema, table, and property prefix.
 *
 * @author Greg Turnquist
 * @author Mark Paluch
 */
class SqlGeneratorFixedNamingStrategyUnitTests {

	private final NamingStrategy fixedCustomTablePrefixStrategy = new NamingStrategy() {

		@Override
		public String getSchema() {
			return "FixedCustomSchema";
		}

		@Override
		public String getTableName(Class<?> type) {
			return "FixedCustomTablePrefix_" + type.getSimpleName();
		}

		@Override
		public String getColumnName(RelationalPersistentProperty property) {
			return "FixedCustomPropertyPrefix_" + property.getName();
		}
	};

	private final NamingStrategy upperCaseLowerCaseStrategy = new NamingStrategy() {

		@Override
		public String getTableName(Class<?> type) {
			return type.getSimpleName().toUpperCase();
		}

		@Override
		public String getColumnName(RelationalPersistentProperty property) {
			return property.getName().toLowerCase();
		}
	};

	private RelationalMappingContext context;

	@Test // DATAJDBC-107
	void findOneWithOverriddenFixedTableName() {

		SqlGenerator sqlGenerator = configureSqlGenerator(fixedCustomTablePrefixStrategy);

		String sql = sqlGenerator.getFindOne();

		SoftAssertions softAssertions = new SoftAssertions();
		softAssertions.assertThat(sql) //
				.isEqualTo(
						"SELECT \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_DUMMYENTITY\".\"FIXEDCUSTOMPROPERTYPREFIX_ID\" AS \"FIXEDCUSTOMPROPERTYPREFIX_ID\", "
								+ "\"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_DUMMYENTITY\".\"FIXEDCUSTOMPROPERTYPREFIX_NAME\" AS \"FIXEDCUSTOMPROPERTYPREFIX_NAME\", "
								+ "\"ref\".\"FIXEDCUSTOMPROPERTYPREFIX_L1ID\" AS \"REF_FIXEDCUSTOMPROPERTYPREFIX_L1ID\", "
								+ "\"ref\".\"FIXEDCUSTOMPROPERTYPREFIX_CONTENT\" AS \"REF_FIXEDCUSTOMPROPERTYPREFIX_CONTENT\", "
								+ "\"ref_further\".\"FIXEDCUSTOMPROPERTYPREFIX_L2ID\" AS \"REF_FURTHER_FIXEDCUSTOMPROPERTYPREFIX_L2ID\", "
								+ "\"ref_further\".\"FIXEDCUSTOMPROPERTYPREFIX_SOMETHING\" AS \"REF_FURTHER_FIXEDCUSTOMPROPERTYPREFIX_SOMETHING\" "
								+ "FROM \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_DUMMYENTITY\" "
								+ "LEFT OUTER JOIN \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_REFERENCEDENTITY\" \"ref\" ON \"ref\".\"FIXEDCUSTOMTABLEPREFIX_DUMMYENTITY\" = \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_DUMMYENTITY\".\"FIXEDCUSTOMPROPERTYPREFIX_ID\" L"
								+ "EFT OUTER JOIN \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_SECONDLEVELREFERENCEDENTITY\" \"ref_further\" ON \"ref_further\".\"FIXEDCUSTOMTABLEPREFIX_REFERENCEDENTITY\" = \"ref\".\"FIXEDCUSTOMPROPERTYPREFIX_L1ID\" "
								+ "WHERE \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_DUMMYENTITY\".\"FIXEDCUSTOMPROPERTYPREFIX_ID\" = :id");
		softAssertions.assertAll();
	}

	@Test // DATAJDBC-107
	void findOneWithUppercasedTablesAndLowercasedColumns() {

		SqlGenerator sqlGenerator = configureSqlGenerator(upperCaseLowerCaseStrategy);

		String sql = sqlGenerator.getFindOne();

		SoftAssertions softAssertions = new SoftAssertions();
		softAssertions.assertThat(sql) //
				.isEqualTo(
						"SELECT \"DUMMYENTITY\".\"ID\" AS \"ID\", \"DUMMYENTITY\".\"NAME\" AS \"NAME\", \"ref\".\"L1ID\" AS \"REF_L1ID\", \"ref\".\"CONTENT\" AS \"REF_CONTENT\", "
								+ "\"ref_further\".\"L2ID\" AS \"REF_FURTHER_L2ID\", \"ref_further\".\"SOMETHING\" AS \"REF_FURTHER_SOMETHING\" "
								+ "FROM \"DUMMYENTITY\" "
								+ "LEFT OUTER JOIN \"REFERENCEDENTITY\" \"ref\" ON \"ref\".\"DUMMYENTITY\" = \"DUMMYENTITY\".\"ID\" "
								+ "LEFT OUTER JOIN \"SECONDLEVELREFERENCEDENTITY\" \"ref_further\" ON \"ref_further\".\"REFERENCEDENTITY\" = \"ref\".\"L1ID\" "
								+ "WHERE \"DUMMYENTITY\".\"ID\" = :id");
		softAssertions.assertAll();
	}

	@Test // DATAJDBC-107
	void cascadingDeleteFirstLevel() {

		SqlGenerator sqlGenerator = configureSqlGenerator(fixedCustomTablePrefixStrategy);

		String sql = sqlGenerator.createDeleteByPath(getPath("ref"));

		assertThat(sql).isEqualTo("DELETE FROM \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_REFERENCEDENTITY\" "
				+ "WHERE \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_REFERENCEDENTITY\".\"FIXEDCUSTOMTABLEPREFIX_DUMMYENTITY\" = :rootId");
	}

	@Test // DATAJDBC-107
	void cascadingDeleteAllSecondLevel() {

		SqlGenerator sqlGenerator = configureSqlGenerator(fixedCustomTablePrefixStrategy);

		String sql = sqlGenerator.createDeleteByPath(getPath("ref.further"));

		assertThat(sql)
				.isEqualTo("DELETE FROM \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_SECONDLEVELREFERENCEDENTITY\" "
						+ "WHERE \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_SECONDLEVELREFERENCEDENTITY\".\"FIXEDCUSTOMTABLEPREFIX_REFERENCEDENTITY\" IN "
						+ "(SELECT \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_REFERENCEDENTITY\".\"FIXEDCUSTOMPROPERTYPREFIX_L1ID\" "
						+ "FROM \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_REFERENCEDENTITY\" "
						+ "WHERE \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_REFERENCEDENTITY\".\"FIXEDCUSTOMTABLEPREFIX_DUMMYENTITY\" = :rootId)");
	}

	@Test // DATAJDBC-107
	void deleteAll() {

		SqlGenerator sqlGenerator = configureSqlGenerator(fixedCustomTablePrefixStrategy);

		String sql = sqlGenerator.createDeleteAllSql(null);

		assertThat(sql).isEqualTo("DELETE FROM \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_DUMMYENTITY\"");
	}

	@Test // DATAJDBC-107
	void cascadingDeleteAllFirstLevel() {

		SqlGenerator sqlGenerator = configureSqlGenerator(fixedCustomTablePrefixStrategy);

		String sql = sqlGenerator.createDeleteAllSql(getPath("ref"));

		assertThat(sql).isEqualTo("DELETE FROM \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_REFERENCEDENTITY\" "
				+ "WHERE \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_REFERENCEDENTITY\".\"FIXEDCUSTOMTABLEPREFIX_DUMMYENTITY\" IS NOT NULL");
	}

	@Test // DATAJDBC-107
	void cascadingDeleteSecondLevel() {

		SqlGenerator sqlGenerator = configureSqlGenerator(fixedCustomTablePrefixStrategy);

		String sql = sqlGenerator.createDeleteAllSql(getPath("ref.further"));

		assertThat(sql)
				.isEqualTo("DELETE FROM \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_SECONDLEVELREFERENCEDENTITY\" "
						+ "WHERE \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_SECONDLEVELREFERENCEDENTITY\".\"FIXEDCUSTOMTABLEPREFIX_REFERENCEDENTITY\" IN "
						+ "(SELECT \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_REFERENCEDENTITY\".\"FIXEDCUSTOMPROPERTYPREFIX_L1ID\" "
						+ "FROM \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_REFERENCEDENTITY\" "
						+ "WHERE \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_REFERENCEDENTITY\".\"FIXEDCUSTOMTABLEPREFIX_DUMMYENTITY\" IS NOT NULL)");
	}

	@Test // DATAJDBC-113
	void deleteByList() {

		SqlGenerator sqlGenerator = configureSqlGenerator(fixedCustomTablePrefixStrategy);

		String sql = sqlGenerator.getDeleteByList();

		assertThat(sql).isEqualTo(
				"DELETE FROM \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_DUMMYENTITY\" WHERE \"FIXEDCUSTOMSCHEMA\".\"FIXEDCUSTOMTABLEPREFIX_DUMMYENTITY\".\"FIXEDCUSTOMPROPERTYPREFIX_ID\" IN (:ids)");
	}

	private PersistentPropertyPath<RelationalPersistentProperty> getPath(String path) {
		return PersistentPropertyPathTestUtils.getPath(path, DummyEntity.class, context);
	}

	/**
	 * Plug in a custom {@link NamingStrategy} for this test case.
	 */
	private SqlGenerator configureSqlGenerator(NamingStrategy namingStrategy) {

		context = new JdbcMappingContext(namingStrategy);
		JdbcConverter converter = new MappingJdbcConverter(context, (identifier, path) -> {
			throw new UnsupportedOperationException();
		});
		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(DummyEntity.class);
		return new SqlGenerator(context, converter, persistentEntity, AnsiDialect.INSTANCE);
	}

	@SuppressWarnings("unused")
	static class DummyEntity {

		@Id Long id;
		String name;
		ReferencedEntity ref;
	}

	@SuppressWarnings("unused")
	static class ReferencedEntity {

		@Id Long l1id;
		String content;
		SecondLevelReferencedEntity further;
	}

	@SuppressWarnings("unused")
	static class SecondLevelReferencedEntity {

		@Id Long l2id;
		String something;
	}

}
