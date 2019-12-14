/*
 * Copyright 2017-2019 the original author or authors.
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
import static org.springframework.data.relational.domain.SqlIdentifier.*;

import org.assertj.core.api.SoftAssertions;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.core.mapping.PersistentPropertyPathTestUtils;
import org.springframework.data.jdbc.testing.AnsiDialect;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.domain.IdentifierProcessing;
import org.springframework.data.relational.domain.SqlIdentifier;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.SqlIdentifier.SimpleSqlIdentifier;

/**
 * Unit tests the {@link SqlGenerator} with a fixed {@link NamingStrategy} implementation containing a hard wired
 * schema, table, and property prefix.
 *
 * @author Greg Turnquist
 */
public class SqlGeneratorFixedNamingStrategyUnitTests {

	final NamingStrategy fixedCustomTablePrefixStrategy = new NamingStrategy() {

		@Override
		public SqlIdentifier getSchema() {
			return unquoted("FixedCustomSchema");
		}

		@Override
		public SqlIdentifier getTableName(Class<?> type) {
			return unquoted("FixedCustomTablePrefix_" + type.getSimpleName());
		}

		@Override
		public SimpleSqlIdentifier getColumnName(RelationalPersistentProperty property) {
			return unquoted("FixedCustomPropertyPrefix_" + property.getName());
		}
	};

	final NamingStrategy upperCaseLowerCaseStrategy = new NamingStrategy() {

		@Override
		public SqlIdentifier getTableName(Class<?> type) {
			return unquoted(type.getSimpleName().toUpperCase());
		}

		@Override
		public SimpleSqlIdentifier getColumnName(RelationalPersistentProperty property) {
			return unquoted(property.getName().toLowerCase());
		}
	};

	private RelationalMappingContext context = new JdbcMappingContext();

	@Test // DATAJDBC-107
	public void findOneWithOverriddenFixedTableName() {

		SqlGenerator sqlGenerator = configureSqlGenerator(fixedCustomTablePrefixStrategy);

		String sql = sqlGenerator.getFindOne();

		SoftAssertions softAssertions = new SoftAssertions();
		softAssertions.assertThat(sql) //
				.startsWith("SELECT") //
				.contains(
						"FixedCustomSchema.FixedCustomTablePrefix_DummyEntity.FixedCustomPropertyPrefix_id AS FixedCustomPropertyPrefix_id,") //
				.contains(
						"FixedCustomSchema.FixedCustomTablePrefix_DummyEntity.FixedCustomPropertyPrefix_name AS FixedCustomPropertyPrefix_name,") //
				.contains("\"REF\".FixedCustomPropertyPrefix_l1id AS ref_FixedCustomPropertyPrefix_l1id") //
				.contains("\"REF\".FixedCustomPropertyPrefix_content AS ref_FixedCustomPropertyPrefix_content") //
				.contains("FROM FixedCustomSchema.FixedCustomTablePrefix_DummyEntity");
		softAssertions.assertAll();
	}

	@Test // DATAJDBC-107
	public void findOneWithUppercasedTablesAndLowercasedColumns() {

		SqlGenerator sqlGenerator = configureSqlGenerator(upperCaseLowerCaseStrategy);

		String sql = sqlGenerator.getFindOne();

		SoftAssertions softAssertions = new SoftAssertions();
		softAssertions.assertThat(sql) //
				.startsWith("SELECT") //
				.contains("DUMMYENTITY.id AS id,") //
				.contains("DUMMYENTITY.name AS name,") //
				.contains("\"REF\".l1id AS ref_l1id") //
				.contains("\"REF\".content AS ref_content") //
				.contains("FROM DUMMYENTITY");
		softAssertions.assertAll();
	}

	@Test // DATAJDBC-107
	public void cascadingDeleteFirstLevel() {

		SqlGenerator sqlGenerator = configureSqlGenerator(fixedCustomTablePrefixStrategy);

		String sql = sqlGenerator.createDeleteByPath(getPath("ref"));

		assertThat(sql).isEqualTo("DELETE FROM FixedCustomSchema.FixedCustomTablePrefix_ReferencedEntity "
				+ "WHERE FixedCustomSchema.FixedCustomTablePrefix_ReferencedEntity.\"DUMMY_ENTITY\" = :rootId");
	}

	@Test // DATAJDBC-107
	public void cascadingDeleteAllSecondLevel() {

		SqlGenerator sqlGenerator = configureSqlGenerator(fixedCustomTablePrefixStrategy);

		String sql = sqlGenerator.createDeleteByPath(getPath("ref.further"));

		assertThat(sql).isEqualTo("DELETE FROM FixedCustomSchema.FixedCustomTablePrefix_SecondLevelReferencedEntity "
				+ "WHERE FixedCustomSchema.FixedCustomTablePrefix_SecondLevelReferencedEntity.\"REFERENCED_ENTITY\" IN "
				+ "(SELECT FixedCustomSchema.FixedCustomTablePrefix_ReferencedEntity.FixedCustomPropertyPrefix_l1id "
				+ "FROM FixedCustomSchema.FixedCustomTablePrefix_ReferencedEntity "
				+ "WHERE FixedCustomSchema.FixedCustomTablePrefix_ReferencedEntity.\"DUMMY_ENTITY\" = :rootId)");
	}

	@Test // DATAJDBC-107
	public void deleteAll() {

		SqlGenerator sqlGenerator = configureSqlGenerator(fixedCustomTablePrefixStrategy);

		String sql = sqlGenerator.createDeleteAllSql(null);

		assertThat(sql).isEqualTo("DELETE FROM FixedCustomSchema.FixedCustomTablePrefix_DummyEntity");
	}

	@Test // DATAJDBC-107
	public void cascadingDeleteAllFirstLevel() {

		SqlGenerator sqlGenerator = configureSqlGenerator(fixedCustomTablePrefixStrategy);

		String sql = sqlGenerator.createDeleteAllSql(getPath("ref"));

		assertThat(sql).isEqualTo("DELETE FROM FixedCustomSchema.FixedCustomTablePrefix_ReferencedEntity "
				+ "WHERE FixedCustomSchema.FixedCustomTablePrefix_ReferencedEntity.\"DUMMY_ENTITY\" IS NOT NULL");
	}

	@Test // DATAJDBC-107
	public void cascadingDeleteSecondLevel() {

		SqlGenerator sqlGenerator = configureSqlGenerator(fixedCustomTablePrefixStrategy);

		String sql = sqlGenerator.createDeleteAllSql(getPath("ref.further"));

		assertThat(sql).isEqualTo("DELETE FROM FixedCustomSchema.FixedCustomTablePrefix_SecondLevelReferencedEntity "
				+ "WHERE FixedCustomSchema.FixedCustomTablePrefix_SecondLevelReferencedEntity.\"REFERENCED_ENTITY\" IN "
				+ "(SELECT FixedCustomSchema.FixedCustomTablePrefix_ReferencedEntity.FixedCustomPropertyPrefix_l1id "
				+ "FROM FixedCustomSchema.FixedCustomTablePrefix_ReferencedEntity "
				+ "WHERE FixedCustomSchema.FixedCustomTablePrefix_ReferencedEntity.\"DUMMY_ENTITY\" IS NOT NULL)");
	}

	@Test // DATAJDBC-113
	public void deleteByList() {

		SqlGenerator sqlGenerator = configureSqlGenerator(fixedCustomTablePrefixStrategy);

		String sql = sqlGenerator.getDeleteByList();

		assertThat(sql).isEqualTo(
				"DELETE FROM FixedCustomSchema.FixedCustomTablePrefix_DummyEntity WHERE FixedCustomSchema.FixedCustomTablePrefix_DummyEntity.FixedCustomPropertyPrefix_id IN (:ids)");
	}

	private PersistentPropertyPath<RelationalPersistentProperty> getPath(String path) {
		return PersistentPropertyPathTestUtils.getPath(context, path, DummyEntity.class);
	}

	/**
	 * Plug in a custom {@link NamingStrategy} for this test case.
	 */
	private SqlGenerator configureSqlGenerator(NamingStrategy namingStrategy) {

		RelationalMappingContext context = new JdbcMappingContext(namingStrategy);
		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(DummyEntity.class);
		return new SqlGenerator(context, persistentEntity, AnsiDialect.INSTANCE);
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
