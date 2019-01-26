/*
 * Copyright 2017-2019 the original author or authors.
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
package org.springframework.data.jdbc.core;

import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;

import java.util.Map;
import java.util.Set;

import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.ReadOnlyProperty;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.core.mapping.PersistentPropertyPathTestUtils;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * Unit tests for the {@link SqlGenerator}.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Oleksandr Kucher
 */
public class SqlGeneratorUnitTests {

	private SqlGenerator sqlGenerator;
	private RelationalMappingContext context = new JdbcMappingContext();

	@Before
	public void setUp() {

		this.sqlGenerator = createSqlGenerator(DummyEntity.class);
	}

	SqlGenerator createSqlGenerator(Class<?> type) {

		NamingStrategy namingStrategy = new PrefixingNamingStrategy();
		RelationalMappingContext context = new JdbcMappingContext(namingStrategy);
		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(type);

		return new SqlGenerator(context, persistentEntity, new SqlGeneratorSource(context));
	}

	@Test // DATAJDBC-112
	public void findOne() {

		String sql = sqlGenerator.getFindOne();

		SoftAssertions softAssertions = new SoftAssertions();
		softAssertions.assertThat(sql) //
				.startsWith("SELECT") //
				.contains("dummy_entity.id1 AS id1,") //
				.contains("dummy_entity.x_name AS x_name,") //
				.contains("dummy_entity.x_other AS x_other,") //
				.contains("ref.x_l1id AS ref_x_l1id") //
				.contains("ref.x_content AS ref_x_content").contains(" FROM dummy_entity") //
				.contains("ON ref.dummy_entity = dummy_entity.id1") //
				.contains("WHERE dummy_entity.id1 = :id") //
				// 1-N relationships do not get loaded via join
				.doesNotContain("Element AS elements");
		softAssertions.assertAll();
	}

	@Test // DATAJDBC-112
	public void cascadingDeleteFirstLevel() {

		String sql = sqlGenerator.createDeleteByPath(getPath("ref", DummyEntity.class));

		assertThat(sql).isEqualTo("DELETE FROM referenced_entity WHERE dummy_entity = :rootId");
	}

	@Test // DATAJDBC-112
	public void cascadingDeleteAllSecondLevel() {

		String sql = sqlGenerator.createDeleteByPath(getPath("ref.further", DummyEntity.class));

		assertThat(sql).isEqualTo(
				"DELETE FROM second_level_referenced_entity WHERE referenced_entity IN (SELECT x_l1id FROM referenced_entity WHERE dummy_entity = :rootId)");
	}

	@Test // DATAJDBC-112
	public void deleteAll() {

		String sql = sqlGenerator.createDeleteAllSql(null);

		assertThat(sql).isEqualTo("DELETE FROM dummy_entity");
	}

	@Test // DATAJDBC-112
	public void cascadingDeleteAllFirstLevel() {

		String sql = sqlGenerator.createDeleteAllSql(getPath("ref", DummyEntity.class));

		assertThat(sql).isEqualTo("DELETE FROM referenced_entity WHERE dummy_entity IS NOT NULL");
	}

	@Test // DATAJDBC-112
	public void cascadingDeleteSecondLevel() {

		String sql = sqlGenerator.createDeleteAllSql(getPath("ref.further", DummyEntity.class));

		assertThat(sql).isEqualTo(
				"DELETE FROM second_level_referenced_entity WHERE referenced_entity IN (SELECT x_l1id FROM referenced_entity WHERE dummy_entity IS NOT NULL)");
	}

	@Test // DATAJDBC-227
	public void deleteAllMap() {

		String sql = sqlGenerator.createDeleteAllSql(getPath("mappedElements", DummyEntity.class));

		assertThat(sql).isEqualTo("DELETE FROM element WHERE dummy_entity IS NOT NULL");
	}

	@Test // DATAJDBC-227
	public void deleteMapByPath() {

		String sql = sqlGenerator.createDeleteByPath(getPath("mappedElements", DummyEntity.class));

		assertThat(sql).isEqualTo("DELETE FROM element WHERE dummy_entity = :rootId");
	}

	@Test // DATAJDBC-131, DATAJDBC-111
	public void findAllByProperty() {

		// this would get called when ListParent is the element type of a Set
		String sql = sqlGenerator.getFindAllByProperty("back-ref", null, false);

		assertThat(sql).isEqualTo("SELECT dummy_entity.id1 AS id1, dummy_entity.x_name AS x_name, " //
				+ "dummy_entity.x_other AS x_other, " //
				+ "ref.x_l1id AS ref_x_l1id, ref.x_content AS ref_x_content, "
				+ "ref_further.x_l2id AS ref_further_x_l2id, ref_further.x_something AS ref_further_x_something " //
				+ "FROM dummy_entity "
				+ "LEFT OUTER JOIN referenced_entity AS ref ON ref.dummy_entity = dummy_entity.id1  " //
				+ "LEFT OUTER JOIN second_level_referenced_entity AS ref_further ON ref_further.referenced_entity = referenced_entity.x_l1id " //
				+ "WHERE back-ref = :back-ref");
	}

	@Test // DATAJDBC-131, DATAJDBC-111
	public void findAllByPropertyWithKey() {

		// this would get called when ListParent is th element type of a Map
		String sql = sqlGenerator.getFindAllByProperty("back-ref", "key-column", false);

		assertThat(sql).isEqualTo("SELECT dummy_entity.id1 AS id1, dummy_entity.x_name AS x_name, " //
				+ "dummy_entity.x_other AS x_other, " //
				+ "ref.x_l1id AS ref_x_l1id, ref.x_content AS ref_x_content, "
				+ "ref_further.x_l2id AS ref_further_x_l2id, ref_further.x_something AS ref_further_x_something, " //
				+ "dummy_entity.key-column AS key-column " //
				+ "FROM dummy_entity "
				+ "LEFT OUTER JOIN referenced_entity AS ref ON ref.dummy_entity = dummy_entity.id1  " //
				+ "LEFT OUTER JOIN second_level_referenced_entity AS ref_further ON ref_further.referenced_entity = referenced_entity.x_l1id " //
				+ "WHERE back-ref = :back-ref");
	}

	@Test(expected = IllegalArgumentException.class) // DATAJDBC-130
	public void findAllByPropertyOrderedWithoutKey() {
		String sql = sqlGenerator.getFindAllByProperty("back-ref", null, true);
	}

	@Test // DATAJDBC-131, DATAJDBC-111
	public void findAllByPropertyWithKeyOrdered() {

		// this would get called when ListParent is th element type of a Map
		String sql = sqlGenerator.getFindAllByProperty("back-ref", "key-column", true);

		assertThat(sql).isEqualTo("SELECT dummy_entity.id1 AS id1, dummy_entity.x_name AS x_name, " //
				+ "dummy_entity.x_other AS x_other, " //
				+ "ref.x_l1id AS ref_x_l1id, ref.x_content AS ref_x_content, "
				+ "ref_further.x_l2id AS ref_further_x_l2id, ref_further.x_something AS ref_further_x_something, " //
				+ "dummy_entity.key-column AS key-column " //
				+ "FROM dummy_entity "
				+ "LEFT OUTER JOIN referenced_entity AS ref ON ref.dummy_entity = dummy_entity.id1  " //
				+ "LEFT OUTER JOIN second_level_referenced_entity AS ref_further ON ref_further.referenced_entity = referenced_entity.x_l1id " //
				+ "WHERE back-ref = :back-ref " + "ORDER BY key-column");
	}

	@Test // DATAJDBC-264
	public void getInsertForEmptyColumnList() {

		SqlGenerator sqlGenerator = createSqlGenerator(IdOnlyEntity.class);

		String insert = sqlGenerator.getInsert(emptySet());

		assertThat(insert).endsWith("()");
	}

	@Test // DATAJDBC-266
	public void joinForOneToOneWithoutIdIncludesTheBackReferenceOfTheOuterJoin() {

		SqlGenerator sqlGenerator = createSqlGenerator(ParentOfNoIdChild.class);

		String findAll = sqlGenerator.getFindAll();

		assertThat(findAll).containsSequence("SELECT", "child.parent_of_no_id_child AS child_parent_of_no_id_child",
				"FROM");
	}

	@Test // DATAJDBC-262
	public void update() {

		assertThat(sqlGenerator.getUpdate()).containsSequence( //
				"UPDATE", //
				"dummy_entity", //
				"SET", //
				"WHERE", //
				"id1 = :id");
	}

	@Test // DATAJDBC-324
	public void readOnlyPropertyExcludedFromQuery_when_generateUpdateSql() {

		final SqlGenerator sqlGenerator = createSqlGenerator(EntityWithReadOnlyProperty.class);

		assertThat(sqlGenerator.getUpdate()).isEqualToIgnoringCase( //
				"UPDATE entity_with_read_only_property " //
						+ "SET x_name = :x_name " //
						+ "WHERE x_id = :x_id" //
		);
	}

	@Test // DATAJDBC-324
	public void readOnlyPropertyExcludedFromQuery_when_generateInsertSql() {

		final SqlGenerator sqlGenerator = createSqlGenerator(EntityWithReadOnlyProperty.class);

		assertThat(sqlGenerator.getInsert(emptySet())).isEqualToIgnoringCase( //
				"INSERT INTO entity_with_read_only_property (x_name) " //
						+ "VALUES (:x_name)" //
		);
	}

	@Test // DATAJDBC-324
	public void readOnlyPropertyIncludedIntoQuery_when_generateFindAllSql() {

		final SqlGenerator sqlGenerator = createSqlGenerator(EntityWithReadOnlyProperty.class);

		assertThat(sqlGenerator.getFindAll()).isEqualToIgnoringCase("SELECT "
				+ "entity_with_read_only_property.x_id AS x_id, " + "entity_with_read_only_property.x_name AS x_name, "
				+ "entity_with_read_only_property.x_read_only_value AS x_read_only_value "
				+ "FROM entity_with_read_only_property");
	}

	@Test // DATAJDBC-324
	public void readOnlyPropertyIncludedIntoQuery_when_generateFindAllByPropertySql() {

		final SqlGenerator sqlGenerator = createSqlGenerator(EntityWithReadOnlyProperty.class);

		assertThat(sqlGenerator.getFindAllByProperty("back-ref", "key-column", true)).isEqualToIgnoringCase( //
				"SELECT " //
				+ "entity_with_read_only_property.x_id AS x_id, " //
				+ "entity_with_read_only_property.x_name AS x_name, " //
				+ "entity_with_read_only_property.x_read_only_value AS x_read_only_value, " //
				+ "entity_with_read_only_property.key-column AS key-column " //
				+ "FROM entity_with_read_only_property " //
				+ "WHERE back-ref = :back-ref " //
				+ "ORDER BY key-column" //
		);
	}

	@Test // DATAJDBC-324
	public void readOnlyPropertyIncludedIntoQuery_when_generateFindAllInListSql() {

		final SqlGenerator sqlGenerator = createSqlGenerator(EntityWithReadOnlyProperty.class);

		assertThat(sqlGenerator.getFindAllInList()).isEqualToIgnoringCase( //
				"SELECT " //
				+ "entity_with_read_only_property.x_id AS x_id, " //
				+ "entity_with_read_only_property.x_name AS x_name, " //
				+ "entity_with_read_only_property.x_read_only_value AS x_read_only_value " //
				+ "FROM entity_with_read_only_property " //
				+ "WHERE entity_with_read_only_property.x_id in(:ids)" //
		);
	}

	@Test // DATAJDBC-324
	public void readOnlyPropertyIncludedIntoQuery_when_generateFindOneSql() {

		final SqlGenerator sqlGenerator = createSqlGenerator(EntityWithReadOnlyProperty.class);

		assertThat(sqlGenerator.getFindOne()).isEqualToIgnoringCase( //
				"SELECT " //
				+ "entity_with_read_only_property.x_id AS x_id, " //
				+ "entity_with_read_only_property.x_name AS x_name, " //
				+ "entity_with_read_only_property.x_read_only_value AS x_read_only_value " //
				+ "FROM entity_with_read_only_property " //
				+ "WHERE entity_with_read_only_property.x_id = :id" //
		);
	}

	private PersistentPropertyPath<RelationalPersistentProperty> getPath(String path, Class<?> base) {
		return PersistentPropertyPathTestUtils.getPath(context, path, base);
	}

	@SuppressWarnings("unused")
	static class DummyEntity {

		@Column("id1")
		@Id Long id;
		String name;
		ReferencedEntity ref;
		Set<Element> elements;
		Map<Integer, Element> mappedElements;
		AggregateReference<OtherAggregate, Long> other;
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

	static class Element {
		@Id Long id;
		String content;
	}

	static class ParentOfNoIdChild {
		@Id Long id;
		NoIdChild child;
	}

	static class NoIdChild {}

	static class OtherAggregate {
		@Id Long id;
		String name;
	}

	private static class PrefixingNamingStrategy implements NamingStrategy {

		@Override
		public String getColumnName(RelationalPersistentProperty property) {
			return "x_" + NamingStrategy.super.getColumnName(property);
		}

	}

	@SuppressWarnings("unused")
	static class IdOnlyEntity {

		@Id Long id;
	}

	static class EntityWithReadOnlyProperty {

		@Id Long id;
		String name;
		@ReadOnlyProperty String readOnlyValue;
	}
}
