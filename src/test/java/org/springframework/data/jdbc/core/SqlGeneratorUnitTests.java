/*
 * Copyright 2017-2018 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.util.Map;
import java.util.Set;

import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.PersistentPropertyPathTestUtils;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.mapping.NamingStrategy;

/**
 * Unit tests for the {@link SqlGenerator}.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 */
public class SqlGeneratorUnitTests {

	private SqlGenerator sqlGenerator;
	private RelationalMappingContext context = new RelationalMappingContext();

	@Before
	public void setUp() {

		NamingStrategy namingStrategy = new PrefixingNamingStrategy();
		RelationalMappingContext context = new RelationalMappingContext(namingStrategy);
		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(DummyEntity.class);
		this.sqlGenerator = new SqlGenerator(context, persistentEntity, new SqlGeneratorSource(context));
	}

	@Test // DATAJDBC-112
	public void findOne() {

		String sql = sqlGenerator.getFindOne();

		SoftAssertions softAssertions = new SoftAssertions();
		softAssertions.assertThat(sql) //
				.startsWith("SELECT") //
				.contains("dummy_entity.x_id AS x_id,") //
				.contains("dummy_entity.x_name AS x_name,") //
				.contains("ref.x_l1id AS ref_x_l1id") //
				.contains("ref.x_content AS ref_x_content").contains(" FROM dummy_entity") //
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

	@Test // DATAJDBC-131
	public void findAllByProperty() {

		// this would get called when DummyEntity is the element type of a Set
		String sql = sqlGenerator.getFindAllByProperty("back-ref", null, false);

		assertThat(sql).isEqualTo("SELECT dummy_entity.x_id AS x_id, dummy_entity.x_name AS x_name, "
				+ "ref.x_l1id AS ref_x_l1id, ref.x_content AS ref_x_content, ref.x_further AS ref_x_further "
				+ "FROM dummy_entity LEFT OUTER JOIN referenced_entity AS ref ON ref.dummy_entity = dummy_entity.x_id "
				+ "WHERE back-ref = :back-ref");
	}

	@Test // DATAJDBC-131
	public void findAllByPropertyWithKey() {

		// this would get called when DummyEntity is th element type of a Map
		String sql = sqlGenerator.getFindAllByProperty("back-ref", "key-column", false);

		assertThat(sql).isEqualTo("SELECT dummy_entity.x_id AS x_id, dummy_entity.x_name AS x_name, " //
				+ "ref.x_l1id AS ref_x_l1id, ref.x_content AS ref_x_content, ref.x_further AS ref_x_further, " //
				+ "dummy_entity.key-column AS key-column " //
				+ "FROM dummy_entity LEFT OUTER JOIN referenced_entity AS ref ON ref.dummy_entity = dummy_entity.x_id " //
				+ "WHERE back-ref = :back-ref");
	}

	@Test(expected = IllegalArgumentException.class) // DATAJDBC-130
	public void findAllByPropertyOrderedWithoutKey() {
		String sql = sqlGenerator.getFindAllByProperty("back-ref", null, true);
	}

	@Test // DATAJDBC-131
	public void findAllByPropertyWithKeyOrdered() {

		// this would get called when DummyEntity is th element type of a Map
		String sql = sqlGenerator.getFindAllByProperty("back-ref", "key-column", true);

		assertThat(sql).isEqualTo("SELECT dummy_entity.x_id AS x_id, dummy_entity.x_name AS x_name, "
				+ "ref.x_l1id AS ref_x_l1id, ref.x_content AS ref_x_content, ref.x_further AS ref_x_further, "
				+ "dummy_entity.key-column AS key-column "
				+ "FROM dummy_entity LEFT OUTER JOIN referenced_entity AS ref ON ref.dummy_entity = dummy_entity.x_id "
				+ "WHERE back-ref = :back-ref " + "ORDER BY key-column");
	}


	private PersistentPropertyPath<RelationalPersistentProperty> getPath(String path, Class<?> base) {
		return PersistentPropertyPathTestUtils.getPath(context, path, base);
	}
	@SuppressWarnings("unused")
	static class DummyEntity {

		@Id Long id;
		String name;
		ReferencedEntity ref;
		Set<Element> elements;
		Map<Integer, Element> mappedElements;
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

	private static class PrefixingNamingStrategy implements NamingStrategy {

		@Override
		public String getColumnName(RelationalPersistentProperty property) {
			return "x_" + NamingStrategy.super.getColumnName(property);
		}

	}
}
