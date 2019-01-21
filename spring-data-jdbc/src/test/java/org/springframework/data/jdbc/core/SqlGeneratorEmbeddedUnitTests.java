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

import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.core.mapping.PersistentPropertyPathTestUtils;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link SqlGenerator}.
 *
 * @author Bastian Wilhelm
 */
public class SqlGeneratorEmbeddedUnitTests {

	private SqlGenerator sqlGenerator;

	@Before
	public void setUp() {
		this.sqlGenerator = createSqlGenerator(DummyEntity.class);
	}

	SqlGenerator createSqlGenerator(Class<?> type) {
		RelationalMappingContext context = new JdbcMappingContext();
		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(type);
		return new SqlGenerator(context, persistentEntity, new SqlGeneratorSource(context));
	}

	@Test // DATAJDBC-111
	public void findOne() {
		final String sql = sqlGenerator.getFindOne();

		SoftAssertions softAssertions = new SoftAssertions();
		softAssertions.assertThat(sql)
				.startsWith("SELECT")
				.contains("dummy_entity.id1 AS id1")
				.contains("dummy_entity.attr1 AS attr1")
				.contains("dummy_entity.attr2 AS attr2")
				.contains("dummy_entity.prefix_attr1 AS prefix_attr1")
				.contains("dummy_entity.prefix_attr2 AS prefix_attr2")
				.contains("WHERE dummy_entity.id1 = :id")
				.doesNotContain("JOIN").doesNotContain("embeddable");
		softAssertions.assertAll();
	}

	@Test // DATAJDBC-111
		public void findAll() {
			final String sql = sqlGenerator.getFindAll();

			SoftAssertions softAssertions = new SoftAssertions();
			softAssertions.assertThat(sql)
					.startsWith("SELECT")
					.contains("dummy_entity.id1 AS id1")
					.contains("dummy_entity.attr1 AS attr1")
					.contains("dummy_entity.attr2 AS attr2")
					.contains("dummy_entity.prefix_attr1 AS prefix_attr1")
					.contains("dummy_entity.prefix_attr2 AS prefix_attr2")
					.doesNotContain("JOIN").doesNotContain("embeddable");
			softAssertions.assertAll();
	}

	@Test // DATAJDBC-111
	public void findAllInList() {
		final String sql = sqlGenerator.getFindAllInList();

		SoftAssertions softAssertions = new SoftAssertions();
		softAssertions.assertThat(sql)
				.startsWith("SELECT")
				.contains("dummy_entity.id1 AS id1")
				.contains("dummy_entity.attr1 AS attr1")
				.contains("dummy_entity.attr2 AS attr2")
				.contains("dummy_entity.prefix_attr1 AS prefix_attr1")
				.contains("dummy_entity.prefix_attr2 AS prefix_attr2")
				.contains("WHERE dummy_entity.id1 in(:ids)")
				.doesNotContain("JOIN").doesNotContain("embeddable");
		softAssertions.assertAll();
	}

	@Test // DATAJDBC-111
	public void insert() {
		final String sql = sqlGenerator.getInsert(emptySet());

		SoftAssertions softAssertions = new SoftAssertions();
		softAssertions.assertThat(sql)
				.startsWith("INSERT INTO")
				.contains("dummy_entity")
				.contains(":attr1")
				.contains(":attr2")
				.contains(":prefix_attr1")
				.contains(":prefix_attr2");
		softAssertions.assertAll();
	}

	@Test // DATAJDBC-111
	public void update() {
		final String sql = sqlGenerator.getUpdate();

		SoftAssertions softAssertions = new SoftAssertions();
		softAssertions.assertThat(sql)
				.startsWith("UPDATE")
				.contains("dummy_entity")
				.contains("attr1 = :attr1")
				.contains("attr2 = :attr2")
				.contains("prefix_attr1 = :prefix_attr1")
				.contains("prefix_attr2 = :prefix_attr2");
		softAssertions.assertAll();
	}

	@SuppressWarnings("unused")
	static class DummyEntity {

		@Column("id1")
		@Id
		Long id;

		@Embedded("prefix_")
		Embeddable prefixedEmbeddable;

		@Embedded
		Embeddable embeddable;
	}

	@SuppressWarnings("unused")
	static class Embeddable
	{
		Long attr1;
		String attr2;
	}
}
