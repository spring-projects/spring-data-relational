/*
 * Copyright 2019-2020 the original author or authors.
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

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.MappedCollection;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * Unit tests of {@link SqlGenerator} has {@link Embedded} {@link Id}.
 *
 * @author Yunyoung LEE
 */
public class SqlGeneratorEmbeddedIdUnitTests {

	private RelationalMappingContext context = new JdbcMappingContext();
	JdbcConverter converter = new BasicJdbcConverter(context, (identifier, path) -> {
		throw new UnsupportedOperationException();
	});
	private SqlGenerator sqlGenerator;
	private SqlGenerator subSqlGenerator;

	@Before
	public void setUp() {
		this.context.setForceQuote(false);
		this.sqlGenerator = createSqlGenerator(DummyEntity.class);
		this.subSqlGenerator = createSqlGenerator(SubEntity.class);
	}

	SqlGenerator createSqlGenerator(Class<?> type) {
		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(type);
		return new SqlGenerator(context, converter, persistentEntity, NonQuotingDialect.INSTANCE);
	}

	@Test // DATAJDBC-352
	public void findOne() {
		final String sql = sqlGenerator.getFindOne();

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(sql).startsWith("SELECT") //
					.contains("dummy_entity.dummy_id1 AS dummy_id1") //
					.contains("dummy_entity.dummy_id2 AS dummy_id2") //
					.contains("dummy_entity.dummy_attr AS dummy_attr") //
					.contains("WHERE dummy_entity.dummy_id1 = :dummy_id1") //
					.contains("AND dummy_entity.dummy_id2 = :dummy_id2") //
					.doesNotContain("JOIN") //
					.doesNotContain("dummy_entity.id"); //
		});
	}

	@Test // DATAJDBC-352
	public void findAll() {
		final String sql = sqlGenerator.getFindAll();

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(sql).startsWith("SELECT") //
					.contains("dummy_entity.dummy_id1 AS dummy_id1") //
					.contains("dummy_entity.dummy_id2 AS dummy_id2") //
					.contains("dummy_entity.dummy_attr AS dummy_attr") //
					.doesNotContain("JOIN") //
					.doesNotContain("dummy_entity.id");
		});
	}

	@Test // DATAJDBC-352
	public void findAllInList() {
		final String sql = sqlGenerator.getFindAllInList();

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(sql).startsWith("SELECT") //
					.contains("dummy_entity.dummy_id1 AS dummy_id1") //
					.contains("dummy_entity.dummy_id2 AS dummy_id2") //
					.contains("dummy_entity.dummy_attr AS dummy_attr") //
					.contains("WHERE (dummy_entity.dummy_id1, dummy_entity.dummy_id2) IN (:ids)") //
					.doesNotContain("JOIN") //
					.doesNotContain("dummy_entity.id");
		});
	}

	@Test // DATAJDBC-352
	public void findAllByAttribute() {
		final String sql = subSqlGenerator.getFindAllByProperty(Identifier //
				.of(SqlIdentifier.quoted("dummy_id1"), null, Long.class) //
				.withPart(SqlIdentifier.quoted("dummy_id2"), null, Long.class), //
				null, false);

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(sql).startsWith("SELECT") //
					.contains("sub_entity.dummy_id1 AS dummy_id1") //
					.contains("sub_entity.dummy_id2 AS dummy_id2") //
					.contains("sub_entity.sub_id AS sub_id") //
					.contains("sub_entity.sub_attr AS sub_attr") //
					.contains("WHERE sub_entity.dummy_id1 = :dummy_id1") //
					.contains("AND sub_entity.dummy_id2 = :dummy_id2");
		});
	}

	@Test // DATAJDBC-352
	public void insert() {
		final String sql = sqlGenerator.getInsert(Collections.emptySet());

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(sql) //
					.startsWith("INSERT INTO") //
					.contains("dummy_entity") //
					.contains(":dummy_attr");
		});
	}

	@Test // DATAJDBC-352
	public void update() {
		final String sql = sqlGenerator.getUpdate();

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(sql) //
					.startsWith("UPDATE") //
					.contains("dummy_entity") //
					.contains("dummy_attr = :dummy_attr") //
					.contains("WHERE") //
					.contains("dummy_entity.dummy_id1 = :dummy_id1") //
					.contains("dummy_entity.dummy_id2 = :dummy_id2");
		});
	}

	@Test // DATAJDBC-352
	public void deleteById() {
		final String sql = sqlGenerator.getDeleteById();

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(sql) //
					.startsWith("DELETE FROM") //
					.contains("dummy_entity") //
					.contains("WHERE") //
					.contains("dummy_entity.dummy_id1 = :dummy_id1") //
					.contains("dummy_entity.dummy_id2 = :dummy_id2");
		});
	}

	@Data
	static class DummyEntity implements Persistable<DummyEntityId> {
		@Id @Embedded.Nullable DummyEntityId id;

		String dummyAttr;

		@MappedCollection(idColumns = { "DUMMY_ID1", "DUMMY_ID2" }) Set<SubEntity> subEntities;

		@Transient boolean isNew;

		SubEntity createSubEntity(Long subId, String subAttr) {
			final SubEntity subEntity = new SubEntity();
			subEntity.setId(new SubEntityId(id, subId));
			subEntity.setSubAttr(subAttr);

			if (subEntities == null) {
				subEntities = new HashSet<>();
			}

			subEntities.add(subEntity);
			return subEntity;
		}
	}

	@Data
	@AllArgsConstructor
	static class DummyEntityId {
		Long dummyId1;
		Long dummyId2;
	}

	@Data
	static class SubEntity {
		@Id @Embedded.Nullable SubEntityId id;
		String subAttr;
	}

	@Data
	@AllArgsConstructor
	static class SubEntityId {
		@Embedded.Nullable DummyEntityId dummyEntityId;
		Long subId;
	}

}
