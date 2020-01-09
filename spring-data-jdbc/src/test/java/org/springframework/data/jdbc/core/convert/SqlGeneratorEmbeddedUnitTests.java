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

import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;

import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.PropertyPathTestingUtils;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.Embedded.OnEmpty;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.sql.Aliased;
import org.springframework.data.relational.domain.IdentifierProcessing;
import org.springframework.data.relational.domain.IdentifierProcessing.LetterCasing;
import org.springframework.data.relational.domain.IdentifierProcessing.Quoting;

/**
 * Unit tests for the {@link SqlGenerator} in a context of the {@link Embedded} annotation.
 *
 * @author Bastian Wilhelm
 */
public class SqlGeneratorEmbeddedUnitTests {

	private RelationalMappingContext context = new JdbcMappingContext();
	private SqlGenerator sqlGenerator;

	@Before
	public void setUp() {
		this.sqlGenerator = createSqlGenerator(DummyEntity.class);
	}

	SqlGenerator createSqlGenerator(Class<?> type) {
		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(type);
		return new SqlGenerator(context, persistentEntity,
				IdentifierProcessing.create(new Quoting(""), LetterCasing.AS_IS));
	}

	@Test // DATAJDBC-111
	public void findOne() {
		final String sql = sqlGenerator.getFindOne();

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(sql).startsWith("SELECT") //
					.contains("dummy_entity.id1 AS id1") //
					.contains("dummy_entity.test AS test") //
					.contains("dummy_entity.attr1 AS attr1") //
					.contains("dummy_entity.attr2 AS attr2") //
					.contains("dummy_entity.prefix2_attr1 AS prefix2_attr1") //
					.contains("dummy_entity.prefix2_attr2 AS prefix2_attr2") //
					.contains("dummy_entity.prefix_test AS prefix_test") //
					.contains("dummy_entity.prefix_attr1 AS prefix_attr1") //
					.contains("dummy_entity.prefix_attr2 AS prefix_attr2") //
					.contains("dummy_entity.prefix_prefix2_attr1 AS prefix_prefix2_attr1") //
					.contains("dummy_entity.prefix_prefix2_attr2 AS prefix_prefix2_attr2") //
					.contains("WHERE dummy_entity.id1 = :id") //
					.doesNotContain("JOIN").doesNotContain("embeddable"); //
		});
	}

	@Test // DATAJDBC-111
	public void findAll() {
		final String sql = sqlGenerator.getFindAll();

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(sql).startsWith("SELECT") //
					.contains("dummy_entity.id1 AS id1") //
					.contains("dummy_entity.test AS test") //
					.contains("dummy_entity.attr1 AS attr1") //
					.contains("dummy_entity.attr2 AS attr2") //
					.contains("dummy_entity.prefix2_attr1 AS prefix2_attr1") //
					.contains("dummy_entity.prefix2_attr2 AS prefix2_attr2") //
					.contains("dummy_entity.prefix_test AS prefix_test") //
					.contains("dummy_entity.prefix_attr1 AS prefix_attr1") //
					.contains("dummy_entity.prefix_attr2 AS prefix_attr2") //
					.contains("dummy_entity.prefix_prefix2_attr1 AS prefix_prefix2_attr1") //
					.contains("dummy_entity.prefix_prefix2_attr2 AS prefix_prefix2_attr2") //
					.doesNotContain("JOIN") //
					.doesNotContain("embeddable");
		});
	}

	@Test // DATAJDBC-111
	public void findAllInList() {
		final String sql = sqlGenerator.getFindAllInList();

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(sql).startsWith("SELECT") //
					.contains("dummy_entity.id1 AS id1") //
					.contains("dummy_entity.test AS test") //
					.contains("dummy_entity.attr1 AS attr1") //
					.contains("dummy_entity.attr2 AS attr2").contains("dummy_entity.prefix2_attr1 AS prefix2_attr1") //
					.contains("dummy_entity.prefix2_attr2 AS prefix2_attr2") //
					.contains("dummy_entity.prefix_test AS prefix_test") //
					.contains("dummy_entity.prefix_attr1 AS prefix_attr1") //
					.contains("dummy_entity.prefix_attr2 AS prefix_attr2") //
					.contains("dummy_entity.prefix_prefix2_attr1 AS prefix_prefix2_attr1") //
					.contains("dummy_entity.prefix_prefix2_attr2 AS prefix_prefix2_attr2") //
					.contains("WHERE dummy_entity.id1 IN (:ids)") //
					.doesNotContain("JOIN") //
					.doesNotContain("embeddable");
		});
	}

	@Test // DATAJDBC-111
	public void insert() {
		final String sql = sqlGenerator.getInsert(emptySet());

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(sql) //
					.startsWith("INSERT INTO") //
					.contains("dummy_entity") //
					.contains(":test") //
					.contains(":attr1") //
					.contains(":attr2") //
					.contains(":prefix2_attr1") //
					.contains(":prefix2_attr2") //
					.contains(":prefix_test") //
					.contains(":prefix_attr1") //
					.contains(":prefix_attr2") //
					.contains(":prefix_prefix2_attr1") //
					.contains(":prefix_prefix2_attr2");
		});
	}

	@Test // DATAJDBC-111
	public void update() {
		final String sql = sqlGenerator.getUpdate();

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(sql) //
					.startsWith("UPDATE") //
					.contains("dummy_entity") //
					.contains("test = :test") //
					.contains("attr1 = :attr1") //
					.contains("attr2 = :attr2") //
					.contains("prefix2_attr1 = :prefix2_attr1") //
					.contains("prefix2_attr2 = :prefix2_attr2") //
					.contains("prefix_test = :prefix_test") //
					.contains("prefix_attr1 = :prefix_attr1") //
					.contains("prefix_attr2 = :prefix_attr2") //
					.contains("prefix_prefix2_attr1 = :prefix_prefix2_attr1") //
					.contains("prefix_prefix2_attr2 = :prefix_prefix2_attr2");
		});
	}

	@Test // DATAJDBC-340
	@Ignore // this is just broken right now
	public void deleteByPath() {

		final String sql = sqlGenerator
				.createDeleteByPath(PropertyPathTestingUtils.toPath("embedded.other", DummyEntity2.class, context));

		assertThat(sql).containsSequence("DELETE FROM other_entity", //
				"WHERE", //
				"embedded_with_reference IN (", //
				"SELECT ", //
				"id ", //
				"FROM", //
				"dummy_entity2", //
				"WHERE", //
				"embedded_with_reference = :rootId");
	}

	@Test // DATAJDBC-340
	public void noJoinForEmbedded() {

		SqlGenerator.Join join = generateJoin("embeddable", DummyEntity.class);

		assertThat(join).isNull();
	}

	@Test // DATAJDBC-340
	public void columnForEmbeddedProperty() {

		assertThat(generatedColumn("embeddable.test", DummyEntity.class)) //
				.extracting(c -> c.getName(), c -> c.getTable().getName(), c -> getAlias(c.getTable()), this::getAlias)
				.containsExactly("test", "dummy_entity", null, "test");
	}

	@Test // DATAJDBC-340
	public void noColumnForEmbedded() {

		assertThat(generatedColumn("embeddable", DummyEntity.class)) //
				.isNull();
	}

	@Test // DATAJDBC-340
	public void noJoinForPrefixedEmbedded() {

		SqlGenerator.Join join = generateJoin("prefixedEmbeddable", DummyEntity.class);

		assertThat(join).isNull();
	}

	@Test // DATAJDBC-340
	public void columnForPrefixedEmbeddedProperty() {

		assertThat(generatedColumn("prefixedEmbeddable.test", DummyEntity.class)) //
				.extracting(c -> c.getName(), c -> c.getTable().getName(), c -> getAlias(c.getTable()), this::getAlias)
				.containsExactly("prefix_test", "dummy_entity", null, "prefix_test");
	}

	@Test // DATAJDBC-340
	public void noJoinForCascadedEmbedded() {

		SqlGenerator.Join join = generateJoin("embeddable.embeddable", DummyEntity.class);

		assertThat(join).isNull();
	}

	@Test // DATAJDBC-340
	public void columnForCascadedEmbeddedProperty() {

		assertThat(generatedColumn("embeddable.embeddable.attr1", DummyEntity.class)) //
				.extracting(c -> c.getName(), c -> c.getTable().getName(), c -> getAlias(c.getTable()), this::getAlias)
				.containsExactly("attr1", "dummy_entity", null, "attr1");
	}

	@Test // DATAJDBC-340
	public void joinForEmbeddedWithReference() {

		SqlGenerator.Join join = generateJoin("embedded.other", DummyEntity2.class);

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(join.getJoinTable().getName()).isEqualTo("other_entity");
			softly.assertThat(join.getJoinColumn().getTable()).isEqualTo(join.getJoinTable());
			softly.assertThat(join.getJoinColumn().getName()).isEqualTo("dummy_entity2");
			softly.assertThat(join.getParentId().getName()).isEqualTo("id");
			softly.assertThat(join.getParentId().getTable().getName()).isEqualTo("dummy_entity2");
		});
	}

	@Test // DATAJDBC-340
	public void columnForEmbeddedWithReferenceProperty() {

		assertThat(generatedColumn("embedded.other.value", DummyEntity2.class)) //
				.extracting(c -> c.getName(), c -> c.getTable().getName(), c -> getAlias(c.getTable()), this::getAlias)
				.containsExactly("value", "other_entity", "prefix_other", "prefix_other_value");
	}

	private SqlGenerator.Join generateJoin(String path, Class<?> type) {
		return createSqlGenerator(type)
				.getJoin(new PersistentPropertyPathExtension(context, PropertyPathTestingUtils.toPath(path, type, context)));
	}

	private String getAlias(Object maybeAliased) {

		if (maybeAliased instanceof Aliased) {
			return ((Aliased) maybeAliased).getAlias();
		}
		return null;
	}

	private org.springframework.data.relational.core.sql.Column generatedColumn(String path, Class<?> type) {
		return createSqlGenerator(type)
				.getColumn(new PersistentPropertyPathExtension(context, PropertyPathTestingUtils.toPath(path, type, context)));
	}

	@SuppressWarnings("unused")
	static class DummyEntity {

		@Column("id1") @Id Long id;

		@Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "prefix_") CascadedEmbedded prefixedEmbeddable;

		@Embedded(onEmpty = OnEmpty.USE_NULL) CascadedEmbedded embeddable;
	}

	@SuppressWarnings("unused")
	static class CascadedEmbedded {
		String test;
		@Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "prefix2_") Embeddable prefixedEmbeddable;
		@Embedded(onEmpty = OnEmpty.USE_NULL) Embeddable embeddable;
	}

	@SuppressWarnings("unused")
	static class Embeddable {
		Long attr1;
		String attr2;
	}

	@SuppressWarnings("unused")
	static class DummyEntity2 {

		@Id Long id;

		@Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "prefix_") EmbeddedWithReference embedded;
	}

	static class EmbeddedWithReference {
		OtherEntity other;
	}

	static class OtherEntity {
		String value;
	}

}
