/*
 * Copyright 2019-2025 the original author or authors.
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
import static org.assertj.core.api.SoftAssertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.PersistentPropertyPathTestUtils;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.Embedded.OnEmpty;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.sql.Aliased;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.lang.Nullable;

/**
 * Unit tests for the {@link SqlGenerator} in a context of the {@link Embedded} annotation.
 *
 * @author Bastian Wilhelm
 * @author Mark Paluch
 * @author Jens Schauder
 */
class SqlGeneratorEmbeddedUnitTests {

	private final RelationalMappingContext context = new JdbcMappingContext();
	private JdbcConverter converter = new MappingJdbcConverter(context, (identifier, path) -> {
		throw new UnsupportedOperationException();
	});
	private SqlGenerator sqlGenerator;

	@BeforeEach
	void setUp() {
		this.context.setForceQuote(false);
		this.sqlGenerator = createSqlGenerator(DummyEntity.class);
	}

	SqlGenerator createSqlGenerator(Class<?> type) {
		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(type);
		return new SqlGenerator(context, converter, persistentEntity, NonQuotingDialect.INSTANCE);
	}

	@Test // DATAJDBC-111
	void findOne() {
		final String sql = sqlGenerator.getFindOne();

		assertSoftly(softly -> {

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

	@Test // GH-574
	void findOneWrappedId() {

		SqlGenerator sqlGenerator = createSqlGenerator(WithWrappedId.class);

		String sql = sqlGenerator.getFindOne();

		assertSoftly(softly -> {

			softly.assertThat(sql).startsWith("SELECT") //
					.contains("with_wrapped_id.name AS name") //
					.contains("with_wrapped_id.id") //
					.contains("WHERE with_wrapped_id.id = :id");
		});
	}

	@Test // GH-574
	void findOneEmbeddedId() {

		SqlGenerator sqlGenerator = createSqlGenerator(WithEmbeddedId.class);

		String sql = sqlGenerator.getFindOne();

		assertSoftly(softly -> {

			softly.assertThat(sql).startsWith("SELECT") //
					.contains("with_embedded_id.name AS name") //
					.contains("with_embedded_id.one") //
					.contains("with_embedded_id.two") //
					.contains(" WHERE ") //
					.contains("with_embedded_id.one = :one") //
					.contains("with_embedded_id.two = :two");
		});
	}

	@Test // GH-574
	void deleteByIdEmbeddedId() {

		SqlGenerator sqlGenerator = createSqlGenerator(WithEmbeddedId.class);

		String sql = sqlGenerator.getDeleteById();

		assertSoftly(softly -> {

			softly.assertThat(sql).startsWith("DELETE") //
					.contains(" WHERE ") //
					.contains("with_embedded_id.one = :one") //
					.contains("with_embedded_id.two = :two");
		});
	}

	@Test // GH-574
	void deleteByIdInEmbeddedId() {

		SqlGenerator sqlGenerator = createSqlGenerator(WithEmbeddedId.class);

		String sql = sqlGenerator.getDeleteByIdIn();

		assertSoftly(softly -> {

			softly.assertThat(sql).startsWith("DELETE") //
					.contains(" WHERE ") //
					.contains("(with_embedded_id.one, with_embedded_id.two) IN (:ids)");
		});
	}

	@Test // GH-574
	void deleteByPathEmbeddedId() {

		SqlGenerator sqlGenerator = createSqlGenerator(WithEmbeddedId.class);
		PersistentPropertyPath<RelationalPersistentProperty> path = PersistentPropertyPathTestUtils.getPath("other",
				WithEmbeddedIdAndReference.class, context);

		String sql = sqlGenerator.createDeleteByPath(path);

		assertSoftly(softly -> {

			softly.assertThat(sql).startsWith("DELETE FROM other_entity WHERE") //
					.contains("other_entity.with_embedded_id_and_reference_one = :one") //
					.contains("other_entity.with_embedded_id_and_reference_two = :two");
		});
	}

	@Test // GH-574
	void deleteInByPathEmbeddedId() {

		SqlGenerator sqlGenerator = createSqlGenerator(WithEmbeddedId.class);
		PersistentPropertyPath<RelationalPersistentProperty> path = PersistentPropertyPathTestUtils.getPath("other",
				WithEmbeddedIdAndReference.class, context);

		String sql = sqlGenerator.createDeleteInByPath(path);

		assertSoftly(softly -> {

			softly.assertThat(sql).startsWith("DELETE FROM other_entity WHERE") //
					.contains(" WHERE ") //
					.contains(
							"(other_entity.with_embedded_id_and_reference_one, other_entity.with_embedded_id_and_reference_two) IN (:ids)");
		});
	}

	@Test // GH-574
	void updateWithEmbeddedId() {

		SqlGenerator sqlGenerator = createSqlGenerator(WithEmbeddedId.class);

		String sql = sqlGenerator.getUpdate();

		assertSoftly(softly -> {

			softly.assertThat(sql).startsWith("UPDATE") //
					.contains(" WHERE ") //
					.contains("with_embedded_id.one = :one") //
					.contains("with_embedded_id.two = :two");
		});
	}

	@Test // GH-574
	void existsByIdEmbeddedId() {

		SqlGenerator sqlGenerator = createSqlGenerator(WithEmbeddedId.class);

		String sql = sqlGenerator.getExists();

		assertSoftly(softly -> {

			softly.assertThat(sql).startsWith("SELECT COUNT") //
					.contains(" WHERE ") //
					.contains("with_embedded_id.one = :one") //
					.contains("with_embedded_id.two = :two");
		});
	}

	@Test // DATAJDBC-111
	void findAll() {
		final String sql = sqlGenerator.getFindAll();

		assertSoftly(softly -> {

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
	void findAllInList() {

		String sql = sqlGenerator.getFindAllInList();

		assertSoftly(softly -> {

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

	@Test // GH-574
	void findAllInListEmbeddedId() {

		SqlGenerator sqlGenerator = createSqlGenerator(WithEmbeddedId.class);

		String sql = sqlGenerator.getFindAllInList();

		assertSoftly(softly -> {

			softly.assertThat(sql).startsWith("SELECT") //
					.contains("with_embedded_id.name AS name") //
					.contains("with_embedded_id.one") //
					.contains("with_embedded_id.two") //
					.contains(" WHERE (with_embedded_id.one, with_embedded_id.two) IN (:ids)");
		});
	}

	@Test // GH-574
	void findOneWithReference() {

		SqlGenerator sqlGenerator = createSqlGenerator(WithEmbeddedIdAndReference.class);

		String sql = sqlGenerator.getFindOne();

		assertSoftly(softly -> {

			softly.assertThat(sql).startsWith("SELECT") //
					.contains(" LEFT OUTER JOIN other_entity other ") //
					.contains(" ON ") //
					.contains(
							" other.with_embedded_id_and_reference_one = with_embedded_id_and_reference.one ") //
					.contains(
							" other.with_embedded_id_and_reference_two = with_embedded_id_and_reference.two ") //
					.contains(" WHERE ") //
					.contains("with_embedded_id_and_reference.one = :one") //
					.contains("with_embedded_id_and_reference.two = :two");
		});
	}

	@Test // DATAJDBC-111
	void insert() {
		final String sql = sqlGenerator.getInsert(emptySet());

		assertSoftly(softly -> {

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
	void update() {
		final String sql = sqlGenerator.getUpdate();

		assertSoftly(softly -> {

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
	void deleteByPath() {

		sqlGenerator = createSqlGenerator(DummyEntity2.class);

		final String sql = sqlGenerator
				.createDeleteByPath(PersistentPropertyPathTestUtils.getPath("embedded.other", DummyEntity2.class, context));

		assertThat(sql).isEqualTo("DELETE FROM other_entity WHERE other_entity.dummy_entity2 = :id");
	}

	@Test // DATAJDBC-340
	void noJoinForEmbedded() {

		SqlGenerator.Join join = generateJoin("embeddable", DummyEntity.class);

		assertThat(join).isNull();
	}

	@Test // DATAJDBC-340
	void columnForEmbeddedProperty() {

		assertThat(generatedColumn("embeddable.test", DummyEntity.class)) //
				.extracting( //
						c -> c.getName(), //
						c -> c.getTable().getName(), //
						c -> getAlias(c.getTable()), //
						this::getAlias) //
				.containsExactly( //
						SqlIdentifier.unquoted("test"), //
						SqlIdentifier.unquoted("dummy_entity"), //
						null, //
						SqlIdentifier.unquoted("test"));
	}

	@Test // GH-1695
	void columnForEmbeddedPropertyWithPrefix() {
		assertThat(generatedColumn("nested.childId", WithEmbeddedAndAggregateReference.class))
				.hasToString("a.nested_child_id AS nested_child_id");
	}

	@Test // DATAJDBC-340
	void noColumnForEmbedded() {

		assertThat(generatedColumn("embeddable", DummyEntity.class)) //
				.isNull();
	}

	@Test // DATAJDBC-340
	void noJoinForPrefixedEmbedded() {

		SqlGenerator.Join join = generateJoin("prefixedEmbeddable", DummyEntity.class);

		assertThat(join).isNull();
	}

	@Test // DATAJDBC-340
	void columnForPrefixedEmbeddedProperty() {

		assertThat(generatedColumn("prefixedEmbeddable.test", DummyEntity.class)) //
				.extracting( //
						c -> c.getName(), //
						c -> c.getTable().getName(), //
						c -> getAlias(c.getTable()), //
						this::getAlias) //
				.containsExactly( //
						SqlIdentifier.unquoted("prefix_test"), //
						SqlIdentifier.unquoted("dummy_entity"), //
						null, //
						SqlIdentifier.unquoted("prefix_test"));
	}

	@Test // DATAJDBC-340
	void noJoinForCascadedEmbedded() {

		SqlGenerator.Join join = generateJoin("embeddable.embeddable", DummyEntity.class);

		assertThat(join).isNull();
	}

	@Test // DATAJDBC-340
	void columnForCascadedEmbeddedProperty() {

		assertThat(generatedColumn("embeddable.embeddable.attr1", DummyEntity.class)) //
				.extracting(c -> c.getName(), c -> c.getTable().getName(), c -> getAlias(c.getTable()), this::getAlias)
				.containsExactly(SqlIdentifier.unquoted("attr1"), SqlIdentifier.unquoted("dummy_entity"), null,
						SqlIdentifier.unquoted("attr1"));
	}

	@Test // DATAJDBC-340
	void joinForEmbeddedWithReference() {

		SqlGenerator.Join join = generateJoin("embedded.other", DummyEntity2.class);

		assertSoftly(softly -> {
			softly.assertThat(join.joinTable().getName()).isEqualTo(SqlIdentifier.unquoted("other_entity"));
			softly.assertThat(join.condition())
					.isEqualTo(SqlGeneratorUnitTests.equalsCondition("dummy_entity2", "id", join.joinTable(), "dummy_entity2"));
		});
	}

	@Test // DATAJDBC-340
	void columnForEmbeddedWithReferenceProperty() {

		assertThat(generatedColumn("embedded.other.value", DummyEntity2.class)) //
				.extracting( //
						c -> c.getName(), //
						c -> c.getTable().getName(), //
						c -> getAlias(c.getTable()), //
						this::getAlias) //
				.containsExactly( //
						SqlIdentifier.unquoted("value"), //
						SqlIdentifier.unquoted("other_entity"), //
						SqlIdentifier.quoted("prefix_other"), //
						SqlIdentifier.unquoted("prefix_other_value"));
	}

	@Nullable
	private SqlGenerator.Join generateJoin(String path, Class<?> type) {
		return createSqlGenerator(type)
				.getJoin(context.getAggregatePath(PersistentPropertyPathTestUtils.getPath(path, type, context)));
	}

	@Nullable
	private SqlIdentifier getAlias(Object maybeAliased) {

		if (maybeAliased instanceof Aliased) {
			return ((Aliased) maybeAliased).getAlias();
		}
		return null;
	}

	@Nullable
	private org.springframework.data.relational.core.sql.Column generatedColumn(String path, Class<?> type) {

		return createSqlGenerator(type)
				.getColumn(context.getAggregatePath(PersistentPropertyPathTestUtils.getPath(path, type, context)));
	}

	@SuppressWarnings("unused")
	static class DummyEntity {

		@Column("id1")
		@Id Long id;

		@Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "prefix_") CascadedEmbedded prefixedEmbeddable;

		@Embedded(onEmpty = OnEmpty.USE_NULL) CascadedEmbedded embeddable;
	}

	record WrappedId(Long id) {
	}

	static class WithWrappedId {

		@Id
		@Embedded(onEmpty = OnEmpty.USE_NULL) WrappedId wrappedId;

		String name;
	}

	record EmbeddedId(Long one, String two) {
	}

	static class WithEmbeddedId {

		@Id
		@Embedded(onEmpty = OnEmpty.USE_NULL) EmbeddedId embeddedId;

		String name;

	}

	static class WithEmbeddedIdAndReference {

		@Id
		@Embedded(onEmpty = OnEmpty.USE_NULL) EmbeddedId embeddedId;

		String name;
		OtherEntity other;
	}

	@SuppressWarnings("unused")
	static class CascadedEmbedded {
		String test;
		@Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "prefix2_") NoId prefixedEmbeddable;
		@Embedded(onEmpty = OnEmpty.USE_NULL) NoId embeddable;
	}

	@SuppressWarnings("unused")
	static class NoId {
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

	@Table("a")
	private record WithEmbeddedAndAggregateReference(@Id long id,
			@Embedded.Nullable(prefix = "nested_") WithAggregateReference nested) {
	}

	private record WithAggregateReference(AggregateReference<Child, Long> childId) {
	}

	private record Child(@Id long id) {

	}

}
