/*
 * Copyright 2017-2020 the original author or authors.
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

import java.util.Map;
import java.util.Set;

import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.ReadOnlyProperty;
import org.springframework.data.jdbc.core.PropertyPathTestingUtils;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.core.mapping.PersistentPropertyPathTestUtils;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.Aliased;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.domain.Identifier;

/**
 * Unit tests for the {@link SqlGenerator}.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Oleksandr Kucher
 * @author Bastian Wilhelm
 * @author Mark Paluch
 */
public class SqlGeneratorUnitTests {

	static final Identifier BACKREF = Identifier.of("backref", "some-value", String.class);

	SqlGenerator sqlGenerator;
	NamingStrategy namingStrategy = new PrefixingNamingStrategy();
	RelationalMappingContext context = new JdbcMappingContext(namingStrategy);

	@Before
	public void setUp() {
		this.sqlGenerator = createSqlGenerator(DummyEntity.class);
	}

	SqlGenerator createSqlGenerator(Class<?> type) {

		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(type);

		return new SqlGenerator(context, persistentEntity);
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

		assertThat(sql).isEqualTo("DELETE FROM referenced_entity WHERE referenced_entity.dummy_entity = :rootId");
	}

	@Test // DATAJDBC-112
	public void cascadingDeleteByPathSecondLevel() {

		String sql = sqlGenerator.createDeleteByPath(getPath("ref.further", DummyEntity.class));

		assertThat(sql).isEqualTo(
				"DELETE FROM second_level_referenced_entity WHERE second_level_referenced_entity.referenced_entity IN (SELECT referenced_entity.x_l1id FROM referenced_entity WHERE referenced_entity.dummy_entity = :rootId)");
	}

	@Test // DATAJDBC-112
	public void deleteAll() {

		String sql = sqlGenerator.createDeleteAllSql(null);

		assertThat(sql).isEqualTo("DELETE FROM dummy_entity");
	}

	@Test // DATAJDBC-112
	public void cascadingDeleteAllFirstLevel() {

		String sql = sqlGenerator.createDeleteAllSql(getPath("ref", DummyEntity.class));

		assertThat(sql).isEqualTo("DELETE FROM referenced_entity WHERE referenced_entity.dummy_entity IS NOT NULL");
	}

	@Test // DATAJDBC-112
	public void cascadingDeleteAllSecondLevel() {

		String sql = sqlGenerator.createDeleteAllSql(getPath("ref.further", DummyEntity.class));

		assertThat(sql).isEqualTo(
				"DELETE FROM second_level_referenced_entity WHERE second_level_referenced_entity.referenced_entity IN (SELECT referenced_entity.x_l1id FROM referenced_entity WHERE referenced_entity.dummy_entity IS NOT NULL)");
	}

	@Test // DATAJDBC-227
	public void deleteAllMap() {

		String sql = sqlGenerator.createDeleteAllSql(getPath("mappedElements", DummyEntity.class));

		assertThat(sql).isEqualTo("DELETE FROM element WHERE element.dummy_entity IS NOT NULL");
	}

	@Test // DATAJDBC-227
	public void deleteMapByPath() {

		String sql = sqlGenerator.createDeleteByPath(getPath("mappedElements", DummyEntity.class));

		assertThat(sql).isEqualTo("DELETE FROM element WHERE element.dummy_entity = :rootId");
	}

	@Test // DATAJDBC-131, DATAJDBC-111
	public void findAllByProperty() {

		// this would get called when ListParent is the element type of a Set
		String sql = sqlGenerator.getFindAllByProperty(BACKREF, null, false);

		assertThat(sql).contains("SELECT", //
				"dummy_entity.id1 AS id1", //
				"dummy_entity.x_name AS x_name", //
				"dummy_entity.x_other AS x_other", //
				"ref.x_l1id AS ref_x_l1id", //
				"ref.x_content AS ref_x_content", //
				"ref_further.x_l2id AS ref_further_x_l2id", //
				"ref_further.x_something AS ref_further_x_something", //
				"FROM dummy_entity ", //
				"LEFT OUTER JOIN referenced_entity AS ref ON ref.dummy_entity = dummy_entity.id1", //
				"LEFT OUTER JOIN second_level_referenced_entity AS ref_further ON ref_further.referenced_entity = ref.x_l1id", //
				"WHERE dummy_entity.backref = :backref");
	}

	@Test // DATAJDBC-223
	public void findAllByPropertyWithMultipartIdentifier() {

		// this would get called when ListParent is the element type of a Set
		String sql = sqlGenerator.getFindAllByProperty(Identifier.of("backref", "some-value", String.class).withPart("backref_key", "key-value", Object.class), null, false);

		assertThat(sql).contains("SELECT", //
				"dummy_entity.id1 AS id1", //
				"dummy_entity.x_name AS x_name", //
				"dummy_entity.x_other AS x_other", //
				"ref.x_l1id AS ref_x_l1id", //
				"ref.x_content AS ref_x_content", //
				"ref_further.x_l2id AS ref_further_x_l2id", //
				"ref_further.x_something AS ref_further_x_something", //
				"FROM dummy_entity ", //
				"LEFT OUTER JOIN referenced_entity AS ref ON ref.dummy_entity = dummy_entity.id1", //
				"LEFT OUTER JOIN second_level_referenced_entity AS ref_further ON ref_further.referenced_entity = ref.x_l1id", //
				"dummy_entity.backref = :backref",
				"dummy_entity.backref_key = :backref_key"
		);
	}

	@Test // DATAJDBC-131, DATAJDBC-111
	public void findAllByPropertyWithKey() {

		// this would get called when ListParent is th element type of a Map
		String sql = sqlGenerator.getFindAllByProperty(BACKREF, "key-column", false);

		assertThat(sql).isEqualTo("SELECT dummy_entity.id1 AS id1, dummy_entity.x_name AS x_name, " //
				+ "dummy_entity.x_other AS x_other, " //
				+ "ref.x_l1id AS ref_x_l1id, ref.x_content AS ref_x_content, "
				+ "ref_further.x_l2id AS ref_further_x_l2id, ref_further.x_something AS ref_further_x_something, " //
				+ "dummy_entity.key-column AS key-column " //
				+ "FROM dummy_entity " //
				+ "LEFT OUTER JOIN referenced_entity AS ref ON ref.dummy_entity = dummy_entity.id1 " //
				+ "LEFT OUTER JOIN second_level_referenced_entity AS ref_further ON ref_further.referenced_entity = ref.x_l1id " //
				+ "WHERE dummy_entity.backref = :backref");
	}

	@Test(expected = IllegalArgumentException.class) // DATAJDBC-130
	public void findAllByPropertyOrderedWithoutKey() {
		sqlGenerator.getFindAllByProperty(BACKREF, null, true);
	}

	@Test // DATAJDBC-131, DATAJDBC-111
	public void findAllByPropertyWithKeyOrdered() {

		// this would get called when ListParent is th element type of a Map
		String sql = sqlGenerator.getFindAllByProperty(BACKREF, "key-column", true);

		assertThat(sql).isEqualTo("SELECT dummy_entity.id1 AS id1, dummy_entity.x_name AS x_name, " //
				+ "dummy_entity.x_other AS x_other, " //
				+ "ref.x_l1id AS ref_x_l1id, ref.x_content AS ref_x_content, "
				+ "ref_further.x_l2id AS ref_further_x_l2id, ref_further.x_something AS ref_further_x_something, " //
				+ "dummy_entity.key-column AS key-column " //
				+ "FROM dummy_entity " //
				+ "LEFT OUTER JOIN referenced_entity AS ref ON ref.dummy_entity = dummy_entity.id1 " //
				+ "LEFT OUTER JOIN second_level_referenced_entity AS ref_further ON ref_further.referenced_entity = ref.x_l1id " //
				+ "WHERE dummy_entity.backref = :backref " + "ORDER BY key-column");
	}

	@Test // DATAJDBC-264
	public void getInsertForEmptyColumnList() {

		SqlGenerator sqlGenerator = createSqlGenerator(IdOnlyEntity.class);

		String insert = sqlGenerator.getInsert(emptySet());

		assertThat(insert).endsWith("()");
	}

	@Test // DATAJDBC-334
	public void getInsertForQuotedColumnName() {

		SqlGenerator sqlGenerator = createSqlGenerator(EntityWithQuotedColumnName.class);

		String insert = sqlGenerator.getInsert(emptySet());

		assertThat(insert)
				.isEqualTo("INSERT INTO entity_with_quoted_column_name " + "(\"test_@123\") " + "VALUES (:test_123)");
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
						+ "WHERE entity_with_read_only_property.x_id = :x_id" //
		);
	}

	@Test // DATAJDBC-334
	public void getUpdateForQuotedColumnName() {

		SqlGenerator sqlGenerator = createSqlGenerator(EntityWithQuotedColumnName.class);

		String update = sqlGenerator.getUpdate();

		assertThat(update).isEqualTo("UPDATE entity_with_quoted_column_name " + "SET \"test_@123\" = :test_123 "
				+ "WHERE entity_with_quoted_column_name.\"test_@id\" = :test_id");
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

		assertThat(sqlGenerator.getFindAllByProperty(BACKREF, "key-column", true)).isEqualToIgnoringCase( //
				"SELECT " //
						+ "entity_with_read_only_property.x_id AS x_id, " //
						+ "entity_with_read_only_property.x_name AS x_name, " //
						+ "entity_with_read_only_property.x_read_only_value AS x_read_only_value, " //
						+ "entity_with_read_only_property.key-column AS key-column " //
						+ "FROM entity_with_read_only_property " //
						+ "WHERE entity_with_read_only_property.backref = :backref " //
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
						+ "WHERE entity_with_read_only_property.x_id IN (:ids)" //
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

	@Test // DATAJDBC-340
	public void deletingLongChain() {

		assertThat(
				createSqlGenerator(Chain4.class).createDeleteByPath(getPath("chain3.chain2.chain1.chain0", Chain4.class))) //
						.isEqualTo("DELETE FROM chain0 " + //
								"WHERE chain0.chain1 IN (" + //
								"SELECT chain1.x_one " + //
								"FROM chain1 " + //
								"WHERE chain1.chain2 IN (" + //
								"SELECT chain2.x_two " + //
								"FROM chain2 " + //
								"WHERE chain2.chain3 IN (" + //
								"SELECT chain3.x_three " + //
								"FROM chain3 " + //
								"WHERE chain3.chain4 = :rootId" + //
								")))");
	}

	@Test // DATAJDBC-359
	public void deletingLongChainNoId() {

		assertThat(createSqlGenerator(NoIdChain4.class)
				.createDeleteByPath(getPath("chain3.chain2.chain1.chain0", NoIdChain4.class))) //
						.isEqualTo("DELETE FROM no_id_chain0 WHERE no_id_chain0.no_id_chain4 = :rootId");
	}

	@Test // DATAJDBC-359
	public void deletingLongChainNoIdWithBackreferenceNotReferencingTheRoot() {

		assertThat(createSqlGenerator(IdIdNoIdChain.class)
				.createDeleteByPath(getPath("idNoIdChain.chain4.chain3.chain2.chain1.chain0", IdIdNoIdChain.class))) //
						.isEqualTo( //
								"DELETE FROM no_id_chain0 " //
										+ "WHERE no_id_chain0.no_id_chain4 IN (" //
										+ "SELECT no_id_chain4.x_four " //
										+ "FROM no_id_chain4 " //
										+ "WHERE no_id_chain4.id_no_id_chain IN (" //
										+ "SELECT id_no_id_chain.x_id " //
										+ "FROM id_no_id_chain " //
										+ "WHERE id_no_id_chain.id_id_no_id_chain = :rootId" //
										+ "))");
	}

	@Test // DATAJDBC-340
	public void noJoinForSimpleColumn() {
		assertThat(generateJoin("id", DummyEntity.class)).isNull();
	}

	@Test // DATAJDBC-340
	public void joinForSimpleReference() {

		SoftAssertions.assertSoftly(softly -> {

			SqlGenerator.Join join = generateJoin("ref", DummyEntity.class);
			softly.assertThat(join.getJoinTable().getName()).isEqualTo("referenced_entity");
			softly.assertThat(join.getJoinColumn().getTable()).isEqualTo(join.getJoinTable());
			softly.assertThat(join.getJoinColumn().getName()).isEqualTo("dummy_entity");
			softly.assertThat(join.getParentId().getName()).isEqualTo("id1");
			softly.assertThat(join.getParentId().getTable().getName()).isEqualTo("dummy_entity");
		});
	}

	@Test // DATAJDBC-340
	public void noJoinForCollectionReference() {

		SqlGenerator.Join join = generateJoin("elements", DummyEntity.class);

		assertThat(join).isNull();

	}

	@Test // DATAJDBC-340
	public void noJoinForMappedReference() {

		SqlGenerator.Join join = generateJoin("mappedElements", DummyEntity.class);

		assertThat(join).isNull();
	}

	@Test // DATAJDBC-340
	public void joinForSecondLevelReference() {

		SoftAssertions.assertSoftly(softly -> {

			SqlGenerator.Join join = generateJoin("ref.further", DummyEntity.class);
			softly.assertThat(join.getJoinTable().getName()).isEqualTo("second_level_referenced_entity");
			softly.assertThat(join.getJoinColumn().getTable()).isEqualTo(join.getJoinTable());
			softly.assertThat(join.getJoinColumn().getName()).isEqualTo("referenced_entity");
			softly.assertThat(join.getParentId().getName()).isEqualTo("x_l1id");
			softly.assertThat(join.getParentId().getTable().getName()).isEqualTo("referenced_entity");
		});
	}

	@Test // DATAJDBC-340
	public void joinForOneToOneWithoutId() {

		SoftAssertions.assertSoftly(softly -> {

			SqlGenerator.Join join = generateJoin("child", ParentOfNoIdChild.class);
			Table joinTable = join.getJoinTable();
			softly.assertThat(joinTable.getName()).isEqualTo("no_id_child");
			softly.assertThat(joinTable).isInstanceOf(Aliased.class);
			softly.assertThat(((Aliased) joinTable).getAlias()).isEqualTo("child");
			softly.assertThat(join.getJoinColumn().getTable()).isEqualTo(joinTable);
			softly.assertThat(join.getJoinColumn().getName()).isEqualTo("parent_of_no_id_child");
			softly.assertThat(join.getParentId().getName()).isEqualTo("x_id");
			softly.assertThat(join.getParentId().getTable().getName()).isEqualTo("parent_of_no_id_child");

		});
	}

	private SqlGenerator.Join generateJoin(String path, Class<?> type) {
		return createSqlGenerator(type)
				.getJoin(new PersistentPropertyPathExtension(context, PropertyPathTestingUtils.toPath(path, type, context)));
	}

	@Test // DATAJDBC-340
	public void simpleColumn() {

		assertThat(generatedColumn("id", DummyEntity.class)) //
				.extracting(c -> c.getName(), c -> c.getTable().getName(), c -> getAlias(c.getTable()), this::getAlias)
				.containsExactly("id1", "dummy_entity", null, "id1");
	}

	@Test // DATAJDBC-340
	public void columnForIndirectProperty() {

		assertThat(generatedColumn("ref.l1id", DummyEntity.class)) //
				.extracting(c -> c.getName(), c -> c.getTable().getName(), c -> getAlias(c.getTable()), this::getAlias) //
				.containsExactly("x_l1id", "referenced_entity", "ref", "ref_x_l1id");
	}

	@Test // DATAJDBC-340
	public void noColumnForReferencedEntity() {

		assertThat(generatedColumn("ref", DummyEntity.class)).isNull();
	}

	@Test // DATAJDBC-340
	public void columnForReferencedEntityWithoutId() {

		assertThat(generatedColumn("child", ParentOfNoIdChild.class)) //
				.extracting(c -> c.getName(), c -> c.getTable().getName(), c -> getAlias(c.getTable()), this::getAlias) //
				.containsExactly("parent_of_no_id_child", "no_id_child", "child", "child_parent_of_no_id_child");
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

	private PersistentPropertyPath<RelationalPersistentProperty> getPath(String path, Class<?> baseType) {
		return PersistentPropertyPathTestUtils.getPath(context, path, baseType);
	}

	@SuppressWarnings("unused")
	static class DummyEntity {

		@Column("id1") @Id Long id;
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

	@SuppressWarnings("unused")
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

	@SuppressWarnings("unused")
	static class EntityWithReadOnlyProperty {

		@Id Long id;
		String name;
		@ReadOnlyProperty String readOnlyValue;
	}

	static class EntityWithQuotedColumnName {

		@Id @Column("\"test_@id\"") Long id;
		@Column("\"test_@123\"") String name;
	}

	@SuppressWarnings("unused")
	static class Chain0 {
		@Id Long zero;
		String zeroValue;
	}

	@SuppressWarnings("unused")
	static class Chain1 {
		@Id Long one;
		String oneValue;
		Chain0 chain0;
	}

	@SuppressWarnings("unused")
	static class Chain2 {
		@Id Long two;
		String twoValue;
		Chain1 chain1;
	}

	@SuppressWarnings("unused")
	static class Chain3 {
		@Id Long three;
		String threeValue;
		Chain2 chain2;
	}

	@SuppressWarnings("unused")
	static class Chain4 {
		@Id Long four;
		String fourValue;
		Chain3 chain3;
	}

	static class NoIdChain0 {
		String zeroValue;
	}

	static class NoIdChain1 {
		String oneValue;
		NoIdChain0 chain0;
	}

	static class NoIdChain2 {
		String twoValue;
		NoIdChain1 chain1;
	}

	static class NoIdChain3 {
		String threeValue;
		NoIdChain2 chain2;
	}

	static class NoIdChain4 {
		@Id Long four;
		String fourValue;
		NoIdChain3 chain3;
	}

	static class IdNoIdChain {
		@Id Long id;
		NoIdChain4 chain4;
	}

	static class IdIdNoIdChain {
		@Id Long id;
		IdNoIdChain idNoIdChain;
	}
}
