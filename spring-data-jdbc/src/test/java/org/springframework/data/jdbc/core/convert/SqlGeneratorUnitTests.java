/*
 * Copyright 2017-2024 the original author or authors.
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
import static org.springframework.data.relational.core.mapping.ForeignKeyNaming.*;
import static org.springframework.data.relational.core.sql.SqlIdentifier.*;

import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.ReadOnlyProperty;
import org.springframework.data.annotation.Version;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.PersistentPropertyPathTestUtils;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.dialect.AnsiDialect;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.PostgresDialect;
import org.springframework.data.relational.core.dialect.SqlServerDialect;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.DefaultNamingStrategy;
import org.springframework.data.relational.core.mapping.MappedCollection;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.sql.Aliased;
import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.lang.Nullable;

/**
 * Unit tests for the {@link SqlGenerator}.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Oleksandr Kucher
 * @author Bastian Wilhelm
 * @author Mark Paluch
 * @author Tom Hombergs
 * @author Milan Milanov
 * @author Myeonghyeon Lee
 * @author Mikhail Polivakha
 * @author Chirag Tailor
 * @author Diego Krupitza
 * @author Hari Ohm Prasath
 * @author Viktor Ardelean
 */
@SuppressWarnings("Convert2MethodRef")
class SqlGeneratorUnitTests {

	private static final Identifier BACKREF = Identifier.of(unquoted("backref"), "some-value", String.class);

	private final PrefixingNamingStrategy namingStrategy = new PrefixingNamingStrategy();
	private RelationalMappingContext context = new JdbcMappingContext(namingStrategy);
	private final JdbcConverter converter = new MappingJdbcConverter(context, (identifier, path) -> {
		throw new UnsupportedOperationException();
	});
	private SqlGenerator sqlGenerator;

	@BeforeEach
	void setUp() {
		this.sqlGenerator = createSqlGenerator(DummyEntity.class);
	}

	SqlGenerator createSqlGenerator(Class<?> type) {

		return createSqlGenerator(type, NonQuotingDialect.INSTANCE);
	}

	SqlGenerator createSqlGenerator(Class<?> type, Dialect dialect) {

		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(type);

		return new SqlGenerator(context, converter, persistentEntity, dialect);
	}

	@Test // DATAJDBC-112
	void findOne() {

		String sql = sqlGenerator.getFindOne();

		assertSoftly(softly -> softly //
				.assertThat(sql) //
				.startsWith("SELECT") //
				.contains("dummy_entity.id1 AS id1,") //
				.contains("dummy_entity.x_name AS x_name,") //
				.contains("dummy_entity.x_other AS x_other,") //
				.contains("ref.x_l1id AS ref_x_l1id") //
				.contains("ref.x_content AS ref_x_content").contains(" FROM dummy_entity") //
				.contains("ON ref.dummy_entity = dummy_entity.id1") //
				.contains("WHERE dummy_entity.id1 = :id") //
				// 1-N relationships do not get loaded via join
				.doesNotContain("Element AS elements"));
	}

	@Test // DATAJDBC-493
	void getAcquireLockById() {

		String sql = sqlGenerator.getAcquireLockById(LockMode.PESSIMISTIC_WRITE);

		assertSoftly(softly -> softly //
				.assertThat(sql) //
				.startsWith("SELECT") //
				.contains("dummy_entity.id1") //
				.contains("WHERE dummy_entity.id1 = :id") //
				.contains("FOR UPDATE") //
				.doesNotContain("Element AS elements"));
	}

	@Test // DATAJDBC-493
	void getAcquireLockAll() {

		String sql = sqlGenerator.getAcquireLockAll(LockMode.PESSIMISTIC_WRITE);

		assertSoftly(softly -> softly //
				.assertThat(sql) //
				.startsWith("SELECT") //
				.contains("dummy_entity.id1") //
				.contains("FOR UPDATE") //
				.doesNotContain("Element AS elements"));
	}

	@Test // DATAJDBC-112
	void cascadingDeleteFirstLevel() {

		String sql = sqlGenerator.createDeleteByPath(getPath("ref", DummyEntity.class));

		assertThat(sql).isEqualTo("DELETE FROM referenced_entity WHERE referenced_entity.dummy_entity = :rootId");
	}

	@Test // GH-537
	void cascadingDeleteInByPathFirstLevel() {

		String sql = sqlGenerator.createDeleteInByPath(getPath("ref", DummyEntity.class));

		assertThat(sql).isEqualTo("DELETE FROM referenced_entity WHERE referenced_entity.dummy_entity IN (:ids)");
	}

	@Test // DATAJDBC-112
	void cascadingDeleteByPathSecondLevel() {

		String sql = sqlGenerator.createDeleteByPath(getPath("ref.further", DummyEntity.class));

		assertThat(sql).isEqualTo(
				"DELETE FROM second_level_referenced_entity WHERE second_level_referenced_entity.referenced_entity IN (SELECT referenced_entity.x_l1id FROM referenced_entity WHERE referenced_entity.dummy_entity = :rootId)");
	}

	@Test // GH-537
	void cascadingDeleteInByPathSecondLevel() {

		String sql = sqlGenerator.createDeleteInByPath(getPath("ref.further", DummyEntity.class));

		assertThat(sql).isEqualTo(
				"DELETE FROM second_level_referenced_entity WHERE second_level_referenced_entity.referenced_entity IN (SELECT referenced_entity.x_l1id FROM referenced_entity WHERE referenced_entity.dummy_entity IN (:ids))");
	}

	@Test // DATAJDBC-112
	void deleteAll() {

		String sql = sqlGenerator.createDeleteAllSql(null);

		assertThat(sql).isEqualTo("DELETE FROM dummy_entity");
	}

	@Test // DATAJDBC-112
	void cascadingDeleteAllFirstLevel() {

		String sql = sqlGenerator.createDeleteAllSql(getPath("ref", DummyEntity.class));

		assertThat(sql).isEqualTo("DELETE FROM referenced_entity WHERE referenced_entity.dummy_entity IS NOT NULL");
	}

	@Test // DATAJDBC-112
	void cascadingDeleteAllSecondLevel() {

		String sql = sqlGenerator.createDeleteAllSql(getPath("ref.further", DummyEntity.class));

		assertThat(sql).isEqualTo(
				"DELETE FROM second_level_referenced_entity WHERE second_level_referenced_entity.referenced_entity IN (SELECT referenced_entity.x_l1id FROM referenced_entity WHERE referenced_entity.dummy_entity IS NOT NULL)");
	}

	@Test // DATAJDBC-227
	void deleteAllMap() {

		String sql = sqlGenerator.createDeleteAllSql(getPath("mappedElements", DummyEntity.class));

		assertThat(sql).isEqualTo("DELETE FROM element WHERE element.dummy_entity IS NOT NULL");
	}

	@Test // DATAJDBC-227
	void deleteMapByPath() {

		String sql = sqlGenerator.createDeleteByPath(getPath("mappedElements", DummyEntity.class));

		assertThat(sql).isEqualTo("DELETE FROM element WHERE element.dummy_entity = :rootId");
	}

	@Test // DATAJDBC-101
	void findAllSortedByUnsorted() {

		String sql = sqlGenerator.getFindAll(Sort.unsorted());

		assertThat(sql).doesNotContain("ORDER BY");
	}

	@Test // DATAJDBC-101
	void findAllSortedBySingleField() {

		String sql = sqlGenerator.getFindAll(Sort.by("name"));

		assertThat(sql).contains("SELECT", //
				"dummy_entity.id1 AS id1", //
				"dummy_entity.x_name AS x_name", //
				"dummy_entity.x_other AS x_other", //
				"ref.x_l1id AS ref_x_l1id", //
				"ref.x_content AS ref_x_content", //
				"ref_further.x_l2id AS ref_further_x_l2id", //
				"ref_further.x_something AS ref_further_x_something", //
				"FROM dummy_entity ", //
				"LEFT OUTER JOIN referenced_entity ref ON ref.dummy_entity = dummy_entity.id1", //
				"LEFT OUTER JOIN second_level_referenced_entity ref_further ON ref_further.referenced_entity = ref.x_l1id", //
				"ORDER BY dummy_entity.x_name ASC");
	}

	@Test // DATAJDBC-101
	void findAllSortedByMultipleFields() {

		String sql = sqlGenerator
				.getFindAll(Sort.by(new Sort.Order(Sort.Direction.DESC, "name"), new Sort.Order(Sort.Direction.ASC, "other")));

		assertThat(sql).contains("SELECT", //
				"dummy_entity.id1 AS id1", //
				"dummy_entity.x_name AS x_name", //
				"dummy_entity.x_other AS x_other", //
				"ref.x_l1id AS ref_x_l1id", //
				"ref.x_content AS ref_x_content", //
				"ref_further.x_l2id AS ref_further_x_l2id", //
				"ref_further.x_something AS ref_further_x_something", //
				"FROM dummy_entity ", //
				"LEFT OUTER JOIN referenced_entity ref ON ref.dummy_entity = dummy_entity.id1", //
				"LEFT OUTER JOIN second_level_referenced_entity ref_further ON ref_further.referenced_entity = ref.x_l1id", //
				"ORDER BY dummy_entity.x_name DESC", //
				"x_other ASC");
	}

	@Test // GH-821
	void findAllSortedWithNullHandling_resolvesNullHandlingWhenDialectSupportsIt() {

		SqlGenerator sqlGenerator = createSqlGenerator(DummyEntity.class, PostgresDialect.INSTANCE);

		String sql = sqlGenerator
				.getFindAll(Sort.by(new Sort.Order(Sort.Direction.ASC, "name", Sort.NullHandling.NULLS_LAST)));

		assertThat(sql).contains("ORDER BY \"dummy_entity\".\"x_name\" ASC NULLS LAST");
	}

	@Test // GH-821
	void findAllSortedWithNullHandling_ignoresNullHandlingWhenDialectDoesNotSupportIt() {

		SqlGenerator sqlGenerator = createSqlGenerator(DummyEntity.class, SqlServerDialect.INSTANCE);

		String sql = sqlGenerator
				.getFindAll(Sort.by(new Sort.Order(Sort.Direction.ASC, "name", Sort.NullHandling.NULLS_LAST)));

		assertThat(sql).endsWith("ORDER BY \"dummy_entity\".\"x_name\" ASC");
	}

	@Test // DATAJDBC-101
	void findAllPagedByUnpaged() {

		String sql = sqlGenerator.getFindAll(Pageable.unpaged());

		assertThat(sql).doesNotContain("ORDER BY").doesNotContain("FETCH FIRST").doesNotContain("OFFSET");
	}

	@Test // DATAJDBC-101
	void findAllPaged() {

		String sql = sqlGenerator.getFindAll(PageRequest.of(2, 20));

		assertThat(sql).contains("SELECT", //
				"dummy_entity.id1 AS id1", //
				"dummy_entity.x_name AS x_name", //
				"dummy_entity.x_other AS x_other", //
				"ref.x_l1id AS ref_x_l1id", //
				"ref.x_content AS ref_x_content", //
				"ref_further.x_l2id AS ref_further_x_l2id", //
				"ref_further.x_something AS ref_further_x_something", //
				"FROM dummy_entity ", //
				"LEFT OUTER JOIN referenced_entity ref ON ref.dummy_entity = dummy_entity.id1", //
				"LEFT OUTER JOIN second_level_referenced_entity ref_further ON ref_further.referenced_entity = ref.x_l1id", //
				"OFFSET 40", //
				"LIMIT 20");
	}

	@Test // DATAJDBC-101
	void findAllPagedAndSorted() {

		String sql = sqlGenerator.getFindAll(PageRequest.of(3, 10, Sort.by("name")));

		assertThat(sql).contains("SELECT", //
				"dummy_entity.id1 AS id1", //
				"dummy_entity.x_name AS x_name", //
				"dummy_entity.x_other AS x_other", //
				"ref.x_l1id AS ref_x_l1id", //
				"ref.x_content AS ref_x_content", //
				"ref_further.x_l2id AS ref_further_x_l2id", //
				"ref_further.x_something AS ref_further_x_something", //
				"FROM dummy_entity ", //
				"LEFT OUTER JOIN referenced_entity ref ON ref.dummy_entity = dummy_entity.id1", //
				"LEFT OUTER JOIN second_level_referenced_entity ref_further ON ref_further.referenced_entity = ref.x_l1id", //
				"ORDER BY dummy_entity.x_name ASC", //
				"OFFSET 30", //
				"LIMIT 10");
	}

	@Test // GH-1919
	void selectByQuery() {

		Query query = Query.query(Criteria.where("id").is(23L));

		String sql = sqlGenerator.selectByQuery(query, new MapSqlParameterSource());

		assertThat(sql).contains( //
				"SELECT", //
				"FROM dummy_entity", //
				"LEFT OUTER JOIN referenced_entity ref ON ref.dummy_entity = dummy_entity.id1", //
				"LEFT OUTER JOIN second_level_referenced_entity ref_further ON ref_further.referenced_entity = ref.x_l1id", //
				"WHERE dummy_entity.id1 = :id1" //
		);
	}

	@Test // GH-1919
	void selectBySortedQuery() {

		Query query = Query.query(Criteria.where("id").is(23L)) //
				.sort(Sort.by(Sort.Order.asc("id")));

		String sql = sqlGenerator.selectByQuery(query, new MapSqlParameterSource());

		assertThat(sql).contains( //
				"SELECT", //
				"FROM dummy_entity", //
				"LEFT OUTER JOIN referenced_entity ref ON ref.dummy_entity = dummy_entity.id1", //
				"LEFT OUTER JOIN second_level_referenced_entity ref_further ON ref_further.referenced_entity = ref.x_l1id", //
				"WHERE dummy_entity.id1 = :id1", //
				"ORDER BY dummy_entity.id1 ASC" //
		);
		assertThat(sql).containsOnlyOnce("LEFT OUTER JOIN referenced_entity ref ON ref.dummy_entity = dummy_entity.id1");
		assertThat(sql).containsOnlyOnce("LEFT OUTER JOIN second_level_referenced_entity ref_further ON ref_further.referenced_entity = ref.x_l1id");
	}

	@Test // DATAJDBC-131, DATAJDBC-111
	void findAllByProperty() {

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
				"LEFT OUTER JOIN referenced_entity ref ON ref.dummy_entity = dummy_entity.id1", //
				"LEFT OUTER JOIN second_level_referenced_entity ref_further ON ref_further.referenced_entity = ref.x_l1id", //
				"WHERE dummy_entity.backref = :backref");
	}

	@Test // DATAJDBC-223
	void findAllByPropertyWithMultipartIdentifier() {

		// this would get called when ListParent is the element type of a Set
		Identifier parentIdentifier = Identifier.of(unquoted("backref"), "some-value", String.class) //
				.withPart(unquoted("backref_key"), "key-value", Object.class);
		String sql = sqlGenerator.getFindAllByProperty(parentIdentifier, null, false);

		assertThat(sql).contains("SELECT", //
				"dummy_entity.id1 AS id1", //
				"dummy_entity.x_name AS x_name", //
				"dummy_entity.x_other AS x_other", //
				"ref.x_l1id AS ref_x_l1id", //
				"ref.x_content AS ref_x_content", //
				"ref_further.x_l2id AS ref_further_x_l2id", //
				"ref_further.x_something AS ref_further_x_something", //
				"FROM dummy_entity ", //
				"LEFT OUTER JOIN referenced_entity ref ON ref.dummy_entity = dummy_entity.id1", //
				"LEFT OUTER JOIN second_level_referenced_entity ref_further ON ref_further.referenced_entity = ref.x_l1id", //
				"dummy_entity.backref = :backref", //
				"dummy_entity.backref_key = :backref_key");
	}

	@Test // DATAJDBC-131, DATAJDBC-111
	void findAllByPropertyWithKey() {

		// this would get called when ListParent is th element type of a Map
		String sql = sqlGenerator.getFindAllByProperty(BACKREF,
				new AggregatePath.ColumnInfo(unquoted("key-column"), unquoted("key-column")), false);

		assertThat(sql).isEqualTo("SELECT dummy_entity.id1 AS id1, dummy_entity.x_name AS x_name, " //
				+ "dummy_entity.x_other AS x_other, " //
				+ "ref.x_l1id AS ref_x_l1id, ref.x_content AS ref_x_content, "
				+ "ref_further.x_l2id AS ref_further_x_l2id, ref_further.x_something AS ref_further_x_something, " //
				+ "dummy_entity.key-column AS key-column " //
				+ "FROM dummy_entity " //
				+ "LEFT OUTER JOIN referenced_entity ref ON ref.dummy_entity = dummy_entity.id1 " //
				+ "LEFT OUTER JOIN second_level_referenced_entity ref_further ON ref_further.referenced_entity = ref.x_l1id " //
				+ "WHERE dummy_entity.backref = :backref");
	}

	@Test // DATAJDBC-130
	void findAllByPropertyOrderedWithoutKey() {
		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> sqlGenerator.getFindAllByProperty(BACKREF, null, true));
	}

	@Test // DATAJDBC-131, DATAJDBC-111
	void findAllByPropertyWithKeyOrdered() {

		// this would get called when ListParent is th element type of a Map
		String sql = sqlGenerator.getFindAllByProperty(BACKREF,
				new AggregatePath.ColumnInfo(unquoted("key-column"), unquoted("key-column")), true);

		assertThat(sql).isEqualTo("SELECT dummy_entity.id1 AS id1, dummy_entity.x_name AS x_name, " //
				+ "dummy_entity.x_other AS x_other, " //
				+ "ref.x_l1id AS ref_x_l1id, ref.x_content AS ref_x_content, "
				+ "ref_further.x_l2id AS ref_further_x_l2id, ref_further.x_something AS ref_further_x_something, " //
				+ "dummy_entity.key-column AS key-column " //
				+ "FROM dummy_entity " //
				+ "LEFT OUTER JOIN referenced_entity ref ON ref.dummy_entity = dummy_entity.id1 " //
				+ "LEFT OUTER JOIN second_level_referenced_entity ref_further ON ref_further.referenced_entity = ref.x_l1id " //
				+ "WHERE dummy_entity.backref = :backref " //
				+ "ORDER BY key-column");
	}

	@Test // GH-1073
	public void findAllByPropertyAvoidsDuplicateColumns() {

		final SqlGenerator sqlGenerator = createSqlGenerator(ReferencedEntity.class);
		final String sql = sqlGenerator.getFindAllByProperty(
				Identifier.of(quoted("id"), "parent-id-value", DummyEntity.class), //
				new AggregatePath.ColumnInfo(quoted("X_L1ID"), quoted("X_L1ID")), // this key column collides with the name
																																					// derived by the naming strategy for the id
																																					// of
				// ReferencedEntity.
				false);

		final String id = "referenced_entity.x_l1id AS x_l1id";
		assertThat(sql.indexOf(id)) //
				.describedAs(sql) //
				.isEqualTo(sql.lastIndexOf(id));

	}

	@Test // GH-833
	void findAllByPropertyWithEmptyBackrefColumn() {

		Identifier emptyIdentifier = Identifier.of(EMPTY, 0, Object.class);
		assertThatThrownBy(() -> sqlGenerator.getFindAllByProperty(emptyIdentifier,
				new AggregatePath.ColumnInfo(unquoted("key-column"), unquoted("key-column")), false)) //
				.isInstanceOf(IllegalArgumentException.class) //
				.hasMessageContaining(
						"An empty SqlIdentifier can't be used in condition. Make sure that all composite primary keys are defined in the query");
	}

	@Test // DATAJDBC-219
	void updateWithVersion() {

		SqlGenerator sqlGenerator = createSqlGenerator(VersionedEntity.class, AnsiDialect.INSTANCE);

		assertThat(sqlGenerator.getUpdateWithVersion()).containsSubsequence( //
				"UPDATE", //
				"\"VERSIONED_ENTITY\"", //
				"SET", //
				"WHERE", //
				"\"id1\" = :id1", //
				"AND", //
				"\"X_VERSION\" = :___oldOptimisticLockingVersion");
	}

	@Test // DATAJDBC-264
	void getInsertForEmptyColumnListPostgres() {

		SqlGenerator sqlGenerator = createSqlGenerator(IdOnlyEntity.class, PostgresDialect.INSTANCE);

		String insert = sqlGenerator.getInsert(emptySet());

		assertThat(insert).endsWith(" VALUES (DEFAULT)");
	}

	@Test // GH-777
	void gerInsertForEmptyColumnListMsSqlServer() {

		SqlGenerator sqlGenerator = createSqlGenerator(IdOnlyEntity.class, SqlServerDialect.INSTANCE);

		String insert = sqlGenerator.getInsert(emptySet());

		assertThat(insert).endsWith(" DEFAULT VALUES");
	}

	@Test // DATAJDBC-334
	void getInsertForQuotedColumnName() {

		SqlGenerator sqlGenerator = createSqlGenerator(EntityWithQuotedColumnName.class, AnsiDialect.INSTANCE);

		String insert = sqlGenerator.getInsert(emptySet());

		assertThat(insert).isEqualTo("INSERT INTO \"ENTITY_WITH_QUOTED_COLUMN_NAME\" " //
				+ "(\"test\"\"_@123\") " + "VALUES (:test_123)");
	}

	@Test // DATAJDBC-266
	void joinForOneToOneWithoutIdIncludesTheBackReferenceOfTheOuterJoin() {

		SqlGenerator sqlGenerator = createSqlGenerator(ParentOfNoIdChild.class, AnsiDialect.INSTANCE);

		String findAll = sqlGenerator.getFindAll();

		assertThat(findAll).containsSubsequence("SELECT",
				"\"child\".\"PARENT_OF_NO_ID_CHILD\" AS \"CHILD_PARENT_OF_NO_ID_CHILD\"", "FROM");
	}

	@Test // DATAJDBC-262
	void update() {

		SqlGenerator sqlGenerator = createSqlGenerator(DummyEntity.class, AnsiDialect.INSTANCE);

		assertThat(sqlGenerator.getUpdate()).containsSubsequence( //
				"UPDATE", //
				"\"DUMMY_ENTITY\"", //
				"SET", //
				"WHERE", //
				"\"id1\" = :id1");
	}

	@Test // DATAJDBC-324
	void readOnlyPropertyExcludedFromQuery_when_generateUpdateSql() {

		final SqlGenerator sqlGenerator = createSqlGenerator(EntityWithReadOnlyProperty.class, AnsiDialect.INSTANCE);

		assertThat(sqlGenerator.getUpdate()).isEqualToIgnoringCase( //
				"UPDATE \"ENTITY_WITH_READ_ONLY_PROPERTY\" " //
						+ "SET \"X_NAME\" = :X_NAME " //
						+ "WHERE \"ENTITY_WITH_READ_ONLY_PROPERTY\".\"X_ID\" = :X_ID" //
		);
	}

	@Test // DATAJDBC-334
	void getUpdateForQuotedColumnName() {

		SqlGenerator sqlGenerator = createSqlGenerator(EntityWithQuotedColumnName.class, AnsiDialect.INSTANCE);

		String update = sqlGenerator.getUpdate();

		assertThat(update).isEqualTo("UPDATE \"ENTITY_WITH_QUOTED_COLUMN_NAME\" " //
				+ "SET \"test\"\"_@123\" = :test_123 " //
				+ "WHERE \"ENTITY_WITH_QUOTED_COLUMN_NAME\".\"test\"\"_@id\" = :test_id");
	}

	@Test // DATAJDBC-324
	void readOnlyPropertyExcludedFromQuery_when_generateInsertSql() {

		final SqlGenerator sqlGenerator = createSqlGenerator(EntityWithReadOnlyProperty.class, AnsiDialect.INSTANCE);

		assertThat(sqlGenerator.getInsert(emptySet())).isEqualToIgnoringCase( //
				"INSERT INTO \"ENTITY_WITH_READ_ONLY_PROPERTY\" (\"X_NAME\") " //
						+ "VALUES (:x_name)" //
		);
	}

	@Test // DATAJDBC-324
	void readOnlyPropertyIncludedIntoQuery_when_generateFindAllSql() {

		final SqlGenerator sqlGenerator = createSqlGenerator(EntityWithReadOnlyProperty.class);

		assertThat(sqlGenerator.getFindAll()).isEqualToIgnoringCase("SELECT "
				+ "entity_with_read_only_property.x_id AS x_id, " + "entity_with_read_only_property.x_name AS x_name, "
				+ "entity_with_read_only_property.x_read_only_value AS x_read_only_value "
				+ "FROM entity_with_read_only_property");
	}

	@Test // DATAJDBC-324
	void readOnlyPropertyIncludedIntoQuery_when_generateFindAllByPropertySql() {

		final SqlGenerator sqlGenerator = createSqlGenerator(EntityWithReadOnlyProperty.class);

		assertThat(sqlGenerator.getFindAllByProperty(BACKREF,
				new AggregatePath.ColumnInfo(unquoted("key-column"), unquoted("key-column")), true)).isEqualToIgnoringCase( //
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
	void readOnlyPropertyIncludedIntoQuery_when_generateFindAllInListSql() {

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
	void readOnlyPropertyIncludedIntoQuery_when_generateFindOneSql() {

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
	void deletingLongChain() {

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
	void deletingLongChainNoId() {

		assertThat(createSqlGenerator(NoIdChain4.class)
				.createDeleteByPath(getPath("chain3.chain2.chain1.chain0", NoIdChain4.class))) //
				.isEqualTo("DELETE FROM no_id_chain0 WHERE no_id_chain0.no_id_chain4 = :rootId");
	}

	@Test // DATAJDBC-359
	void deletingLongChainNoIdWithBackreferenceNotReferencingTheRoot() {

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
	void noJoinForSimpleColumn() {
		assertThat(generateJoin("id", DummyEntity.class)).isNull();
	}

	@Test // DATAJDBC-340
	void joinForSimpleReference() {

		SqlGenerator.Join join = generateJoin("ref", DummyEntity.class);

		assertSoftly(softly -> {

			softly.assertThat(join.getJoinTable().getName()).isEqualTo(SqlIdentifier.quoted("REFERENCED_ENTITY"));
			softly.assertThat(join.getJoinColumn().getTable()).isEqualTo(join.getJoinTable());
			softly.assertThat(join.getJoinColumn().getName()).isEqualTo(SqlIdentifier.quoted("DUMMY_ENTITY"));
			softly.assertThat(join.getParentId().getName()).isEqualTo(SqlIdentifier.quoted("id1"));
			softly.assertThat(join.getParentId().getTable().getName()).isEqualTo(SqlIdentifier.quoted("DUMMY_ENTITY"));
		});
	}

	@Test // DATAJDBC-340
	void noJoinForCollectionReference() {

		SqlGenerator.Join join = generateJoin("elements", DummyEntity.class);

		assertThat(join).isNull();

	}

	@Test // DATAJDBC-340
	void noJoinForMappedReference() {

		SqlGenerator.Join join = generateJoin("mappedElements", DummyEntity.class);

		assertThat(join).isNull();
	}

	@Test // DATAJDBC-340
	void joinForSecondLevelReference() {

		SqlGenerator.Join join = generateJoin("ref.further", DummyEntity.class);

		assertSoftly(softly -> {

			softly.assertThat(join.getJoinTable().getName())
					.isEqualTo(SqlIdentifier.quoted("SECOND_LEVEL_REFERENCED_ENTITY"));
			softly.assertThat(join.getJoinColumn().getTable()).isEqualTo(join.getJoinTable());
			softly.assertThat(join.getJoinColumn().getName()).isEqualTo(SqlIdentifier.quoted("REFERENCED_ENTITY"));
			softly.assertThat(join.getParentId().getName()).isEqualTo(SqlIdentifier.quoted("X_L1ID"));
			softly.assertThat(join.getParentId().getTable().getName()).isEqualTo(SqlIdentifier.quoted("REFERENCED_ENTITY"));
		});
	}

	@Test // DATAJDBC-340
	void joinForOneToOneWithoutId() {

		SqlGenerator.Join join = generateJoin("child", ParentOfNoIdChild.class);
		Table joinTable = join.getJoinTable();

		assertSoftly(softly -> {

			softly.assertThat(joinTable.getName()).isEqualTo(SqlIdentifier.quoted("NO_ID_CHILD"));
			softly.assertThat(joinTable).isInstanceOf(Aliased.class);
			softly.assertThat(((Aliased) joinTable).getAlias()).isEqualTo(SqlIdentifier.quoted("child"));
			softly.assertThat(join.getJoinColumn().getTable()).isEqualTo(joinTable);
			softly.assertThat(join.getJoinColumn().getName()).isEqualTo(SqlIdentifier.quoted("PARENT_OF_NO_ID_CHILD"));
			softly.assertThat(join.getParentId().getName()).isEqualTo(SqlIdentifier.quoted("X_ID"));
			softly.assertThat(join.getParentId().getTable().getName())
					.isEqualTo(SqlIdentifier.quoted("PARENT_OF_NO_ID_CHILD"));

		});
	}

	@Nullable
	private SqlGenerator.Join generateJoin(String path, Class<?> type) {
		return createSqlGenerator(type, AnsiDialect.INSTANCE)
				.getJoin(context.getAggregatePath(PersistentPropertyPathTestUtils.getPath(path, type, context)));
	}

	@Test // DATAJDBC-340
	void simpleColumn() {

		assertThat(generatedColumn("id", DummyEntity.class)) //
				.extracting(c -> c.getName(), c -> c.getTable().getName(), c -> getAlias(c.getTable()), this::getAlias)
				.containsExactly(SqlIdentifier.quoted("id1"), SqlIdentifier.quoted("DUMMY_ENTITY"), null,
						SqlIdentifier.quoted("id1"));
	}

	@Test // DATAJDBC-340
	void columnForIndirectProperty() {

		assertThat(generatedColumn("ref.l1id", DummyEntity.class)) //
				.extracting(c -> c.getName(), c -> c.getTable().getName(), c -> getAlias(c.getTable()), this::getAlias) //
				.containsExactly(SqlIdentifier.quoted("X_L1ID"), SqlIdentifier.quoted("REFERENCED_ENTITY"),
						SqlIdentifier.quoted("ref"), SqlIdentifier.quoted("REF_X_L1ID"));
	}

	@Test // DATAJDBC-340
	void noColumnForReferencedEntity() {
		assertThat(generatedColumn("ref", DummyEntity.class)).isNull();
	}

	@Test // DATAJDBC-340
	void columnForReferencedEntityWithoutId() {

		assertThat(generatedColumn("child", ParentOfNoIdChild.class)) //
				.extracting(c -> c.getName(), c -> c.getTable().getName(), c -> getAlias(c.getTable()), this::getAlias) //
				.containsExactly(SqlIdentifier.quoted("PARENT_OF_NO_ID_CHILD"), SqlIdentifier.quoted("NO_ID_CHILD"),
						SqlIdentifier.quoted("child"), SqlIdentifier.quoted("CHILD_PARENT_OF_NO_ID_CHILD"));
	}

	@Test // GH-1192
	void selectByQueryValidTest() {

		SqlGenerator sqlGenerator = createSqlGenerator(DummyEntity.class);

		DummyEntity probe = new DummyEntity();
		probe.name = "Diego";

		Criteria criteria = Criteria.where("name").is(probe.name);
		Query query = Query.query(criteria);

		MapSqlParameterSource parameterSource = new MapSqlParameterSource();

		String generatedSQL = sqlGenerator.selectByQuery(query, parameterSource);
		assertThat(generatedSQL).isNotNull().contains(":x_name");

		assertThat(parameterSource.getValues()) //
				.containsOnly(entry("x_name", probe.name));
	}

	@Test // GH-1329
	void selectWithOutAnyCriteriaTest() {

		SqlGenerator sqlGenerator = createSqlGenerator(DummyEntity.class);
		Query query = Query.query(Criteria.empty());
		MapSqlParameterSource parameterSource = new MapSqlParameterSource();

		String generatedSQL = sqlGenerator.selectByQuery(query, parameterSource);

		assertThat(generatedSQL).isNotNull().doesNotContain("where");
	}

	@Test // GH-1192
	void existsByQuerySimpleValidTest() {

		SqlGenerator sqlGenerator = createSqlGenerator(DummyEntity.class);

		DummyEntity probe = new DummyEntity();
		probe.name = "Diego";

		Criteria criteria = Criteria.where("name").is(probe.name);
		Query query = Query.query(criteria);

		MapSqlParameterSource parameterSource = new MapSqlParameterSource();

		String generatedSQL = sqlGenerator.existsByQuery(query, parameterSource);
		assertThat(generatedSQL).isNotNull().contains(":x_name");

		assertThat(parameterSource.getValues()) //
				.containsOnly(entry("x_name", probe.name));
	}

	@Test // GH-1192
	void countByQuerySimpleValidTest() {

		SqlGenerator sqlGenerator = createSqlGenerator(DummyEntity.class);

		DummyEntity probe = new DummyEntity();
		probe.name = "Diego";

		Criteria criteria = Criteria.where("name").is(probe.name);
		Query query = Query.query(criteria);

		MapSqlParameterSource parameterSource = new MapSqlParameterSource();

		String generatedSQL = sqlGenerator.countByQuery(query, parameterSource);
		assertThat(generatedSQL) //
				.isNotNull() //
				.containsIgnoringCase("COUNT(1)") //
				.contains(":x_name");

		assertThat(parameterSource.getValues()) //
				.containsOnly(entry("x_name", probe.name));
	}

	@Test // GH-1192
	void selectByQueryPaginationValidTest() {

		SqlGenerator sqlGenerator = createSqlGenerator(DummyEntity.class);

		DummyEntity probe = new DummyEntity();
		probe.name = "Diego";

		Criteria criteria = Criteria.where("name").is(probe.name);
		Query query = Query.query(criteria);

		PageRequest pageRequest = PageRequest.of(2, 1, Sort.by(Sort.Order.asc("name")));

		MapSqlParameterSource parameterSource = new MapSqlParameterSource();

		String generatedSQL = sqlGenerator.selectByQuery(query, parameterSource, pageRequest);
		assertThat(generatedSQL) //
				.isNotNull() //
				.contains(":x_name") //
				.containsIgnoringCase("ORDER BY dummy_entity.x_name ASC") //
				.containsIgnoringCase("LIMIT 1") //
				.containsIgnoringCase("OFFSET 2 LIMIT 1");

		assertThat(parameterSource.getValues()) //
				.containsOnly(entry("x_name", probe.name));
	}

	@Test // GH-1161
	void backReferenceShouldConsiderRenamedParent() {

		namingStrategy.setForeignKeyNaming(APPLY_RENAMING);
		context = new JdbcMappingContext(namingStrategy);

		String sql = sqlGenerator.createDeleteInByPath(getPath("ref", RenamedDummy.class));

		assertThat(sql).isEqualTo("DELETE FROM referenced_entity WHERE referenced_entity.renamed IN (:ids)");
	}

	@Test // GH-1161
	void backReferenceShouldIgnoreRenamedParent() {

		namingStrategy.setForeignKeyNaming(IGNORE_RENAMING);
		context = new JdbcMappingContext(namingStrategy);

		String sql = sqlGenerator.createDeleteInByPath(getPath("ref", RenamedDummy.class));

		assertThat(sql).isEqualTo("DELETE FROM referenced_entity WHERE referenced_entity.renamed_dummy IN (:ids)");
	}

	@Test // GH-1161
	void keyColumnShouldConsiderRenamedParent() {

		namingStrategy.setForeignKeyNaming(APPLY_RENAMING);
		context = new JdbcMappingContext(namingStrategy);

		SqlGenerator sqlGenerator = createSqlGenerator(ReferencedEntity.class);
		String sql = sqlGenerator.getFindAllByProperty(Identifier.of(unquoted("parentId"), 23, RenamedDummy.class),
				getPath("ref", RenamedDummy.class));

		assertThat(sql).contains("referenced_entity.renamed_key AS renamed_key", "WHERE referenced_entity.parentId");
	}

	@Test // GH-1161
	void keyColumnShouldIgnoreRenamedParent() {

		namingStrategy.setForeignKeyNaming(IGNORE_RENAMING);
		context = new JdbcMappingContext(namingStrategy);

		SqlGenerator sqlGenerator = createSqlGenerator(ReferencedEntity.class);
		String sql = sqlGenerator.getFindAllByProperty(Identifier.of(unquoted("parentId"), 23, RenamedDummy.class),
				getPath("ref", RenamedDummy.class));

		assertThat(sql).contains("referenced_entity.renamed_dummy_key AS renamed_dummy_key",
				"WHERE referenced_entity.parentId");
	}

	@Test // GH-1865
	void mappingMapKeyToChildShouldNotResultInDuplicateColumn() {

		SqlGenerator sqlGenerator = createSqlGenerator(Child.class);
		String sql = sqlGenerator.getFindAllByProperty(Identifier.of(unquoted("parent"), 23, Parent.class),
				context.getAggregatePath(getPath("children", Parent.class)).getTableInfo().qualifierColumnInfo(), false);

		assertThat(sql).containsOnlyOnce("child.NICK_NAME AS NICK_NAME");
	}

	@Nullable
	private SqlIdentifier getAlias(Object maybeAliased) {

		if (maybeAliased instanceof Aliased aliased) {
			return aliased.getAlias();
		}
		return null;
	}

	@Nullable
	private org.springframework.data.relational.core.sql.Column generatedColumn(String path, Class<?> type) {

		return createSqlGenerator(type, AnsiDialect.INSTANCE)
				.getColumn(context.getAggregatePath(PersistentPropertyPathTestUtils.getPath(path, type, context)));
	}

	private PersistentPropertyPath<RelationalPersistentProperty> getPath(String path, Class<?> baseType) {
		return PersistentPropertyPathTestUtils.getPath(path, baseType, context);
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
		Map<Long, ReferencedEntity> mappedReference;
	}

	@SuppressWarnings("unused")
	@org.springframework.data.relational.core.mapping.Table("renamed")
	static class RenamedDummy {

		@Id Long id;
		String name;
		Map<String, ReferencedEntity> ref;
	}

	@SuppressWarnings("unused")
	static class VersionedEntity extends DummyEntity {
		@Version Integer version;
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

	@SuppressWarnings("unused")
	static class Element {
		@Id Long id;
		String content;
	}

	@SuppressWarnings("unused")
	static class ParentOfNoIdChild {

		@Id Long id;
		NoIdChild child;
	}

	private static class NoIdChild {}

	@SuppressWarnings("unused")
	static class OtherAggregate {
		@Id Long id;
		String name;
	}

	private static class PrefixingNamingStrategy extends DefaultNamingStrategy {

		@Override
		public String getColumnName(RelationalPersistentProperty property) {
			return "x_" + super.getColumnName(property);
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

	@SuppressWarnings("unused")
	static class EntityWithQuotedColumnName {

		// these column names behave like single double quote in the name since the get quoted and then doubling the double
		// quote escapes it.
		@Id
		@Column("test\"\"_@id") Long id;
		@Column("test\"\"_@123") String name;
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

	@SuppressWarnings("unused")
	static class NoIdChain0 {
		String zeroValue;
	}

	@SuppressWarnings("unused")
	static class NoIdChain1 {
		String oneValue;
		NoIdChain0 chain0;
	}

	@SuppressWarnings("unused")
	static class NoIdChain2 {
		String twoValue;
		NoIdChain1 chain1;
	}

	@SuppressWarnings("unused")
	static class NoIdChain3 {
		String threeValue;
		NoIdChain2 chain2;
	}

	@SuppressWarnings("unused")
	static class NoIdChain4 {
		@Id Long four;
		String fourValue;
		NoIdChain3 chain3;
	}

	@SuppressWarnings("unused")
	static class IdNoIdChain {
		@Id Long id;
		NoIdChain4 chain4;
	}

	@SuppressWarnings("unused")
	static class IdIdNoIdChain {
		@Id Long id;
		IdNoIdChain idNoIdChain;
	}

	record Parent(@Id Long id, String name, @MappedCollection(keyColumn = "NICK_NAME") Map<String, Child> children) {
	}

	record Child(@Column("NICK_NAME") String nickName, String name) {
	}
}
