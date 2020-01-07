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

import static java.util.Arrays.*;
import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.experimental.Wither;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.naming.OperationNotSupportedException;

import org.assertj.core.api.SoftAssertions;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.Embedded.OnEmpty;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.Identifier;
import org.springframework.data.repository.query.Param;
import org.springframework.util.Assert;

/**
 * Tests the extraction of entities from a {@link ResultSet} by the {@link EntityRowMapper}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Maciej Walkowiak
 * @author Bastian Wilhelm
 * @author Christoph Strobl
 * @author Myeonghyeon Lee
 */
public class EntityRowMapperUnitTests {

	public static final long ID_FOR_ENTITY_REFERENCING_MAP = 42L;
	public static final long ID_FOR_ENTITY_REFERENCING_LIST = 4711L;
	public static final long ID_FOR_ENTITY_NOT_REFERENCING_MAP = 23L;
	public static final NamingStrategy X_APPENDING_NAMINGSTRATEGY = new NamingStrategy() {
		@Override
		public String getColumnName(RelationalPersistentProperty property) {
			return NamingStrategy.super.getColumnName(property) + "x";
		}
	};

	@Test // DATAJDBC-113
	public void simpleEntitiesGetProperlyExtracted() throws SQLException {

		ResultSet rs = mockResultSet(asList("id", "name"), //
				ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha");
		rs.next();

		Trivial extracted = createRowMapper(Trivial.class).mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.name) //
				.containsExactly(ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha");
	}

	@Test // DATAJDBC-181
	public void namingStrategyGetsHonored() throws SQLException {

		ResultSet rs = mockResultSet(asList("idx", "namex"), //
				ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha");
		rs.next();

		Trivial extracted = createRowMapper(Trivial.class, X_APPENDING_NAMINGSTRATEGY).mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.name) //
				.containsExactly(ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha");
	}

	@Test // DATAJDBC-181
	public void namingStrategyGetsHonoredForConstructor() throws SQLException {

		ResultSet rs = mockResultSet(asList("idx", "namex"), //
				ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha");
		rs.next();

		TrivialImmutable extracted = createRowMapper(TrivialImmutable.class, X_APPENDING_NAMINGSTRATEGY).mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.name) //
				.containsExactly(ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha");
	}

	@Test // DATAJDBC-427
	public void simpleWithReferenceGetProperlyExtracted() throws SQLException {

		ResultSet rs = mockResultSet(asList("id", "name", "trivial_id"), //
				ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha", 100L);
		rs.next();

		WithReference extracted = createRowMapper(WithReference.class).mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.name, e -> e.trivialId) //
				.containsExactly(ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha", AggregateReference.to(100L));
	}

	@Test // DATAJDBC-113
	public void simpleOneToOneGetsProperlyExtracted() throws SQLException {

		ResultSet rs = mockResultSet(asList("id", "name", "child_id", "child_name"), //
				ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha", 24L, "beta");
		rs.next();

		OneToOne extracted = createRowMapper(OneToOne.class).mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.name, e -> e.child.id, e -> e.child.name) //
				.containsExactly(ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha", 24L, "beta");
	}

	@Test // DATAJDBC-286
	public void immutableOneToOneGetsProperlyExtracted() throws SQLException {

		ResultSet rs = mockResultSet(asList("id", "name", "child_id", "child_name"), //
				ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha", 24L, "beta");
		rs.next();

		OneToOneImmutable extracted = createRowMapper(OneToOneImmutable.class).mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.name, e -> e.child.id, e -> e.child.name) //
				.containsExactly(ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha", 24L, "beta");
	}

	@Test // DATAJDBC-427
	public void immutableWithReferenceGetsProperlyExtracted() throws SQLException {

		ResultSet rs = mockResultSet(asList("id", "name", "trivial_id"), //
				ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha", 100L);
		rs.next();

		WithReferenceImmutable extracted = createRowMapper(WithReferenceImmutable.class).mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.name, e -> e.trivialId) //
				.containsExactly(ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha", AggregateReference.to(100L));
	}

	// TODO add additional test for multilevel embeddables
	@Test // DATAJDBC-111
	public void simpleEmbeddedGetsProperlyExtracted() throws SQLException {

		ResultSet rs = mockResultSet(asList("id", "name", "prefix_id", "prefix_name"), //
				ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha", 24L, "beta");
		rs.next();

		EmbeddedEntity extracted = createRowMapper(EmbeddedEntity.class).mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.name, e -> e.children.id, e -> e.children.name) //
				.containsExactly(ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha", 24L, "beta");
	}

	@Test // DATAJDBC-113
	public void collectionReferenceGetsLoadedWithAdditionalSelect() throws SQLException {

		ResultSet rs = mockResultSet(asList("id", "name"), //
				ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha");
		rs.next();

		OneToSet extracted = createRowMapper(OneToSet.class).mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.name, e -> e.children.size()) //
				.containsExactly(ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha", 2);
	}

	@Test // DATAJDBC-131
	public void mapReferenceGetsLoadedWithAdditionalSelect() throws SQLException {

		ResultSet rs = mockResultSet(asList("id", "name"), //
				ID_FOR_ENTITY_REFERENCING_MAP, "alpha");
		rs.next();

		OneToMap extracted = createRowMapper(OneToMap.class).mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.name, e -> e.children.size()) //
				.containsExactly(ID_FOR_ENTITY_REFERENCING_MAP, "alpha", 2);
	}

	@Test // DATAJDBC-130
	public void listReferenceGetsLoadedWithAdditionalSelect() throws SQLException {

		ResultSet rs = mockResultSet(asList("id", "name"), //
				ID_FOR_ENTITY_REFERENCING_LIST, "alpha");
		rs.next();

		OneToMap extracted = createRowMapper(OneToMap.class).mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.name, e -> e.children.size()) //
				.containsExactly(ID_FOR_ENTITY_REFERENCING_LIST, "alpha", 2);
	}

	@Test // DATAJDBC-252
	public void doesNotTryToSetPropertiesThatAreSetViaConstructor() throws SQLException {

		ResultSet rs = mockResultSet(singletonList("value"), //
				"value-from-resultSet");
		rs.next();

		DontUseSetter extracted = createRowMapper(DontUseSetter.class).mapRow(rs, 1);

		assertThat(extracted.value) //
				.isEqualTo("setThroughConstructor:value-from-resultSet");
	}

	@Test // DATAJDBC-252
	public void handlesMixedProperties() throws SQLException {

		ResultSet rs = mockResultSet(asList("one", "two", "three"), //
				"111", "222", "333");
		rs.next();

		MixedProperties extracted = createRowMapper(MixedProperties.class).mapRow(rs, 1);

		assertThat(extracted) //
				.extracting(e -> e.one, e -> e.two, e -> e.three) //
				.isEqualTo(new String[] { "111", "222", "333" });
	}

	@Test // DATAJDBC-273
	public void handlesNonSimplePropertyInConstructor() throws SQLException {

		ResultSet rs = mockResultSet(singletonList("id"), //
				ID_FOR_ENTITY_REFERENCING_LIST);
		rs.next();

		EntityWithListInConstructor extracted = createRowMapper(EntityWithListInConstructor.class).mapRow(rs, 1);

		assertThat(extracted.content).hasSize(2);
	}

	@Test // DATAJDBC-359
	public void chainedEntitiesWithoutId() throws SQLException {

		// @formatter:off
		Fixture<NoIdChain4> fixture = this.<NoIdChain4> buildFixture() //
				// Id of the aggregate root and backreference to it from
				// the various aggregate members.
				.value(4L).inColumns("four", //
						"chain3_no_id_chain4", //
						"chain3_chain2_no_id_chain4", //
						"chain3_chain2_chain1_no_id_chain4", //
						"chain3_chain2_chain1_chain0_no_id_chain4") //
				.endUpIn(e -> e.four)
				// values for the different entities
				.value("four_value").inColumns("four_value").endUpIn(e -> e.fourValue) //

				.value("three_value").inColumns("chain3_three_value").endUpIn(e -> e.chain3.threeValue) //

				.value("two_value").inColumns("chain3_chain2_two_value").endUpIn(e -> e.chain3.chain2.twoValue) //

				.value("one_value").inColumns("chain3_chain2_chain1_one_value").endUpIn(e -> e.chain3.chain2.chain1.oneValue) //

				.value("zero_value").inColumns("chain3_chain2_chain1_chain0_zero_value")
				.endUpIn(e -> e.chain3.chain2.chain1.chain0.zeroValue) //
				.build();
		// @formatter:on

		ResultSet rs = fixture.resultSet;

		rs.next();

		NoIdChain4 extracted = createRowMapper(NoIdChain4.class).mapRow(rs, 1);

		fixture.assertOn(extracted);
	}

	@Test // DATAJDBC-370
	public void simpleNullableImmutableEmbeddedGetsProperlyExtracted() throws SQLException {

		ResultSet rs = mockResultSet(asList("id", "value"), //
				ID_FOR_ENTITY_NOT_REFERENCING_MAP, "ru'Ha'");
		rs.next();

		WithNullableEmbeddedImmutableValue extracted = createRowMapper(WithNullableEmbeddedImmutableValue.class) //
				.mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.embeddedImmutableValue) //
				.containsExactly(ID_FOR_ENTITY_NOT_REFERENCING_MAP, new ImmutableValue("ru'Ha'"));
	}

	@Test // DATAJDBC-374
	public void simpleEmptyImmutableEmbeddedGetsProperlyExtracted() throws SQLException {

		ResultSet rs = mockResultSet(asList("id", "value"), //
				ID_FOR_ENTITY_NOT_REFERENCING_MAP, null);
		rs.next();

		WithEmptyEmbeddedImmutableValue extracted = createRowMapper(WithEmptyEmbeddedImmutableValue.class).mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.embeddedImmutableValue) //
				.containsExactly(ID_FOR_ENTITY_NOT_REFERENCING_MAP, new ImmutableValue(null));
	}

	@Test // DATAJDBC-370
	@SneakyThrows
	public void simplePrimitiveImmutableEmbeddedGetsProperlyExtracted() {

		ResultSet rs = mockResultSet(asList("id", "value"), //
				ID_FOR_ENTITY_NOT_REFERENCING_MAP, 24);
		rs.next();

		WithEmbeddedPrimitiveImmutableValue extracted = createRowMapper(WithEmbeddedPrimitiveImmutableValue.class)
				.mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.embeddedImmutablePrimitiveValue) //
				.containsExactly(ID_FOR_ENTITY_NOT_REFERENCING_MAP, new ImmutablePrimitiveValue(24));
	}

	@Test // DATAJDBC-370
	public void simpleImmutableEmbeddedShouldBeNullIfAllOfTheEmbeddableAreNull() throws SQLException {

		ResultSet rs = mockResultSet(asList("id", "value"), //
				ID_FOR_ENTITY_NOT_REFERENCING_MAP, null);
		rs.next();

		WithNullableEmbeddedImmutableValue extracted = createRowMapper(WithNullableEmbeddedImmutableValue.class) //
				.mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.embeddedImmutableValue) //
				.containsExactly(ID_FOR_ENTITY_NOT_REFERENCING_MAP, null);
	}

	@Test // DATAJDBC-370
	@SneakyThrows
	public void embeddedShouldBeNullWhenFieldsAreNull() {

		ResultSet rs = mockResultSet(asList("id", "name", "prefix_id", "prefix_name"), //
				ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha", null, null);
		rs.next();

		EmbeddedEntity extracted = createRowMapper(EmbeddedEntity.class).mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.name, e -> e.children) //
				.containsExactly(ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha", null);
	}

	@Test // DATAJDBC-370
	@SneakyThrows
	public void embeddedShouldNotBeNullWhenAtLeastOneFieldIsNotNull() {

		ResultSet rs = mockResultSet(asList("id", "name", "prefix_id", "prefix_name"), //
				ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha", 24, null);
		rs.next();

		EmbeddedEntity extracted = createRowMapper(EmbeddedEntity.class).mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.name, e -> e.children) //
				.containsExactly(ID_FOR_ENTITY_NOT_REFERENCING_MAP, "alpha", new Trivial(24L, null));
	}

	@Test // DATAJDBC-370
	@SneakyThrows
	public void primitiveEmbeddedShouldBeNullWhenNoValuePresent() {

		ResultSet rs = mockResultSet(asList("id", "value"), //
				ID_FOR_ENTITY_NOT_REFERENCING_MAP, null);
		rs.next();

		WithEmbeddedPrimitiveImmutableValue extracted = createRowMapper(WithEmbeddedPrimitiveImmutableValue.class)
				.mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.embeddedImmutablePrimitiveValue) //
				.containsExactly(ID_FOR_ENTITY_NOT_REFERENCING_MAP, null);
	}

	@Test // DATAJDBC-370
	@SneakyThrows
	public void deepNestedEmbeddable() {

		ResultSet rs = mockResultSet(asList("id", "level0", "level1_value", "level1_level2_value"), //
				ID_FOR_ENTITY_NOT_REFERENCING_MAP, "0", "1", "2");
		rs.next();

		WithDeepNestedEmbeddable extracted = createRowMapper(WithDeepNestedEmbeddable.class).mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> extracted.level0, e -> e.level1.value, e -> e.level1.level2.value) //
				.containsExactly(ID_FOR_ENTITY_NOT_REFERENCING_MAP, "0", "1", "2");
	}

	// Model classes to be used in tests

	@Wither
	@RequiredArgsConstructor
	static class TrivialImmutable {

		@Id private final Long id;
		private final String name;
	}

	@EqualsAndHashCode
	@NoArgsConstructor
	@AllArgsConstructor
	@Getter
	static class Trivial {

		@Id Long id;
		String name;
	}

	@EqualsAndHashCode
	@NoArgsConstructor
	@AllArgsConstructor
	@Getter
	static class WithReference {

		@Id Long id;
		String name;
		AggregateReference<Trivial, Long> trivialId;
	}

	@Wither
	@RequiredArgsConstructor
	static class WithReferenceImmutable {

		@Id private final Long id;
		private final String name;
		private final AggregateReference<TrivialImmutable, Long> trivialId;
	}

	static class OneToOne {

		@Id Long id;
		String name;
		Trivial child;
	}

	@Wither
	@RequiredArgsConstructor
	static class OneToOneImmutable {

		private final @Id Long id;
		private final String name;
		private final TrivialImmutable child;
	}

	static class OneToSet {

		@Id Long id;
		String name;
		Set<Trivial> children;
	}

	static class OneToMap {

		@Id Long id;
		String name;
		Map<String, Trivial> children;
	}

	static class OneToList {

		@Id Long id;
		String name;
		List<Trivial> children;
	}

	static class EmbeddedEntity {

		@Id Long id;
		String name;
		@Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "prefix_") Trivial children;
	}

	private static class DontUseSetter {
		String value;

		DontUseSetter(@Param("value") String value) {
			this.value = "setThroughConstructor:" + value;
		}
	}

	static class MixedProperties {

		final String one;
		String two;
		final String three;

		@PersistenceConstructor
		MixedProperties(String one) {
			this.one = one;
			this.three = "unset";
		}

		private MixedProperties(String one, String two, String three) {

			this.one = one;
			this.two = two;
			this.three = three;
		}

		MixedProperties withThree(String three) {
			return new MixedProperties(one, two, three);
		}
	}

	@AllArgsConstructor
	static class EntityWithListInConstructor {

		@Id final Long id;

		final List<Trivial> content;
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

	static class WithNullableEmbeddedImmutableValue {

		@Id Long id;
		@Embedded(onEmpty = OnEmpty.USE_NULL) ImmutableValue embeddedImmutableValue;
	}

	static class WithEmptyEmbeddedImmutableValue {

		@Id Long id;
		@Embedded.Empty ImmutableValue embeddedImmutableValue;
	}

	static class WithEmbeddedPrimitiveImmutableValue {

		@Id Long id;
		@Embedded.Nullable ImmutablePrimitiveValue embeddedImmutablePrimitiveValue;
	}

	@Value
	static class ImmutableValue {
		Object value;
	}

	@Value
	static class ImmutablePrimitiveValue {
		int value;
	}

	static class WithDeepNestedEmbeddable {

		@Id Long id;
		String level0;
		@Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "level1_") EmbeddedWithEmbedded level1;
	}

	static class EmbeddedWithEmbedded {

		Object value;
		@Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "level2_") ImmutableValue level2;
	}

	// Infrastructure for assertions and constructing mocks

	private <T> FixtureBuilder<T> buildFixture() {
		return new FixtureBuilder<>();
	}

	private <T> EntityRowMapper<T> createRowMapper(Class<T> type) {
		return createRowMapper(type, NamingStrategy.INSTANCE);
	}

	@SuppressWarnings("unchecked")
	private <T> EntityRowMapper<T> createRowMapper(Class<T> type, NamingStrategy namingStrategy) {

		RelationalMappingContext context = new JdbcMappingContext(namingStrategy);

		DataAccessStrategy accessStrategy = mock(DataAccessStrategy.class);

		// the ID of the entity is used to determine what kind of ResultSet is needed for subsequent selects.
		Set<Trivial> trivials = Stream.of(new Trivial(1L, "one"), //
				new Trivial(2L, "two")) //
				.collect(Collectors.toSet());

		Set<Map.Entry<Integer, Trivial>> simpleEntriesWithInts = trivials.stream()
				.collect(Collectors.toMap(it -> it.getId().intValue(), Function.identity())).entrySet();
		Set<Map.Entry<String, Trivial>> simpleEntriesWithStringKeys = trivials.stream()
				.collect(Collectors.toMap(Trivial::getName, Function.identity())).entrySet();

		doReturn(trivials).when(accessStrategy).findAllByProperty(eq(ID_FOR_ENTITY_NOT_REFERENCING_MAP),
				any(RelationalPersistentProperty.class));

		doReturn(simpleEntriesWithStringKeys).when(accessStrategy).findAllByProperty(eq(ID_FOR_ENTITY_REFERENCING_MAP),
				any(RelationalPersistentProperty.class));

		doReturn(simpleEntriesWithInts).when(accessStrategy).findAllByProperty(eq(ID_FOR_ENTITY_REFERENCING_LIST),
				any(RelationalPersistentProperty.class));

		doReturn(trivials).when(accessStrategy).findAllByPath(identifierOfValue(ID_FOR_ENTITY_NOT_REFERENCING_MAP),
				any(PersistentPropertyPath.class));

		doReturn(simpleEntriesWithStringKeys).when(accessStrategy)
				.findAllByPath(identifierOfValue(ID_FOR_ENTITY_REFERENCING_MAP), any(PersistentPropertyPath.class));

		doReturn(simpleEntriesWithInts).when(accessStrategy)
				.findAllByPath(identifierOfValue(ID_FOR_ENTITY_REFERENCING_LIST), any(PersistentPropertyPath.class));

		BasicJdbcConverter converter = new BasicJdbcConverter(context, accessStrategy, new JdbcCustomConversions(),
				JdbcTypeFactory.unsupported());

		return new EntityRowMapper<>( //
				(RelationalPersistentEntity<T>) context.getRequiredPersistentEntity(type), //
				converter //
		);
	}

	private Identifier identifierOfValue(long value) {
		return ArgumentMatchers.argThat(argument -> argument.toMap().containsValue(value));
	}

	private static ResultSet mockResultSet(List<String> columns, Object... values) {

		Assert.isTrue( //
				values.length % columns.size() == 0, //
				String //
						.format( //
								"Number of values [%d] must be a multiple of the number of columns [%d]", //
								values.length, //
								columns.size() //
						) //
		);

		List<Map<String, Object>> result = convertValues(columns, values);

		return mock(ResultSet.class, new ResultSetAnswer(result));
	}

	private static List<Map<String, Object>> convertValues(List<String> columns, Object[] values) {

		List<Map<String, Object>> result = new ArrayList<>();

		int index = 0;
		while (index < values.length) {

			Map<String, Object> row = new HashMap<>();
			result.add(row);
			for (String column : columns) {

				row.put(column, values[index]);
				index++;
			}
		}
		return result;
	}

	@RequiredArgsConstructor
	private static class ResultSetAnswer implements Answer {

		private final List<Map<String, Object>> values;
		private int index = -1;

		@Override
		public Object answer(InvocationOnMock invocation) throws Throwable {

			switch (invocation.getMethod().getName()) {
				case "next":
					return next();
				case "getObject":
					return getObject(invocation.getArgument(0));
				case "isAfterLast":
					return isAfterLast();
				case "isBeforeFirst":
					return isBeforeFirst();
				case "getRow":
					return isAfterLast() || isBeforeFirst() ? 0 : index + 1;
				case "toString":
					return this.toString();
				default:
					throw new OperationNotSupportedException(invocation.getMethod().getName());
			}
		}

		private boolean isAfterLast() {
			return index >= values.size() && !values.isEmpty();
		}

		private boolean isBeforeFirst() {
			return index < 0 && !values.isEmpty();
		}

		private Object getObject(String column) throws SQLException {

			Map<String, Object> rowMap = values.get(index);

			if (!rowMap.containsKey(column)) {
				throw new SQLException(String.format("Trying to access a column (%s) that does not exist", column));
			}

			return rowMap.get(column);
		}

		private boolean next() {

			index++;
			return index < values.size();
		}
	}

	private interface SetValue<T> {
		SetColumns<T> value(Object value);

		Fixture<T> build();
	}

	private interface SetColumns<T> {

		SetExpectation<T> inColumns(String... columns);
	}

	private interface SetExpectation<T> {
		SetValue<T> endUpIn(Function<T, Object> extractor);
	}

	private static class FixtureBuilder<T> implements SetValue<T>, SetColumns<T>, SetExpectation<T> {

		private List<Object> values = new ArrayList<>();
		private List<String> columns = new ArrayList<>();
		private String explainingColumn;
		private List<Expectation<T>> expectations = new ArrayList<>();

		@Override
		public SetColumns<T> value(Object value) {

			values.add(value);

			return this;
		}

		@Override
		public SetExpectation<T> inColumns(String... columns) {

			boolean isFirst = true;
			for (String column : columns) {

				// if more than one column is mentioned, we need to copy the value for all but the first column;
				if (!isFirst) {

					values.add(values.get(values.size() - 1));
				} else {

					explainingColumn = column;
					isFirst = false;
				}

				this.columns.add(column);
			}

			return this;
		}

		@Override
		public Fixture<T> build() {

			return new Fixture<>(mockResultSet(columns, values.toArray()), expectations);
		}

		@Override
		public SetValue<T> endUpIn(Function<T, Object> extractor) {

			expectations.add(new Expectation<T>(extractor, values.get(values.size() - 1), explainingColumn));
			return this;
		}
	}

	@AllArgsConstructor
	private static class Fixture<T> {

		final ResultSet resultSet;
		final List<Expectation<T>> expectations;

		public void assertOn(T result) {

			SoftAssertions.assertSoftly(softly -> {
				expectations.forEach(expectation -> {

					softly.assertThat(expectation.extractor.apply(result)).describedAs("From column: " + expectation.sourceColumn)
							.isEqualTo(expectation.expectedValue);
				});

			});
		}
	}

	@AllArgsConstructor
	private static class Expectation<T> {

		final Function<T, Object> extractor;
		final Object expectedValue;
		final String sourceColumn;
	}
}
