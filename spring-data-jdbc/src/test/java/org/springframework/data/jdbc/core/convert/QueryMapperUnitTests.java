/*
 * Copyright 2020-present the original author or authors.
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
import static org.mockito.Mockito.*;
import static org.springframework.data.domain.Sort.Order.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Functions;
import org.springframework.data.relational.core.sql.OrderByField;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.domain.SqlSort;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

/**
 * Unit tests for {@link QueryMapper}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Mikhail Fedorov
 * @author Christoph Strobl
 */
public class QueryMapperUnitTests {

	private JdbcMappingContext context = new JdbcMappingContext();
	private JdbcConverter converter = new MappingJdbcConverter(context, mock(RelationResolver.class));

	private QueryMapper mapper = new QueryMapper(converter);
	private MapSqlParameterSource parameterSource = new MapSqlParameterSource();

	QueryMapper createMapper(Converter<?, ?>... converters) {

		JdbcCustomConversions conversions = new JdbcCustomConversions(Arrays.asList(converters));

		JdbcConverter converter = new MappingJdbcConverter(context, mock(RelationResolver.class), conversions,
				mock(JdbcTypeFactory.class));

		return new QueryMapper(converter);
	}

	@Test // DATAJDBC-318
	void shouldNotMapEmptyCriteria() {

		Criteria criteria = Criteria.empty();

		assertThatIllegalArgumentException().isThrownBy(() -> map(criteria));
	}

	@Test // DATAJDBC-318
	void shouldNotMapEmptyAndCriteria() {

		Criteria criteria = Criteria.empty().and(Collections.emptyList());

		assertThatIllegalArgumentException().isThrownBy(() -> map(criteria));
	}

	@Test // DATAJDBC-318
	void shouldNotMapEmptyNestedCriteria() {

		Criteria criteria = Criteria.empty().and(Collections.emptyList()).and(Criteria.empty().and(Criteria.empty()));

		assertThat(criteria.isEmpty()).isTrue();
		assertThatIllegalArgumentException().isThrownBy(() -> map(criteria));
	}

	@Test // DATAJDBC-318
	void shouldMapSomeNestedCriteria() {

		Criteria criteria = Criteria.empty().and(Collections.emptyList())
				.and(Criteria.empty().and(Criteria.where("name").is("Hank")));

		assertThat(criteria.isEmpty()).isFalse();

		Condition condition = map(criteria);

		assertThat(condition).hasToString("((person.\"NAME\" = ?[:name]))");
	}

	@Test // DATAJDBC-318
	void shouldMapNestedGroup() {

		Criteria initial = Criteria.empty();

		Criteria criteria = initial.and(Criteria.where("name").is("Foo")) //
				.and(Criteria.where("name").is("Bar") //
						.or("age").lessThan(49) //
						.or(Criteria.where("name").not("Bar") //
								.and("age").greaterThan(49) //
						) //
				);

		assertThat(criteria.isEmpty()).isFalse();

		Condition condition = map(criteria);

		assertThat(condition).hasToString(
				"(person.\"NAME\" = ?[:name]) AND (person.\"NAME\" = ?[:name1] OR person.age < ?[:age] OR (person.\"NAME\" != ?[:name3] AND person.age > ?[:age4]))");
	}

	@Test // DATAJDBC-318
	void shouldMapFrom() {

		Criteria criteria = Criteria.from(Criteria.where("name").is("Foo")) //
				.and(Criteria.where("name").is("Bar") //
						.or("age").lessThan(49) //
				);

		assertThat(criteria.isEmpty()).isFalse();

		Condition condition = map(criteria);

		assertThat(condition)
				.hasToString("person.\"NAME\" = ?[:name] AND (person.\"NAME\" = ?[:name1] OR person.age < ?[:age])");
	}

	@Test // DATAJDBC-560
	void shouldMapFromConcat() {

		Criteria criteria = Criteria.from(Criteria.where("name").is("Foo"), Criteria.where("name").is("Bar") //
				.or("age").lessThan(49));

		assertThat(criteria.isEmpty()).isFalse();

		Condition condition = map(criteria);

		assertThat(condition)
				.hasToString("(person.\"NAME\" = ?[:name] AND (person.\"NAME\" = ?[:name1] OR person.age < ?[:age]))");
	}

	@Test // DATAJDBC-318
	void shouldMapSimpleCriteria() {

		Criteria criteria = Criteria.where("name").is("foo");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" = ?[:name]");
	}

	@Test // DATAJDBC-318
	void shouldMapSimpleCriteriaWithoutEntity() {

		Criteria criteria = Criteria.where("name").is("foo");

		Condition condition = mapper.getMappedObject(new MapSqlParameterSource(), criteria, Table.create("person"), null);

		assertThat(condition).hasToString("person.name = ?[:name]");
	}

	@Test // GH-2191
	void shouldMapCompositeIdCriteria() {

		Criteria criteria = Criteria.where("id").is(new CompositeId(1, "a")).or("foo").is("bar");

		assertThat(map(criteria, WithCompositeId.class)).hasToString(
				"(withcompositeid.\"TENANT\" = ?[:tenant] AND withcompositeid.\"NAME\" = ?[:name]) OR withcompositeid.foo = ?[:foo]");

		criteria = Criteria.where("id").not(new CompositeId(1, "a")).or("foo").is("bar");

		assertThat(map(criteria, WithCompositeId.class)).hasToString(
				"(withcompositeid.\"TENANT\" != ?[:tenant3] AND withcompositeid.\"NAME\" != ?[:name4]) OR withcompositeid.foo = ?[:foo5]");
	}

	@Test // DATAJDBC-318
	void shouldMapExpression() {

		Table table = Table.create("my_table").as("my_aliased_table");

		Expression mappedObject = mapper.getMappedObject(table.column("alternative").as("my_aliased_col"),
				context.getRequiredPersistentEntity(Person.class));

		assertThat(mappedObject).hasToString("my_aliased_table.\"another_name\" AS my_aliased_col");
	}

	@Test // DATAJDBC-318
	void shouldMapCountFunction() {

		Table table = Table.create("my_table").as("my_aliased_table");

		Expression mappedObject = mapper.getMappedObject(Functions.count(table.column("alternative")),
				context.getRequiredPersistentEntity(Person.class));

		assertThat(mappedObject).hasToString("COUNT(my_aliased_table.\"another_name\")");
	}

	@Test // DATAJDBC-318
	void shouldMapExpressionToUnknownColumn() {

		Table table = Table.create("my_table").as("my_aliased_table");

		Expression mappedObject = mapper.getMappedObject(table.column("unknown").as("my_aliased_col"),
				context.getRequiredPersistentEntity(Person.class));

		assertThat(mappedObject).hasToString("my_aliased_table.unknown AS my_aliased_col");
	}

	@Test // DATAJDBC-318
	void shouldMapExpressionWithoutEntity() {

		Table table = Table.create("my_table").as("my_aliased_table");

		Expression mappedObject = mapper.getMappedObject(table.column("my_col").as("my_aliased_col"), null);

		assertThat(mappedObject).hasToString("my_aliased_table.my_col AS my_aliased_col");
	}

	@Test // DATAJDBC-318
	void shouldMapSimpleNullableCriteria() {

		Criteria criteria = Criteria.where("name").isNull();

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" IS NULL");
	}

	@Test // DATAJDBC-318
	void shouldConsiderColumnName() {

		Criteria criteria = Criteria.where("alternative").is("foo");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"another_name\" = ?[:another_name]");
	}

	@Test // DATAJDBC-318
	void shouldMapAndCriteria() {

		Criteria criteria = Criteria.where("name").is("foo").and("bar").is("baz");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" = ?[:name] AND person.bar = ?[:bar]");
	}

	@Test // DATAJDBC-318
	void shouldMapOrCriteria() {

		Criteria criteria = Criteria.where("name").is("foo").or("bar").is("baz");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" = ?[:name] OR person.bar = ?[:bar]");
	}

	@Test // DATAJDBC-318
	void shouldMapAndOrCriteria() {

		Criteria criteria = Criteria.where("name").is("foo") //
				.and("name").isNotNull() //
				.or("bar").is("baz") //
				.and("anotherOne").is("alternative");

		Condition condition = map(criteria);

		assertThat(condition).hasToString(
				"person.\"NAME\" = ?[:name] AND person.\"NAME\" IS NOT NULL OR person.bar = ?[:bar] AND person.anotherOne = ?[:anotherOne]");
	}

	@Test // DATAJDBC-318
	void shouldMapNeq() {

		Criteria criteria = Criteria.where("name").not("foo");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" != ?[:name]");
	}

	@Test // DATAJDBC-318
	void shouldMapIsNull() {

		Criteria criteria = Criteria.where("name").isNull();

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" IS NULL");
	}

	@Test // DATAJDBC-318
	void shouldMapIsNotNull() {

		Criteria criteria = Criteria.where("name").isNotNull();

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" IS NOT NULL");
	}

	@Test // DATAJDBC-318
	void shouldMapIsIn() {

		Criteria criteria = Criteria.where("name").in("a", "b", "c");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" IN (?[:name], ?[:name1], ?[:name2])");
	}

	@Test // DATAJDBC-318
	void shouldMapIsNotIn() {

		Criteria criteria = Criteria.where("name").notIn("a", "b", "c");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" NOT IN (?[:name], ?[:name1], ?[:name2])");
	}

	@Test // GH-2208
	void shouldMapInComposite() {

		Criteria criteria = Criteria.where("id").in(new CompositeId(1, "a"));
		assertThat(map(criteria, WithCompositeId.class))
				.hasToString("(withcompositeid.\"TENANT\" = ?[:tenant] AND withcompositeid.\"NAME\" = ?[:name])");

		criteria = Criteria.where("id").in(new CompositeId(1, "a"), new CompositeId(2, "b"));
		assertThat(map(criteria, WithCompositeId.class)).hasToString(
				"(withcompositeid.\"TENANT\" = ?[:tenant2] AND withcompositeid.\"NAME\" = ?[:name3]) OR (withcompositeid.\"TENANT\" = ?[:tenant4] AND withcompositeid.\"NAME\" = ?[:name5])");

		criteria = Criteria.where("id").in();
		assertThat(map(criteria, WithCompositeId.class)).hasToString("1 = 0");
	}

	@Test // GH-2208
	void shouldMapNotInComposite() {

		Criteria criteria = Criteria.where("id").notIn(new CompositeId(1, "a"));
		assertThat(map(criteria, WithCompositeId.class))
				.hasToString("(withcompositeid.\"TENANT\" != ?[:tenant] AND withcompositeid.\"NAME\" != ?[:name])");

		criteria = Criteria.where("id").notIn(new CompositeId(1, "a"), new CompositeId(2, "b"));
		assertThat(map(criteria, WithCompositeId.class)).hasToString(
				"(withcompositeid.\"TENANT\" != ?[:tenant2] AND withcompositeid.\"NAME\" != ?[:name3]) OR (withcompositeid.\"TENANT\" != ?[:tenant4] AND withcompositeid.\"NAME\" != ?[:name5])");

		criteria = Criteria.where("id").notIn();
		assertThat(map(criteria, WithCompositeId.class)).hasToString("1 = 1");
	}

	@Test
	void shouldMapIsNotInWithCollectionToStringConverter() {

		mapper = createMapper(CollectionToStringConverter.INSTANCE);

		Criteria criteria = Criteria.where("name").notIn("a", "b", "c");

		Condition bindings = map(criteria);

		assertThat(bindings).hasToString("person.\"NAME\" NOT IN (?[:name], ?[:name1], ?[:name2])");
	}

	@Test // DATAJDBC-318
	void shouldMapIsGt() {

		Criteria criteria = Criteria.where("name").greaterThan("a");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" > ?[:name]");
	}

	@Test // DATAJDBC-318
	void shouldMapIsGte() {

		Criteria criteria = Criteria.where("name").greaterThanOrEquals("a");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" >= ?[:name]");
	}

	@Test // DATAJDBC-318
	void shouldMapIsLt() {

		Criteria criteria = Criteria.where("name").lessThan("a");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" < ?[:name]");
	}

	@Test // DATAJDBC-318
	void shouldMapIsLte() {

		Criteria criteria = Criteria.where("name").lessThanOrEquals("a");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" <= ?[:name]");
	}

	@Test // DATAJDBC-318
	void shouldMapBetween() {

		Criteria criteria = Criteria.where("name").between("a", "b");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" BETWEEN ?[:name] AND ?[:name1]");
	}

	@Test // DATAJDBC-318
	void shouldMapIsLike() {

		Criteria criteria = Criteria.where("name").like("a");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" LIKE ?[:name]");
	}

	@Test // DATAJDBC-318
	void shouldMapSort() {

		Sort sort = Sort.by(desc("alternative"));

		List<OrderByField> fields = mapper.getMappedSort(Table.create("tbl"), sort,
				context.getRequiredPersistentEntity(Person.class));

		assertThat(fields) //
				.extracting(Objects::toString) //
				.containsExactly("tbl.\"another_name\" DESC");
	}

	@Test // GH-1507
	void shouldMapSortWithUnknownField() {

		Sort sort = Sort.by(desc("unknownField"));

		List<OrderByField> fields = mapper.getMappedSort(Table.create("tbl"), sort,
				context.getRequiredPersistentEntity(Person.class));

		assertThat(fields) //
				.extracting(Objects::toString) //
				.containsExactly("tbl.unknownField DESC");
	}

	@Test // GH-1507
	void shouldMapSortWithAllowedSpecialCharacters() {

		Sort sort = Sort.by(desc("x(._)x"));

		List<OrderByField> fields = mapper.getMappedSort(Table.create("tbl"), sort,
				context.getRequiredPersistentEntity(Person.class));

		assertThat(fields) //
				.extracting(Objects::toString) //
				.containsExactly("tbl.x(._)x DESC");
	}

	@ParameterizedTest // GH-1507
	@ValueSource(strings = { " ", ";", "--" })
	void shouldNotMapSortWithIllegalExpression(String input) {

		Sort sort = Sort.by(desc("unknown" + input + "Field"));

		assertThatThrownBy(
				() -> mapper.getMappedSort(Table.create("tbl"), sort, context.getRequiredPersistentEntity(Person.class)))
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test // GH-1507
	void shouldMapSortWithUnsafeExpression() {

		String unsafeExpression = "arbitrary expression that may include evil stuff like ; & --";
		Sort sort = SqlSort.unsafe(unsafeExpression);

		List<OrderByField> fields = mapper.getMappedSort(Table.create("tbl"), sort,
				context.getRequiredPersistentEntity(Person.class));

		assertThat(fields) //
				.extracting(Objects::toString) //
				.containsExactly(unsafeExpression + " ASC");
	}

	@Test // GH-2096
	void shouldMapPathToEmbeddable() {

		Criteria criteria = Criteria.where("home").is(new Address(new Country("DE")));

		Condition condition = map(criteria, WithEmbeddable.class);

		assertThat(condition).hasToString(
				"(withcompositeid.\"HOME_COUNTRY_NAME\" = ?[:home_country_name] AND withcompositeid.\"HOME_STREET\" = ?[:home_street])");
	}

	@Test // GH-2096
	void shouldMapPathToNestedEmbeddable() {

		Criteria criteria = Criteria.where("home.country").is(new Country("DE"));

		Condition condition = map(criteria, WithEmbeddable.class);

		assertThat(condition).hasToString("withcompositeid.\"HOME_COUNTRY_NAME\" = ?[:home_country_name]");
	}

	@Test // GH-2096
	void shouldMapPathIntoEmbeddable() {

		Criteria criteria = Criteria.where("home.country.name").is("DE");

		Condition condition = map(criteria, WithEmbeddable.class);

		assertThat(condition).hasToString("withcompositeid.\"HOME_COUNTRY_NAME\" = ?[:home_country_name]");
	}

	@Test // GH-2096
	void shouldMapSortPathForEmbeddable() {

		List<OrderByField> orderByFields = map(Sort.by("home"), WithEmbeddable.class);

		Table table = Table.create("withembeddable");
		assertThat(orderByFields)
				.contains(OrderByField.from(table.column(SqlIdentifier.quoted("HOME_COUNTRY_NAME")), Sort.Direction.ASC))
				.contains(OrderByField.from(table.column(SqlIdentifier.quoted("HOME_STREET")), Sort.Direction.ASC));
	}

	@Test // GH-2096
	void shouldMapSortPathIntoNestedEmbeddable() {

		List<OrderByField> orderByFields = map(Sort.by("home.country"), WithEmbeddable.class);

		Table table = Table.create("withembeddable");
		assertThat(orderByFields)
				.contains(OrderByField.from(table.column(SqlIdentifier.quoted("HOME_COUNTRY_NAME")), Sort.Direction.ASC));
	}

	@Test // GH-2096
	void shouldMapSortPathIntoEmbeddable() {

		List<OrderByField> orderByFields = map(Sort.by("home.country.name"), WithEmbeddable.class);

		Table table = Table.create("withembeddable");
		assertThat(orderByFields)
				.contains(OrderByField.from(table.column(SqlIdentifier.quoted("HOME_COUNTRY_NAME")), Sort.Direction.ASC));
	}

	private Condition map(Criteria criteria) {

		return mapper.getMappedObject(parameterSource, criteria, Table.create("person"),
				context.getRequiredPersistentEntity(Person.class));
	}

	private Condition map(Criteria criteria, Class<?> entityType) {

		return mapper.getMappedObject(parameterSource, criteria, Table.create("withcompositeid"),
				context.getRequiredPersistentEntity(entityType));
	}

	private List<OrderByField> map(Sort sort, Class<?> entityType) {

		return mapper.getMappedSort(Table.create(entityType.getSimpleName().toLowerCase()), sort,
				mapper.getMappingContext().getRequiredPersistentEntity(entityType));
	}

	static class Person {

		String name;
		@Column("another_name") String alternative;
	}

	private record CompositeId(int tenant, String name) {
	}

	private record WithCompositeId(@Id CompositeId id) {
	}

	static class WithEmbeddable {

		@Embedded.Nullable(prefix = "home_") Address home;

		@Embedded.Nullable(prefix = "work_") Address work;
	}

	static class Address {

		@Embedded.Nullable(prefix = "country_") Country country;
		String street;

		public Address(Country country) {
			this.country = country;
		}
	}

	static class Country {

		String name;

		public Country(String name) {
			this.name = name;
		}
	}

	enum CollectionToStringConverter implements Converter<Collection<?>, String> {
		INSTANCE;

		@Override
		public String convert(Collection<?> source) {
			return source.toString();
		}
	}
}
