/*
 * Copyright 2019-2024 the original author or authors.
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
package org.springframework.data.r2dbc.query;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.domain.Sort.Order.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.convert.MappingR2dbcConverter;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.convert.R2dbcCustomConversions;
import org.springframework.data.r2dbc.dialect.MySqlDialect;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Functions;
import org.springframework.data.relational.core.sql.OrderByField;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.domain.SqlSort;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.r2dbc.core.binding.BindMarkersFactory;
import org.springframework.r2dbc.core.binding.BindTarget;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.node.TextNode;

/**
 * Unit tests for {@link QueryMapper}.
 *
 * @author Mark Paluch
 * @author Mingyuan Wu
 * @author Jens Schauder
 * @author Yan Qiang
 */
class QueryMapperUnitTests {

	private BindTarget bindTarget = mock(BindTarget.class);
	private QueryMapper mapper = createMapper(PostgresDialect.INSTANCE);

	QueryMapper createMapper(R2dbcDialect dialect) {
		return createMapper(dialect, JsonNodeToStringConverter.INSTANCE, StringToJsonNodeConverter.INSTANCE);
	}

	QueryMapper createMapper(R2dbcDialect dialect, Converter<?, ?>... converters) {

		R2dbcCustomConversions conversions = R2dbcCustomConversions.of(dialect, Arrays.asList(converters));

		R2dbcMappingContext context = new R2dbcMappingContext();
		context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		context.afterPropertiesSet();

		R2dbcConverter converter = new MappingR2dbcConverter(context, conversions);

		return new QueryMapper(dialect, converter);
	}

	@Test // gh-289
	void shouldNotMapEmptyCriteria() {

		Criteria criteria = Criteria.empty();

		assertThatIllegalArgumentException().isThrownBy(() -> map(criteria));
	}

	@Test // gh-289
	void shouldNotMapEmptyAndCriteria() {

		Criteria criteria = Criteria.empty().and(Collections.emptyList());

		assertThatIllegalArgumentException().isThrownBy(() -> map(criteria));
	}

	@Test // gh-289
	void shouldNotMapEmptyNestedCriteria() {

		Criteria criteria = Criteria.empty().and(Collections.emptyList()).and(Criteria.empty().and(Criteria.empty()));

		assertThat(criteria.isEmpty()).isTrue();
		assertThatIllegalArgumentException().isThrownBy(() -> map(criteria));
	}

	@Test // gh-289
	void shouldMapSomeNestedCriteria() {

		Criteria criteria = Criteria.empty().and(Collections.emptyList())
				.and(Criteria.empty().and(Criteria.where("name").is("Hank")));

		assertThat(criteria.isEmpty()).isFalse();

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("((person.name = ?[$1]))");
	}

	@Test // gh-289
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

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString(
				"(person.name = ?[$1]) AND (person.name = ?[$2] OR person.age < ?[$3] OR (person.name != ?[$4] AND person.age > ?[$5]))");
	}

	@Test // gh-289
	void shouldMapFrom() {

		Criteria criteria = Criteria.from(Criteria.where("name").is("Foo")) //
				.and(Criteria.where("name").is("Bar") //
						.or("age").lessThan(49) //
				);

		assertThat(criteria.isEmpty()).isFalse();

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition())
				.hasToString("person.name = ?[$1] AND (person.name = ?[$2] OR person.age < ?[$3])");
	}

	@Test // gh-383
	void shouldMapFromConcat() {

		Criteria criteria = Criteria.from(Criteria.where("name").is("Foo"), Criteria.where("name").is("Bar") //
				.or("age").lessThan(49));

		assertThat(map(criteria).getCondition())
				.hasToString("(person.name = ?[$1] AND (person.name = ?[$2] OR person.age < ?[$3]))");

		criteria = Criteria.from(Criteria.where("name").is("Foo"), Criteria.where("name").is("Bar") //
				.or("age").lessThan(49), Criteria.where("foo").is("bar"));

		assertThat(map(criteria).getCondition())
				.hasToString("(person.name = ?[$1] AND (person.name = ?[$2] OR person.age < ?[$3]) AND (person.foo = ?[$4]))");
	}

	@Test // gh-64
	void shouldMapSimpleCriteria() {

		Criteria criteria = Criteria.where("name").is("foo");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name = ?[$1]");

		bindings.getBindings().apply(bindTarget);
		verify(bindTarget).bind(0, "foo");
	}

	@Test // gh-518
	void shouldMapSimpleCriteriaWithIgnoreCase() {

		Criteria criteria = Criteria.where("some_col").is("foo").ignoreCase(true);

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("UPPER(person.some_col) = UPPER(?[$1])");

		bindings.getBindings().apply(bindTarget);
		verify(bindTarget).bind(0, "foo");
	}

	@Test // gh-300
	void shouldMapSimpleCriteriaWithoutEntity() {

		Criteria criteria = Criteria.where("name").is("foo");

		BindMarkersFactory markers = BindMarkersFactory.indexed("$", 1);

		BoundCondition bindings = mapper.getMappedObject(markers.create(), criteria, Table.create("person"), null);

		assertThat(bindings.getCondition()).hasToString("person.name = ?[$1]");

		bindings.getBindings().apply(bindTarget);
		verify(bindTarget).bind(0, "foo");
	}

	@Test // gh-300
	void shouldMapExpression() {

		Table table = Table.create("my_table").as("my_aliased_table");

		Expression mappedObject = mapper.getMappedObject(table.column("alternative").as("my_aliased_col"),
				mapper.getMappingContext().getRequiredPersistentEntity(Person.class));

		assertThat(mappedObject).hasToString("my_aliased_table.another_name AS my_aliased_col");
	}

	@Test // gh-300
	void shouldMapCountFunction() {

		Table table = Table.create("my_table").as("my_aliased_table");

		Expression mappedObject = mapper.getMappedObject(Functions.count(table.column("alternative")),
				mapper.getMappingContext().getRequiredPersistentEntity(Person.class));

		assertThat(mappedObject).hasToString("COUNT(my_aliased_table.another_name)");
	}

	@Test // gh-300
	void shouldMapExpressionToUnknownColumn() {

		Table table = Table.create("my_table").as("my_aliased_table");

		Expression mappedObject = mapper.getMappedObject(table.column("unknown").as("my_aliased_col"),
				mapper.getMappingContext().getRequiredPersistentEntity(Person.class));

		assertThat(mappedObject).hasToString("my_aliased_table.unknown AS my_aliased_col");
	}

	@Test // gh-300
	void shouldMapExpressionWithoutEntity() {

		Table table = Table.create("my_table").as("my_aliased_table");

		Expression mappedObject = mapper.getMappedObject(table.column("my_col").as("my_aliased_col"), null);

		assertThat(mappedObject).hasToString("my_aliased_table.my_col AS my_aliased_col");
	}

	@Test // gh-64
	void shouldMapSimpleNullableCriteria() {

		Criteria criteria = Criteria.where("name").is(Parameter.empty(Integer.class));

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name = ?[$1]");

		bindings.getBindings().apply(bindTarget);
		verify(bindTarget).bindNull(0, Integer.class);
	}

	@Test // gh-64
	void shouldConsiderColumnName() {

		Criteria criteria = Criteria.where("alternative").is("foo");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.another_name = ?[$1]");
	}

	@Test // gh-64
	void shouldMapAndCriteria() {

		Criteria criteria = Criteria.where("name").is("foo").and("bar").is("baz");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name = ?[$1] AND person.bar = ?[$2]");

		bindings.getBindings().apply(bindTarget);
		verify(bindTarget).bind(0, "foo");
		verify(bindTarget).bind(1, "baz");
	}

	@Test // gh-64
	void shouldMapOrCriteria() {

		Criteria criteria = Criteria.where("name").is("foo").or("bar").is("baz");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name = ?[$1] OR person.bar = ?[$2]");
	}

	@Test // gh-64
	void shouldMapAndOrCriteria() {

		Criteria criteria = Criteria.where("name").is("foo") //
				.and("name").isNotNull() //
				.or("bar").is("baz") //
				.and("anotherOne").is("alternative");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString(
				"person.name = ?[$1] AND person.name IS NOT NULL OR person.bar = ?[$2] AND person.anotherOne = ?[$3]");
	}

	@Test // gh-64
	void shouldMapNeq() {

		Criteria criteria = Criteria.where("name").not("foo");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name != ?[$1]");
	}

	@Test // gh-64
	void shouldMapIsNull() {

		Criteria criteria = Criteria.where("name").isNull();

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name IS NULL");
	}

	@Test // gh-64
	void shouldMapIsNotNull() {

		Criteria criteria = Criteria.where("name").isNotNull();

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name IS NOT NULL");
	}

	@Test // gh-64
	void shouldMapIsIn() {

		Criteria criteria = Criteria.where("name").in("a", "b", "c");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name IN (?[$1], ?[$2], ?[$3])");
	}

	@Test // gh-64, gh-177
	void shouldMapIsNotIn() {

		Criteria criteria = Criteria.where("name").notIn("a", "b", "c");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name NOT IN (?[$1], ?[$2], ?[$3])");
	}

	@Test
	void shouldMapIsNotInWithCollectionToStringConverter() {

		mapper = createMapper(PostgresDialect.INSTANCE, JsonNodeToStringConverter.INSTANCE,
				StringToJsonNodeConverter.INSTANCE, CollectionToStringConverter.INSTANCE);

		Criteria criteria = Criteria.where("name").notIn("a", "b", "c");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name NOT IN (?[$1], ?[$2], ?[$3])");
	}

	@Test // gh-64
	void shouldMapIsGt() {

		Criteria criteria = Criteria.where("name").greaterThan("a");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name > ?[$1]");
	}

	@Test // gh-64
	void shouldMapIsGte() {

		Criteria criteria = Criteria.where("name").greaterThanOrEquals("a");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name >= ?[$1]");
	}

	@Test // gh-64
	void shouldMapIsLt() {

		Criteria criteria = Criteria.where("name").lessThan("a");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name < ?[$1]");
	}

	@Test // gh-64
	void shouldMapIsLte() {

		Criteria criteria = Criteria.where("name").lessThanOrEquals("a");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name <= ?[$1]");
	}

	@Test // gh-64
	void shouldMapIsLike() {

		Criteria criteria = Criteria.where("name").like("a");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name LIKE ?[$1]");
	}

	@Test // GH-1507
	public void shouldMapSortWithUnknownField() {

		Sort sort = Sort.by(desc("unknownField"));

		List<OrderByField> fields = mapper.getMappedSort(Table.create("tbl"), sort,
				mapper.getMappingContext().getRequiredPersistentEntity(Person.class));

		assertThat(fields) //
				.extracting(Objects::toString) //
				.containsExactly("tbl.unknownField DESC");
	}

	@Test // GH-1512
	public void shouldTablePrefixUnsafeOrderExpression() {

		Sort sort = Sort.by(SqlSort.SqlOrder.desc("unknownField").withUnsafe());

		List<OrderByField> fields = mapper.getMappedSort(Table.create("tbl"), sort,
				mapper.getMappingContext().getRequiredPersistentEntity(Person.class));

		assertThat(fields) //
				.extracting(Objects::toString) //
				.containsExactly("unknownField DESC");
	}

	@Test // GH-1507
	public void shouldMapSortWithAllowedSpecialCharacters() {

		Sort sort = Sort.by(desc("x(._)x"));

		List<OrderByField> fields = mapper.getMappedSort(Table.create("tbl"), sort,
				mapper.getMappingContext().getRequiredPersistentEntity(Person.class));

		assertThat(fields) //
				.extracting(Objects::toString) //
				.containsExactly("tbl.x(._)x DESC");
	}

	@Test // GH-1507
	public void shouldNotMapSortWithIllegalExpression() {

		Sort sort = Sort.by(desc("unknown Field"));

		assertThatThrownBy(() -> mapper.getMappedSort(Table.create("tbl"), sort,
				mapper.getMappingContext().getRequiredPersistentEntity(Person.class)))
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test // gh-369
	void mapQueryForPropertyPathInPrimitiveShouldFallBackToColumnName() {

		Criteria criteria = Criteria.where("alternative_name").is("a");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.alternative_name = ?[$1]");
	}

	@Test // gh-593
	void mapQueryForEnumArrayShouldMapToStringList() {

		Criteria criteria = Criteria.where("enumValue").in(MyEnum.ONE, MyEnum.TWO);

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.enum_value IN (?[$1], ?[$2])");
	}

	@Test // gh-733
	void shouldMapBooleanConditionProperly() {

		Criteria criteria = Criteria.where("state").isFalse();

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.state = ?[$1]");
		assertThat(bindings.getBindings().iterator().next().getValue()).isEqualTo(false);
	}

	@Test // gh-733
	void shouldMapAndConvertBooleanConditionProperly() {

		mapper = createMapper(MySqlDialect.INSTANCE);
		Criteria criteria = Criteria.where("state").isTrue();

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.state = ?[$1]");
		assertThat(bindings.getBindings().iterator().next().getValue()).isEqualTo((byte) 1);
	}

	@Test // gh-1452
	void shouldMapJsonNodeToString() {

		Criteria criteria = Criteria.where("jsonNode").is(new TextNode("foo"));

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.json_node = ?[$1]");
		assertThat(bindings.getBindings().iterator().next().getValue()).isEqualTo("foo");
	}

	@Test // gh-1452
	void shouldMapJsonNodeListToString() {

		Criteria criteria = Criteria.where("jsonNode").in(new TextNode("foo"), new TextNode("bar"));

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.json_node IN (?[$1], ?[$2])");
		assertThat(bindings.getBindings().iterator().next().getValue()).isEqualTo("foo");
	}

	private BoundCondition map(Criteria criteria) {

		BindMarkersFactory markers = BindMarkersFactory.indexed("$", 1);

		return mapper.getMappedObject(markers.create(), criteria, Table.create("person"),
				mapper.getMappingContext().getRequiredPersistentEntity(Person.class));
	}

	static class Person {

		String name;
		@Column("another_name") String alternative;
		MyEnum enumValue;

		boolean state;

		JsonNode jsonNode;
	}

	enum MyEnum {
		ONE, TWO,
	}

	enum JsonNodeToStringConverter implements Converter<JsonNode, String> {
		INSTANCE;

		@Override
		public String convert(JsonNode source) {
			return source.asText();
		}
	}

	enum CollectionToStringConverter implements Converter<Collection<?>, String> {
		INSTANCE;

		@Override
		public String convert(Collection<?> source) {
			return source.toString();
		}
	}

	enum StringToJsonNodeConverter implements Converter<String, JsonNode> {
		INSTANCE;

		@Override
		public JsonNode convert(String source) {
			return new TextNode(source);
		}
	}
}
