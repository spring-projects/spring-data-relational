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
package org.springframework.data.r2dbc.query;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.domain.Sort.Order.*;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.convert.MappingR2dbcConverter;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.dialect.BindMarkersFactory;
import org.springframework.data.r2dbc.dialect.BindTarget;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.r2dbc.mapping.SettableValue;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Functions;
import org.springframework.data.relational.core.sql.Table;

/**
 * Unit tests for {@link QueryMapper}.
 *
 * @author Mark Paluch
 * @author Mingyuan Wu
 */
public class QueryMapperUnitTests {

	R2dbcMappingContext context = new R2dbcMappingContext();
	R2dbcConverter converter = new MappingR2dbcConverter(context);

	QueryMapper mapper = new QueryMapper(PostgresDialect.INSTANCE, converter);
	BindTarget bindTarget = mock(BindTarget.class);

	@Test // gh-289
	public void shouldNotMapEmptyCriteria() {

		Criteria criteria = Criteria.empty();

		assertThatIllegalArgumentException().isThrownBy(() -> map(criteria));
	}

	@Test // gh-289
	public void shouldNotMapEmptyAndCriteria() {

		Criteria criteria = Criteria.empty().and(Collections.emptyList());

		assertThatIllegalArgumentException().isThrownBy(() -> map(criteria));
	}

	@Test // gh-289
	public void shouldNotMapEmptyNestedCriteria() {

		Criteria criteria = Criteria.empty().and(Collections.emptyList()).and(Criteria.empty().and(Criteria.empty()));

		assertThat(criteria.isEmpty()).isTrue();
		assertThatIllegalArgumentException().isThrownBy(() -> map(criteria));
	}

	@Test // gh-289
	public void shouldMapSomeNestedCriteria() {

		Criteria criteria = Criteria.empty().and(Collections.emptyList())
				.and(Criteria.empty().and(Criteria.where("name").is("Hank")));

		assertThat(criteria.isEmpty()).isFalse();

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("((person.name = ?[$1]))");
	}

	@Test // gh-289
	public void shouldMapNestedGroup() {

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
	public void shouldMapFrom() {

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
	public void shouldMapFromConcat() {

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
	public void shouldMapSimpleCriteria() {

		Criteria criteria = Criteria.where("name").is("foo");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name = ?[$1]");

		bindings.getBindings().apply(bindTarget);
		verify(bindTarget).bind(0, "foo");
	}

	@Test // gh-300
	public void shouldMapSimpleCriteriaWithoutEntity() {

		Criteria criteria = Criteria.where("name").is("foo");

		BindMarkersFactory markers = BindMarkersFactory.indexed("$", 1);

		BoundCondition bindings = mapper.getMappedObject(markers.create(), criteria, Table.create("person"), null);

		assertThat(bindings.getCondition()).hasToString("person.name = ?[$1]");

		bindings.getBindings().apply(bindTarget);
		verify(bindTarget).bind(0, "foo");
	}

	@Test // gh-300
	public void shouldMapExpression() {

		Table table = Table.create("my_table").as("my_aliased_table");

		Expression mappedObject = mapper.getMappedObject(table.column("alternative").as("my_aliased_col"),
				context.getRequiredPersistentEntity(Person.class));

		assertThat(mappedObject).hasToString("my_aliased_table.another_name AS my_aliased_col");
	}

	@Test // gh-300
	public void shouldMapCountFunction() {

		Table table = Table.create("my_table").as("my_aliased_table");

		Expression mappedObject = mapper.getMappedObject(Functions.count(table.column("alternative")),
				context.getRequiredPersistentEntity(Person.class));

		assertThat(mappedObject).hasToString("COUNT(my_aliased_table.another_name)");
	}

	@Test // gh-300
	public void shouldMapExpressionToUnknownColumn() {

		Table table = Table.create("my_table").as("my_aliased_table");

		Expression mappedObject = mapper.getMappedObject(table.column("unknown").as("my_aliased_col"),
				context.getRequiredPersistentEntity(Person.class));

		assertThat(mappedObject).hasToString("my_aliased_table.unknown AS my_aliased_col");
	}

	@Test // gh-300
	public void shouldMapExpressionWithoutEntity() {

		Table table = Table.create("my_table").as("my_aliased_table");

		Expression mappedObject = mapper.getMappedObject(table.column("my_col").as("my_aliased_col"), null);

		assertThat(mappedObject).hasToString("my_aliased_table.my_col AS my_aliased_col");
	}

	@Test // gh-64
	public void shouldMapSimpleNullableCriteria() {

		Criteria criteria = Criteria.where("name").is(SettableValue.empty(Integer.class));

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name = ?[$1]");

		bindings.getBindings().apply(bindTarget);
		verify(bindTarget).bindNull(0, Integer.class);
	}

	@Test // gh-64
	public void shouldConsiderColumnName() {

		Criteria criteria = Criteria.where("alternative").is("foo");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.another_name = ?[$1]");
	}

	@Test // gh-64
	public void shouldMapAndCriteria() {

		Criteria criteria = Criteria.where("name").is("foo").and("bar").is("baz");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name = ?[$1] AND person.bar = ?[$2]");

		bindings.getBindings().apply(bindTarget);
		verify(bindTarget).bind(0, "foo");
		verify(bindTarget).bind(1, "baz");
	}

	@Test // gh-64
	public void shouldMapOrCriteria() {

		Criteria criteria = Criteria.where("name").is("foo").or("bar").is("baz");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name = ?[$1] OR person.bar = ?[$2]");
	}

	@Test // gh-64
	public void shouldMapAndOrCriteria() {

		Criteria criteria = Criteria.where("name").is("foo") //
				.and("name").isNotNull() //
				.or("bar").is("baz") //
				.and("anotherOne").is("alternative");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString(
				"person.name = ?[$1] AND person.name IS NOT NULL OR person.bar = ?[$2] AND person.anotherOne = ?[$3]");
	}

	@Test // gh-64
	public void shouldMapNeq() {

		Criteria criteria = Criteria.where("name").not("foo");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name != ?[$1]");
	}

	@Test // gh-64
	public void shouldMapIsNull() {

		Criteria criteria = Criteria.where("name").isNull();

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name IS NULL");
	}

	@Test // gh-64
	public void shouldMapIsNotNull() {

		Criteria criteria = Criteria.where("name").isNotNull();

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name IS NOT NULL");
	}

	@Test // gh-64
	public void shouldMapIsIn() {

		Criteria criteria = Criteria.where("name").in("a", "b", "c");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name IN (?[$1], ?[$2], ?[$3])");
	}

	@Test // gh-64, gh-177
	public void shouldMapIsNotIn() {

		Criteria criteria = Criteria.where("name").notIn("a", "b", "c");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name NOT IN (?[$1], ?[$2], ?[$3])");
	}

	@Test // gh-64
	public void shouldMapIsGt() {

		Criteria criteria = Criteria.where("name").greaterThan("a");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name > ?[$1]");
	}

	@Test // gh-64
	public void shouldMapIsGte() {

		Criteria criteria = Criteria.where("name").greaterThanOrEquals("a");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name >= ?[$1]");
	}

	@Test // gh-64
	public void shouldMapIsLt() {

		Criteria criteria = Criteria.where("name").lessThan("a");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name < ?[$1]");
	}

	@Test // gh-64
	public void shouldMapIsLte() {

		Criteria criteria = Criteria.where("name").lessThanOrEquals("a");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name <= ?[$1]");
	}

	@Test // gh-64
	public void shouldMapIsLike() {

		Criteria criteria = Criteria.where("name").like("a");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.name LIKE ?[$1]");
	}

	@Test // gh-64
	public void shouldMapSort() {

		Sort sort = Sort.by(desc("alternative"));

		Sort mapped = mapper.getMappedObject(sort, context.getRequiredPersistentEntity(Person.class));

		assertThat(mapped.getOrderFor("another_name")).isEqualTo(desc("another_name"));
		assertThat(mapped.getOrderFor("alternative")).isNull();
	}

	@Test // gh-369
	public void mapSortForPropertyPathInPrimitiveShouldFallBackToColumnName() {

		Sort sort = Sort.by(desc("alternative_name"));

		Sort mapped = mapper.getMappedObject(sort, context.getRequiredPersistentEntity(Person.class));
		assertThat(mapped.getOrderFor("alternative_name")).isEqualTo(desc("alternative_name"));
	}

	@Test // gh-369
	public void mapQueryForPropertyPathInPrimitiveShouldFallBackToColumnName() {

		Criteria criteria = Criteria.where("alternative_name").is("a");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition()).hasToString("person.alternative_name = ?[$1]");
	}

	private BoundCondition map(Criteria criteria) {

		BindMarkersFactory markers = BindMarkersFactory.indexed("$", 1);

		return mapper.getMappedObject(markers.create(), criteria, Table.create("person"),
				context.getRequiredPersistentEntity(Person.class));
	}

	static class Person {

		String name;
		@Column("another_name") String alternative;
	}
}
