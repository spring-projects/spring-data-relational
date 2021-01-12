/*
 * Copyright 2020-2021 the original author or authors.
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
package org.springframework.data.jdbc.repository.query;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.domain.Sort.Order.*;

import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.BasicJdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.RelationResolver;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.dialect.PostgresDialect;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Functions;
import org.springframework.data.relational.core.sql.OrderByField;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

/**
 * Unit tests for {@link QueryMapper}.
 *
 * @author Mark Paluch
 */
public class QueryMapperUnitTests {

	JdbcMappingContext context = new JdbcMappingContext();
	JdbcConverter converter = new BasicJdbcConverter(context, mock(RelationResolver.class));

	QueryMapper mapper = new QueryMapper(PostgresDialect.INSTANCE, converter);
	MapSqlParameterSource parameterSource = new MapSqlParameterSource();

	@Test // DATAJDBC-318
	public void shouldNotMapEmptyCriteria() {

		Criteria criteria = Criteria.empty();

		assertThatIllegalArgumentException().isThrownBy(() -> map(criteria));
	}

	@Test // DATAJDBC-318
	public void shouldNotMapEmptyAndCriteria() {

		Criteria criteria = Criteria.empty().and(Collections.emptyList());

		assertThatIllegalArgumentException().isThrownBy(() -> map(criteria));
	}

	@Test // DATAJDBC-318
	public void shouldNotMapEmptyNestedCriteria() {

		Criteria criteria = Criteria.empty().and(Collections.emptyList()).and(Criteria.empty().and(Criteria.empty()));

		assertThat(criteria.isEmpty()).isTrue();
		assertThatIllegalArgumentException().isThrownBy(() -> map(criteria));
	}

	@Test // DATAJDBC-318
	public void shouldMapSomeNestedCriteria() {

		Criteria criteria = Criteria.empty().and(Collections.emptyList())
				.and(Criteria.empty().and(Criteria.where("name").is("Hank")));

		assertThat(criteria.isEmpty()).isFalse();

		Condition condition = map(criteria);

		assertThat(condition).hasToString("((person.\"NAME\" = ?[:name]))");
	}

	@Test // DATAJDBC-318
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

		Condition condition = map(criteria);

		assertThat(condition).hasToString(
				"(person.\"NAME\" = ?[:name]) AND (person.\"NAME\" = ?[:name1] OR person.age < ?[:age] OR (person.\"NAME\" != ?[:name2] AND person.age > ?[:age1]))");
	}

	@Test // DATAJDBC-318
	public void shouldMapFrom() {

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
	public void shouldMapFromConcat() {

		Criteria criteria = Criteria.from(Criteria.where("name").is("Foo"), Criteria.where("name").is("Bar") //
				.or("age").lessThan(49));

		assertThat(criteria.isEmpty()).isFalse();

		Condition condition = map(criteria);

		assertThat(condition)
				.hasToString("(person.\"NAME\" = ?[:name] AND (person.\"NAME\" = ?[:name1] OR person.age < ?[:age]))");
	}

	@Test // DATAJDBC-318
	public void shouldMapSimpleCriteria() {

		Criteria criteria = Criteria.where("name").is("foo");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" = ?[:name]");
	}

	@Test // DATAJDBC-318
	public void shouldMapSimpleCriteriaWithoutEntity() {

		Criteria criteria = Criteria.where("name").is("foo");

		Condition condition = mapper.getMappedObject(new MapSqlParameterSource(), criteria, Table.create("person"), null);

		assertThat(condition).hasToString("person.name = ?[:name]");
	}

	@Test // DATAJDBC-318
	public void shouldMapExpression() {

		Table table = Table.create("my_table").as("my_aliased_table");

		Expression mappedObject = mapper.getMappedObject(table.column("alternative").as("my_aliased_col"),
				context.getRequiredPersistentEntity(Person.class));

		assertThat(mappedObject).hasToString("my_aliased_table.\"another_name\" AS my_aliased_col");
	}

	@Test // DATAJDBC-318
	public void shouldMapCountFunction() {

		Table table = Table.create("my_table").as("my_aliased_table");

		Expression mappedObject = mapper.getMappedObject(Functions.count(table.column("alternative")),
				context.getRequiredPersistentEntity(Person.class));

		assertThat(mappedObject).hasToString("COUNT(my_aliased_table.\"another_name\")");
	}

	@Test // DATAJDBC-318
	public void shouldMapExpressionToUnknownColumn() {

		Table table = Table.create("my_table").as("my_aliased_table");

		Expression mappedObject = mapper.getMappedObject(table.column("unknown").as("my_aliased_col"),
				context.getRequiredPersistentEntity(Person.class));

		assertThat(mappedObject).hasToString("my_aliased_table.unknown AS my_aliased_col");
	}

	@Test // DATAJDBC-318
	public void shouldMapExpressionWithoutEntity() {

		Table table = Table.create("my_table").as("my_aliased_table");

		Expression mappedObject = mapper.getMappedObject(table.column("my_col").as("my_aliased_col"), null);

		assertThat(mappedObject).hasToString("my_aliased_table.my_col AS my_aliased_col");
	}

	@Test // DATAJDBC-318
	public void shouldMapSimpleNullableCriteria() {

		Criteria criteria = Criteria.where("name").isNull();

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" IS NULL");
	}

	@Test // DATAJDBC-318
	public void shouldConsiderColumnName() {

		Criteria criteria = Criteria.where("alternative").is("foo");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"another_name\" = ?[:another_name]");
	}

	@Test // DATAJDBC-318
	public void shouldMapAndCriteria() {

		Criteria criteria = Criteria.where("name").is("foo").and("bar").is("baz");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" = ?[:name] AND person.bar = ?[:bar]");
	}

	@Test // DATAJDBC-318
	public void shouldMapOrCriteria() {

		Criteria criteria = Criteria.where("name").is("foo").or("bar").is("baz");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" = ?[:name] OR person.bar = ?[:bar]");
	}

	@Test // DATAJDBC-318
	public void shouldMapAndOrCriteria() {

		Criteria criteria = Criteria.where("name").is("foo") //
				.and("name").isNotNull() //
				.or("bar").is("baz") //
				.and("anotherOne").is("alternative");

		Condition condition = map(criteria);

		assertThat(condition).hasToString(
				"person.\"NAME\" = ?[:name] AND person.\"NAME\" IS NOT NULL OR person.bar = ?[:bar] AND person.anotherOne = ?[:anotherOne]");
	}

	@Test // DATAJDBC-318
	public void shouldMapNeq() {

		Criteria criteria = Criteria.where("name").not("foo");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" != ?[:name]");
	}

	@Test // DATAJDBC-318
	public void shouldMapIsNull() {

		Criteria criteria = Criteria.where("name").isNull();

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" IS NULL");
	}

	@Test // DATAJDBC-318
	public void shouldMapIsNotNull() {

		Criteria criteria = Criteria.where("name").isNotNull();

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" IS NOT NULL");
	}

	@Test // DATAJDBC-318
	public void shouldMapIsIn() {

		Criteria criteria = Criteria.where("name").in("a", "b", "c");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" IN (?[:name], ?[:name1], ?[:name2])");
	}

	@Test // DATAJDBC-318
	public void shouldMapIsNotIn() {

		Criteria criteria = Criteria.where("name").notIn("a", "b", "c");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" NOT IN (?[:name], ?[:name1], ?[:name2])");
	}

	@Test // DATAJDBC-318
	public void shouldMapIsGt() {

		Criteria criteria = Criteria.where("name").greaterThan("a");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" > ?[:name]");
	}

	@Test // DATAJDBC-318
	public void shouldMapIsGte() {

		Criteria criteria = Criteria.where("name").greaterThanOrEquals("a");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" >= ?[:name]");
	}

	@Test // DATAJDBC-318
	public void shouldMapIsLt() {

		Criteria criteria = Criteria.where("name").lessThan("a");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" < ?[:name]");
	}

	@Test // DATAJDBC-318
	public void shouldMapIsLte() {

		Criteria criteria = Criteria.where("name").lessThanOrEquals("a");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" <= ?[:name]");
	}

	@Test // DATAJDBC-318
	public void shouldMapBetween() {

		Criteria criteria = Criteria.where("name").between("a", "b");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" BETWEEN ?[:name] AND ?[:name1]");
	}

	@Test // DATAJDBC-318
	public void shouldMapIsLike() {

		Criteria criteria = Criteria.where("name").like("a");

		Condition condition = map(criteria);

		assertThat(condition).hasToString("person.\"NAME\" LIKE ?[:name]");
	}

	@Test // DATAJDBC-318
	public void shouldMapSort() {

		Sort sort = Sort.by(desc("alternative"));

		List<OrderByField> fields = mapper.getMappedSort(Table.create("tbl"), sort,
				context.getRequiredPersistentEntity(Person.class));

		assertThat(fields).hasSize(1);
		assertThat(fields.get(0)).hasToString("tbl.\"another_name\" DESC");
	}

	private Condition map(Criteria criteria) {

		return mapper.getMappedObject(parameterSource, criteria, Table.create("person"),
				context.getRequiredPersistentEntity(Person.class));
	}

	static class Person {

		String name;
		@Column("another_name") String alternative;
	}
}
