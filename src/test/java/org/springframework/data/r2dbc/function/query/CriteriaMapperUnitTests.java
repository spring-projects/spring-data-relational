/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.r2dbc.function.query;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.spi.Statement;

import org.junit.Test;

import org.springframework.data.r2dbc.dialect.BindMarkersFactory;
import org.springframework.data.r2dbc.function.convert.MappingR2dbcConverter;
import org.springframework.data.r2dbc.function.convert.R2dbcConverter;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.sql.Table;

/**
 * Unit tests for {@link CriteriaMapper}.
 *
 * @author Mark Paluch
 */
public class CriteriaMapperUnitTests {

	R2dbcConverter converter = new MappingR2dbcConverter(new RelationalMappingContext());
	CriteriaMapper mapper = new CriteriaMapper(converter);
	Statement statementMock = mock(Statement.class);

	@Test // gh-64
	public void shouldMapSimpleCriteria() {

		Criteria criteria = Criteria.of("name").is("foo");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition().toString()).isEqualTo("person.name = ?[$1]");

		bindings.getBindings().apply(statementMock);
		verify(statementMock).bind(0, "foo");
	}

	@Test // gh-64
	public void shouldConsiderColumnName() {

		Criteria criteria = Criteria.of("alternative").is("foo");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition().toString()).isEqualTo("person.another_name = ?[$1]");
	}

	@Test // gh-64
	public void shouldMapAndCriteria() {

		Criteria criteria = Criteria.of("name").is("foo").and("bar").is("baz");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition().toString()).isEqualTo("person.name = ?[$1] AND person.bar = ?[$2]");

		bindings.getBindings().apply(statementMock);
		verify(statementMock).bind(0, "foo");
		verify(statementMock).bind(1, "baz");
	}

	@Test // gh-64
	public void shouldMapOrCriteria() {

		Criteria criteria = Criteria.of("name").is("foo").or("bar").is("baz");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition().toString()).isEqualTo("person.name = ?[$1] OR person.bar = ?[$2]");
	}

	@Test // gh-64
	public void shouldMapAndOrCriteria() {

		Criteria criteria = Criteria.of("name").is("foo") //
				.and("name").isNotNull() //
				.or("bar").is("baz") //
				.and("anotherOne").is("alternative");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition().toString()).isEqualTo(
				"person.name = ?[$1] AND person.name IS NOT NULL OR person.bar = ?[$2] AND person.anotherOne = ?[$3]");
	}

	@Test // gh-64
	public void shouldMapNeq() {

		Criteria criteria = Criteria.of("name").not("foo");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition().toString()).isEqualTo("person.name != ?[$1]");
	}

	@Test // gh-64
	public void shouldMapIsNull() {

		Criteria criteria = Criteria.of("name").isNull();

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition().toString()).isEqualTo("person.name IS NULL");
	}

	@Test // gh-64
	public void shouldMapIsNotNull() {

		Criteria criteria = Criteria.of("name").isNotNull();

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition().toString()).isEqualTo("person.name IS NOT NULL");
	}

	@Test // gh-64
	public void shouldMapIsIn() {

		Criteria criteria = Criteria.of("name").in("a", "b", "c");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition().toString()).isEqualTo("person.name IN (?[$1], ?[$2], ?[$3])");
	}

	@Test // gh-64
	public void shouldMapIsNotIn() {

		Criteria criteria = Criteria.of("name").notIn("a", "b", "c");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition().toString()).isEqualTo("NOT person.name IN (?[$1], ?[$2], ?[$3])");
	}

	@Test // gh-64
	public void shouldMapIsGt() {

		Criteria criteria = Criteria.of("name").greaterThan("a");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition().toString()).isEqualTo("person.name > ?[$1]");
	}

	@Test // gh-64
	public void shouldMapIsGte() {

		Criteria criteria = Criteria.of("name").greaterThanOrEquals("a");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition().toString()).isEqualTo("person.name >= ?[$1]");
	}

	@Test // gh-64
	public void shouldMapIsLt() {

		Criteria criteria = Criteria.of("name").lessThan("a");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition().toString()).isEqualTo("person.name < ?[$1]");
	}

	@Test // gh-64
	public void shouldMapIsLte() {

		Criteria criteria = Criteria.of("name").lessThanOrEquals("a");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition().toString()).isEqualTo("person.name <= ?[$1]");
	}

	@Test // gh-64
	public void shouldMapIsLike() {

		Criteria criteria = Criteria.of("name").like("a");

		BoundCondition bindings = map(criteria);

		assertThat(bindings.getCondition().toString()).isEqualTo("person.name LIKE ?[$1]");
	}

	@SuppressWarnings("unchecked")
	private BoundCondition map(Criteria criteria) {

		BindMarkersFactory markers = BindMarkersFactory.indexed("$", 1);

		return mapper.getMappedObject(markers.create(), criteria, Table.create("person"),
				converter.getMappingContext().getRequiredPersistentEntity(Person.class));
	}

	static class Person {

		String name;
		@Column("another_name") String alternative;
	}
}
