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

import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.springframework.data.r2dbc.convert.MappingR2dbcConverter;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.query.Update;
import org.springframework.data.relational.core.sql.AssignValue;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.r2dbc.core.binding.BindMarkersFactory;
import org.springframework.r2dbc.core.binding.BindTarget;

/**
 * Unit tests for {@link UpdateMapper}.
 *
 * @author Mark Paluch
 * @author Mingyuan Wu
 */
public class UpdateMapperUnitTests {

	private final R2dbcConverter converter = new MappingR2dbcConverter(new R2dbcMappingContext());
	private final UpdateMapper mapper = new UpdateMapper(PostgresDialect.INSTANCE, converter);
	private final BindTarget bindTarget = mock(BindTarget.class);

	@Test // gh-64
	void shouldMapFieldNamesInUpdate() {

		Update update = Update.update("alternative", "foo");

		BoundAssignments mapped = map(update);

		Map<SqlIdentifier, Expression> assignments = mapped.getAssignments().stream().map(it -> (AssignValue) it)
				.collect(Collectors.toMap(k -> k.getColumn().getName(), AssignValue::getValue));

		assertThat(assignments).containsEntry(SqlIdentifier.unquoted("another_name"), SQL.bindMarker("$1"));
	}

	@Test // gh-64
	void shouldUpdateToSettableValue() {

		Update update = Update.update("alternative", Parameter.empty(String.class));

		BoundAssignments mapped = map(update);

		Map<SqlIdentifier, Expression> assignments = mapped.getAssignments().stream().map(it -> (AssignValue) it)
				.collect(Collectors.toMap(k -> k.getColumn().getName(), AssignValue::getValue));

		assertThat(assignments).containsEntry(SqlIdentifier.unquoted("another_name"), SQL.bindMarker("$1"));

		mapped.getBindings().apply(bindTarget);
		verify(bindTarget).bindNull(0, String.class);
	}

	@Test // gh-64
	void shouldUpdateToNull() {

		Update update = Update.update("alternative", null);

		BoundAssignments mapped = map(update);

		assertThat(mapped.getAssignments()).hasSize(1);
		assertThat(mapped.getAssignments().get(0).toString()).isEqualTo("person.another_name = NULL");

		mapped.getBindings().apply(bindTarget);
		verifyNoInteractions(bindTarget);
	}

	@Test // gh-195
	void shouldMapMultipleFields() {

		Update update = Update.update("c1", "a").set("c2", "b").set("c3", "c");

		BoundAssignments mapped = map(update);

		Map<SqlIdentifier, Expression> assignments = mapped.getAssignments().stream().map(it -> (AssignValue) it)
				.collect(Collectors.toMap(k -> k.getColumn().getName(), AssignValue::getValue));

		assertThat(update.getAssignments()).hasSize(3);
		assertThat(assignments).hasSize(3).containsEntry(SqlIdentifier.unquoted("c1"), SQL.bindMarker("$1"))
				.containsEntry(SqlIdentifier.unquoted("c2"), SQL.bindMarker("$2"));
	}

	private BoundAssignments map(Update update) {

		BindMarkersFactory markers = BindMarkersFactory.indexed("$", 1);

		return mapper.getMappedObject(markers.create(), update, Table.create("person"),
				converter.getMappingContext().getRequiredPersistentEntity(Person.class));
	}

	static class Person {

		String name;
		@Column("another_name") String alternative;
	}
}
