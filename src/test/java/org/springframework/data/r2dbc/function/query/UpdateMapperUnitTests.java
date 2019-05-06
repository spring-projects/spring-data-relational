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

import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Test;

import org.springframework.data.r2dbc.dialect.BindMarkersFactory;
import org.springframework.data.r2dbc.domain.BindTarget;
import org.springframework.data.r2dbc.domain.SettableValue;
import org.springframework.data.r2dbc.function.convert.MappingR2dbcConverter;
import org.springframework.data.r2dbc.function.convert.R2dbcConverter;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.sql.AssignValue;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.Table;

/**
 * Unit tests for {@link UpdateMapper}.
 *
 * @author Mark Paluch
 */
public class UpdateMapperUnitTests {

	R2dbcConverter converter = new MappingR2dbcConverter(new RelationalMappingContext());
	UpdateMapper mapper = new UpdateMapper(converter);
	BindTarget bindTarget = mock(BindTarget.class);

	@Test // gh-64
	public void shouldMapFieldNamesInUpdate() {

		Update update = Update.update("alternative", "foo");

		BoundAssignments mapped = map(update);

		Map<String, Expression> assignments = mapped.getAssignments().stream().map(it -> (AssignValue) it)
				.collect(Collectors.toMap(k -> k.getColumn().getName(), AssignValue::getValue));

		assertThat(assignments).containsEntry("another_name", SQL.bindMarker("$1"));
	}

	@Test // gh-64
	public void shouldUpdateToSettableValue() {

		Update update = Update.update("alternative", SettableValue.empty(String.class));

		BoundAssignments mapped = map(update);

		Map<String, Expression> assignments = mapped.getAssignments().stream().map(it -> (AssignValue) it)
				.collect(Collectors.toMap(k -> k.getColumn().getName(), AssignValue::getValue));

		assertThat(assignments).containsEntry("another_name", SQL.bindMarker("$1"));

		mapped.getBindings().apply(bindTarget);
		verify(bindTarget).bindNull(0, String.class);
	}

	@Test // gh-64
	public void shouldUpdateToNull() {

		Update update = Update.update("alternative", null);

		BoundAssignments mapped = map(update);

		assertThat(mapped.getAssignments()).hasSize(1);
		assertThat(mapped.getAssignments().get(0).toString()).isEqualTo("person.another_name = NULL");

		mapped.getBindings().apply(bindTarget);
		verifyZeroInteractions(bindTarget);
	}

	@SuppressWarnings("unchecked")
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
