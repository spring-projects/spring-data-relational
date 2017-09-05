/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.core;

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import lombok.RequiredArgsConstructor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.OperationNotSupportedException;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.mapping.model.DefaultNamingStrategy;
import org.springframework.data.jdbc.mapping.model.JdbcMappingContext;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntity;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentProperty;
import org.springframework.util.Assert;

/**
 * Tests the extraction of entities from a {@link ResultSet} by the {@link EntityRowMapper}.
 *
 * @author Jens Schauder
 */
public class EntityRowMapperUnitTests {

	@Test // DATAJDBC-113
	public void simpleEntitiesGetProperlyExtracted() throws SQLException {

		ResultSet rs = mockResultSet(asList("id", "name"), //
				23L, "alpha");
		rs.next();

		Trivial extracted = createRowMapper(Trivial.class).mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.name) //
				.containsExactly(23L, "alpha");

	}

	@Test // DATAJDBC-113
	public void simpleOneToOneGetsProperlyExtracted() throws SQLException {

		ResultSet rs = mockResultSet(asList("id", "name", "child_id", "child_name"), //
				23L, "alpha", 42L, "beta");
		rs.next();

		OneToOne extracted = createRowMapper(OneToOne.class).mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.name, e -> e.child.id, e -> e.child.name) //
				.containsExactly(23L, "alpha", 42L, "beta");
	}

	@Test // DATAJDBC-113
	public void collectionReferenceGetsLoadedWithAdditionalSelect() throws SQLException {

		ResultSet rs = mockResultSet(asList("id", "name"), //
				23L, "alpha");
		rs.next();

		OneToSet extracted = createRowMapper(OneToSet.class).mapRow(rs, 1);

		assertThat(extracted) //
				.isNotNull() //
				.extracting(e -> e.id, e -> e.name, e -> e.children.size()) //
				.containsExactly(23L, "alpha", 2);
	}

	private <T> EntityRowMapper<T> createRowMapper(Class<T> type) {

		JdbcMappingContext context = new JdbcMappingContext(new DefaultNamingStrategy());
		JdbcEntityOperations template = mock(JdbcEntityOperations.class);

		doReturn(new HashSet<>(asList(new Trivial(), new Trivial()))).when(template).findAllByProperty(eq(23L),
				any(JdbcPersistentProperty.class));

		return new EntityRowMapper<>((JdbcPersistentEntity<T>) context.getRequiredPersistentEntity(type),
				new DefaultConversionService(), context, template);
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

	private static class ResultSetAnswer implements Answer {

		private final List<Map<String, Object>> values;
		private int index = -1;

		public ResultSetAnswer(List<Map<String, Object>> values) {

			this.values = values;
		}

		@Override
		public Object answer(InvocationOnMock invocation) throws Throwable {

			switch (invocation.getMethod().getName()) {
				case "next":
			}

			if (invocation.getMethod().getName().equals("next"))
				return next();

			if (invocation.getMethod().getName().equals("getObject"))
				return getObject(invocation.getArgument(0));

			if (invocation.getMethod().getName().equals("isAfterLast"))
				return isAfterLast();

			if (invocation.getMethod().getName().equals("isBeforeFirst"))
				return isBeforeFirst();

			if (invocation.getMethod().getName().equals("getRow"))
				return isAfterLast() || isBeforeFirst() ? 0 : index + 1;

			if (invocation.getMethod().getName().equals("toString"))
				return this.toString();

			throw new OperationNotSupportedException(invocation.getMethod().getName());
		}

		private boolean isAfterLast() {
			return index >= values.size() && !values.isEmpty();
		}

		private boolean isBeforeFirst() {
			return index < 0 && !values.isEmpty();
		}

		private Object getObject(String column) {
			return values.get(index).get(column);
		}

		private boolean next() {

			index++;
			return index < values.size();
		}
	}

	@RequiredArgsConstructor
	static class Trivial {

		@Id Long id;
		String name;
	}

	@RequiredArgsConstructor
	static class OneToOne {

		@Id Long id;
		String name;
		Trivial child;
	}

	@RequiredArgsConstructor
	static class OneToSet {

		@Id Long id;
		String name;
		Set<Trivial> children;
	}

}
