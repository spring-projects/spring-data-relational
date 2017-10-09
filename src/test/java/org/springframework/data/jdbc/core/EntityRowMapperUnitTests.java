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
import java.util.AbstractMap.SimpleEntry;
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
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.Jsr310Converters;
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

	public static final long ID_FOR_ENTITY_REFERENCING_MAP = 42L;
	public static final long ID_FOR_ENTITY_NOT_REFERENCING_MAP = 23L;

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

	private <T> EntityRowMapper<T> createRowMapper(Class<T> type) {

		JdbcMappingContext context = new JdbcMappingContext(new DefaultNamingStrategy());
		DataAccessStrategy accessStrategy = mock(DataAccessStrategy.class);

		// the ID of the entity is used to determin what kind of resultset is needed for subsequent selects.
		doReturn(new HashSet<>(asList(new Trivial(), new Trivial()))).when(accessStrategy).findAllByProperty(eq(ID_FOR_ENTITY_NOT_REFERENCING_MAP),
				any(JdbcPersistentProperty.class));

		doReturn(new HashSet<>(asList( //
				new SimpleEntry("one", new Trivial()), //
				new SimpleEntry("two", new Trivial()) //
		))).when(accessStrategy).findAllByProperty(eq(ID_FOR_ENTITY_REFERENCING_MAP), any(JdbcPersistentProperty.class));

		GenericConversionService conversionService = new GenericConversionService();
		conversionService.addConverter(new IterableOfEntryToMapConverter());
		DefaultConversionService.addDefaultConverters(conversionService);
		Jsr310Converters.getConvertersToRegister().forEach(conversionService::addConverter);

		return new EntityRowMapper<>((JdbcPersistentEntity<T>) context.getRequiredPersistentEntity(type),
				conversionService, context, accessStrategy);
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

	@RequiredArgsConstructor
	static class OneToMap {

		@Id Long id;
		String name;
		Map<String, Trivial> children;
	}
}
