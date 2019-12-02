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
package org.springframework.data.jdbc.mybatis;

import static java.util.Arrays.*;
import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.relational.domain.SqlIdentifier.*;

import java.util.Collections;

import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.data.jdbc.core.PropertyPathTestingUtils;
import org.springframework.data.jdbc.core.convert.BasicJdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.Identifier;
import org.springframework.data.relational.domain.IdentifierProcessing;

/**
 * Unit tests for the {@link MyBatisDataAccessStrategy}, mainly ensuring that the correct statements get's looked up.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Tyler Van Gorder
 */
public class MyBatisDataAccessStrategyUnitTests {

	RelationalMappingContext context = new JdbcMappingContext();
	JdbcConverter converter = new BasicJdbcConverter(context, (Identifier, path) -> null);

	SqlSession session = mock(SqlSession.class);
	ArgumentCaptor<MyBatisContext> captor = ArgumentCaptor.forClass(MyBatisContext.class);

	MyBatisDataAccessStrategy accessStrategy = new MyBatisDataAccessStrategy(session, IdentifierProcessing.ANSI);

	PersistentPropertyPath<RelationalPersistentProperty> path = PropertyPathTestingUtils.toPath("one.two",
			DummyEntity.class, context);

	@Before
	public void before() {

		doReturn(false).when(session).selectOne(any(), any());
	}

	@Test // DATAJDBC-123
	public void insert() {

		accessStrategy.insert("x", String.class, Collections.singletonMap(unquoted("key"), "value"));

		verify(session).insert(eq("java.lang.StringMapper.insert"), captor.capture());

		assertThat(captor.getValue()) //
				.isNotNull() //
				.extracting( //
						MyBatisContext::getInstance, //
						MyBatisContext::getId, //
						MyBatisContext::getDomainType, //
						c -> c.get("key") //
				).containsExactly( //
						"x", //
						null, //
						String.class, //
						"value" //
				);
	}

	@Test // DATAJDBC-123
	public void update() {

		accessStrategy.update("x", String.class);

		verify(session).update(eq("java.lang.StringMapper.update"), captor.capture());

		assertThat(captor.getValue()) //
				.isNotNull() //
				.extracting( //
						MyBatisContext::getInstance, //
						MyBatisContext::getId, //
						MyBatisContext::getDomainType, //
						c -> c.get("key") //
				).containsExactly( //
						"x", //
						null, //
						String.class, //
						null //
				);
	}

	@Test // DATAJDBC-123
	public void delete() {

		accessStrategy.delete("an-id", String.class);

		verify(session).delete(eq("java.lang.StringMapper.delete"), captor.capture());

		assertThat(captor.getValue()) //
				.isNotNull() //
				.extracting( //
						MyBatisContext::getInstance, //
						MyBatisContext::getId, //
						MyBatisContext::getDomainType, //
						c -> c.get("key") //
				).containsExactly( //
						null, //
						"an-id", //
						String.class, //
						null //
				);
	}

	@Test // DATAJDBC-123
	public void deleteAllByPath() {

		accessStrategy.deleteAll(path);

		verify(session).delete(eq(
				"org.springframework.data.jdbc.mybatis.MyBatisDataAccessStrategyUnitTests$DummyEntityMapper.deleteAll-one-two"),
				captor.capture());

		assertThat(captor.getValue()) //
				.isNotNull() //
				.extracting( //
						MyBatisContext::getInstance, //
						MyBatisContext::getId, //
						MyBatisContext::getDomainType, //
						c -> c.get("key") //
				).containsExactly( //
						null, //
						null, //
						ChildTwo.class, //
						null //
				);
	}

	@Test // DATAJDBC-123
	public void deleteAllByType() {

		accessStrategy.deleteAll(String.class);

		verify(session).delete(eq("java.lang.StringMapper.deleteAll"), captor.capture());

		assertThat(captor.getValue()) //
				.isNotNull() //
				.extracting( //
						MyBatisContext::getInstance, //
						MyBatisContext::getId, //
						MyBatisContext::getDomainType, //
						c -> c.get("key") //
				).containsExactly( //
						null, //
						null, //
						String.class, //
						null //
				);
	}

	@Test // DATAJDBC-123
	public void deleteByPath() {

		accessStrategy.delete("rootid", path);

		verify(session).delete(
				eq("org.springframework.data.jdbc.mybatis.MyBatisDataAccessStrategyUnitTests$DummyEntityMapper.delete-one-two"),
				captor.capture());

		assertThat(captor.getValue()) //
				.isNotNull() //
				.extracting( //
						MyBatisContext::getInstance, //
						MyBatisContext::getId, //
						MyBatisContext::getDomainType, //
						c -> c.get("key") //
				).containsExactly( //
						null, "rootid", //
						ChildTwo.class, //
						null //
				);
	}

	@Test // DATAJDBC-123
	public void findById() {

		accessStrategy.findById("an-id", String.class);

		verify(session).selectOne(eq("java.lang.StringMapper.findById"), captor.capture());

		assertThat(captor.getValue()) //
				.isNotNull() //
				.extracting( //
						MyBatisContext::getInstance, //
						MyBatisContext::getId, //
						MyBatisContext::getDomainType, //
						c -> c.get("key") //
				).containsExactly( //
						null, "an-id", //
						String.class, //
						null //
				);
	}

	@Test // DATAJDBC-123
	public void findAll() {

		accessStrategy.findAll(String.class);

		verify(session).selectList(eq("java.lang.StringMapper.findAll"), captor.capture());

		assertThat(captor.getValue()) //
				.isNotNull() //
				.extracting( //
						MyBatisContext::getInstance, //
						MyBatisContext::getId, //
						MyBatisContext::getDomainType, //
						c -> c.get("key") //
				).containsExactly( //
						null, //
						null, //
						String.class, //
						null //
				);
	}

	@Test // DATAJDBC-123
	public void findAllById() {

		accessStrategy.findAllById(asList("id1", "id2"), String.class);

		verify(session).selectList(eq("java.lang.StringMapper.findAllById"), captor.capture());

		assertThat(captor.getValue()) //
				.isNotNull() //
				.extracting( //
						MyBatisContext::getInstance, //
						MyBatisContext::getId, //
						MyBatisContext::getDomainType, //
						c -> c.get("key") //
				).containsExactly( //
						null, //
						asList("id1", "id2"), //
						String.class, //
						null //
				);
	}

	@SuppressWarnings("unchecked")
	@Test // DATAJDBC-123
	public void findAllByProperty() {

		RelationalPersistentProperty property = mock(RelationalPersistentProperty.class, Mockito.RETURNS_DEEP_STUBS);

		when(property.getOwner().getType()).thenReturn((Class) String.class);
		doReturn(Number.class).when(property).getType();
		doReturn("propertyName").when(property).getName();

		accessStrategy.findAllByProperty("id", property);

		verify(session).selectList(eq("java.lang.StringMapper.findAllByProperty-propertyName"), captor.capture());

		assertThat(captor.getValue()) //
				.isNotNull() //
				.extracting( //
						MyBatisContext::getInstance, //
						MyBatisContext::getId, //
						MyBatisContext::getDomainType, //
						c -> c.get("key") //
				).containsExactly( //
						null, //
						"id", //
						Number.class, //
						null //
				);
	}

	@SuppressWarnings("unchecked")
	@Test // DATAJDBC-384
	public void findAllByPath() {

		RelationalPersistentProperty property = mock(RelationalPersistentProperty.class, RETURNS_DEEP_STUBS);
		PersistentPropertyPath path = mock(PersistentPropertyPath.class, RETURNS_DEEP_STUBS);

		when(path.getBaseProperty()).thenReturn(property);
		when(property.getOwner().getType()).thenReturn((Class) String.class);

		when(path.getRequiredLeafProperty()).thenReturn(property);
		when(property.getType()).thenReturn((Class) Number.class);

		when(path.toDotPath()).thenReturn("dot.path");

		accessStrategy.findAllByPath(Identifier.empty(), path);

		verify(session).selectList(eq("java.lang.StringMapper.findAllByPath-dot.path"), captor.capture());

		assertThat(captor.getValue()) //
				.isNotNull() //
				.extracting( //
						MyBatisContext::getInstance, //
						MyBatisContext::getId, //
						MyBatisContext::getIdentifier, //
						MyBatisContext::getDomainType, //
						c -> c.get("key") //
				).containsExactly( //
						null, //
						null, //
						Identifier.empty(), //
						Number.class, //
						null //
				);
	}

	@SuppressWarnings("unchecked")
	@Test // DATAJDBC-384
	public void findAllByPathFallsBackToFindAllByProperty() {

		RelationalPersistentProperty property = mock(RelationalPersistentProperty.class, RETURNS_DEEP_STUBS);
		PersistentPropertyPath path = mock(PersistentPropertyPath.class, RETURNS_DEEP_STUBS);

		when(path.getBaseProperty()).thenReturn(property);
		when(property.getOwner().getType()).thenReturn((Class) String.class);

		when(path.getRequiredLeafProperty()).thenReturn(property);
		when(property.getType()).thenReturn((Class) Number.class);

		when(path.toDotPath()).thenReturn("dot.path");

		when(session.selectList(any(), any())).thenThrow(PersistenceException.class).thenReturn(emptyList());

		accessStrategy.findAllByPath(Identifier.empty(), path);

		verify(session, times(2)).selectList(any(), any());

	}

	@Test // DATAJDBC-123
	public void existsById() {

		accessStrategy.existsById("id", String.class);

		verify(session).selectOne(eq("java.lang.StringMapper.existsById"), captor.capture());

		assertThat(captor.getValue()) //
				.isNotNull() //
				.extracting( //
						MyBatisContext::getInstance, //
						MyBatisContext::getId, //
						MyBatisContext::getDomainType, //
						c -> c.get("key") //
				).containsExactly( //
						null, //
						"id", //
						String.class, //
						null //
				);
	}

	@Test // DATAJDBC-157
	public void count() {

		doReturn(0L).when(session).selectOne(anyString(), any());

		accessStrategy.count(String.class);

		verify(session).selectOne(eq("java.lang.StringMapper.count"), captor.capture());

		assertThat(captor.getValue()) //
				.isNotNull() //
				.extracting( //
						MyBatisContext::getInstance, //
						MyBatisContext::getId, //
						MyBatisContext::getDomainType, //
						c -> c.get("key") //
				).containsExactly( //
						null, //
						null, //
						String.class, //
						null //
				);
	}

	@SuppressWarnings("unused")
	private static class DummyEntity {
		ChildOne one;
	}

	@SuppressWarnings("unused")
	private static class ChildOne {
		ChildTwo two;
	}

	private static class ChildTwo {}
}
