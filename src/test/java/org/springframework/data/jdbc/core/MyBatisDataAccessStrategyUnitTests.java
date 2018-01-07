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

import java.util.Collections;

import javax.sql.DataSource;

import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mybatis.spring.transaction.SpringManagedTransactionFactory;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentProperty;
import org.springframework.data.jdbc.mybatis.MyBatisContext;
import org.springframework.data.jdbc.mybatis.MyBatisDataAccessStrategy;
import org.springframework.data.mapping.PropertyPath;

/**
 * Unit tests for the {@link MyBatisDataAccessStrategy}, mainly ensuring that the correct statements get's looked up.
 *
 * @author Jens Schauder
 * @author Kazuki Shimizu
 */
public class MyBatisDataAccessStrategyUnitTests {

	DataSource dataSource = mock(DataSource.class);
	SqlSessionFactory sessionFactory = mock(SqlSessionFactory.class);
	SqlSession session = mock(SqlSession.class);
	ArgumentCaptor<MyBatisContext> captor = ArgumentCaptor.forClass(MyBatisContext.class);

	MyBatisDataAccessStrategy accessStrategy;

	@Before
	public void before() {

		Configuration configuration = new Configuration();
		Environment environment = new Environment("unittest", new SpringManagedTransactionFactory(), dataSource);
		configuration.setEnvironment(environment);
		doReturn(configuration).when(sessionFactory).getConfiguration();
		doReturn(session).when(sessionFactory).openSession(configuration.getDefaultExecutorType());
		doReturn(false).when(session).selectOne(any(), any());
		this.accessStrategy = new MyBatisDataAccessStrategy(sessionFactory);
	}

	@Test // DATAJDBC-123
	public void insert() {

		accessStrategy.insert("x", String.class, Collections.singletonMap("key", "value"));

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

		accessStrategy.deleteAll(PropertyPath.from("class.name.bytes", String.class));

		verify(session).delete(eq("java.lang.StringMapper.deleteAll.class.name.bytes"), captor.capture());

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
						byte[].class, //
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

		accessStrategy.delete("rootid", PropertyPath.from("class.name.bytes", String.class));

		verify(session).delete(eq("java.lang.StringMapper.delete.class.name.bytes"), captor.capture());

		assertThat(captor.getValue()) //
				.isNotNull() //
				.extracting( //
						MyBatisContext::getInstance, //
						MyBatisContext::getId, //
						MyBatisContext::getDomainType, //
						c -> c.get("key") //
				).containsExactly( //
						null, "rootid", //
						byte[].class, //
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

	@Test // DATAJDBC-123
	public void findAllByProperty() {

		JdbcPersistentProperty property = mock(JdbcPersistentProperty.class, Mockito.RETURNS_DEEP_STUBS);

		when(property.getOwner().getType()).thenReturn((Class) String.class);
		doReturn(Number.class).when(property).getType();
		doReturn("propertyName").when(property).getName();

		accessStrategy.findAllByProperty("id", property);

		verify(session).selectList(eq("java.lang.StringMapper.findAllByProperty.propertyName"), captor.capture());

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

}
