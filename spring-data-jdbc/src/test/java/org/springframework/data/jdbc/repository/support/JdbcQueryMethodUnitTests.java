/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.jdbc.repository.support;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.Properties;

import org.junit.Test;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.PropertiesBasedNamedQueries;
import org.springframework.jdbc.core.RowMapper;

/**
 * Unit tests for {@link JdbcQueryMethod}.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Moises Cisneros
 */
public class JdbcQueryMethodUnitTests {

	public static final String DUMMY_SELECT_VALUE = "SELECT something";
	public static final String DUMMY_SELECT_NAME = "DUMMY.SELECT";
	public static final String DUMMY_SELECT_METHOD = "queryWhitoutQueryAnnotation";
	public static final String DUMMY_SELECT_NAME_VALUE= "SELECT something NAME AND VALUE";

	@Test // DATAJDBC-165
	public void returnsSqlStatement() throws NoSuchMethodException {

		RepositoryMetadata metadata = mock(RepositoryMetadata.class);

		doReturn(String.class).when(metadata).getReturnedDomainClass(any(Method.class));
		Properties properties = new Properties();
		properties.setProperty(DUMMY_SELECT_NAME, DUMMY_SELECT_VALUE);
		NamedQueries nameQueries = new PropertiesBasedNamedQueries(properties);
		JdbcQueryMethod queryMethod = new JdbcQueryMethod(
				JdbcQueryMethodUnitTests.class.getDeclaredMethod("queryMethod"), metadata,
				mock(ProjectionFactory.class), nameQueries);
		assertThat(queryMethod.getAnnotatedQuery()).isEqualTo(DUMMY_SELECT_VALUE);
	}

	@Test // DATAJDBC-165
	public void returnsSpecifiedRowMapperClass() throws NoSuchMethodException {

		RepositoryMetadata metadata = mock(RepositoryMetadata.class);

		doReturn(String.class).when(metadata).getReturnedDomainClass(any(Method.class));
		Properties properties = new Properties();
		properties.setProperty(DUMMY_SELECT_NAME, DUMMY_SELECT_VALUE);
		NamedQueries nameQueries = new PropertiesBasedNamedQueries(properties);

		JdbcQueryMethod queryMethod = new JdbcQueryMethod(
				JdbcQueryMethodUnitTests.class.getDeclaredMethod("queryMethod"), metadata,
				mock(ProjectionFactory.class), nameQueries);

		assertThat(queryMethod.getRowMapperClass()).isEqualTo(CustomRowMapper.class);
	}









	@Test // DATAJDBC-234
	public void returnsSqlStatementName() throws NoSuchMethodException {

		RepositoryMetadata metadata = mock(RepositoryMetadata.class);

		doReturn(String.class).when(metadata).getReturnedDomainClass(any(Method.class));

		Properties properties = new Properties();
		properties.setProperty(DUMMY_SELECT_NAME, DUMMY_SELECT_VALUE);
		NamedQueries nameQueries = new PropertiesBasedNamedQueries(properties);

		JdbcQueryMethod queryMethod = new JdbcQueryMethod(
				JdbcQueryMethodUnitTests.class.getDeclaredMethod("queryMethodName"), metadata,
				mock(ProjectionFactory.class), nameQueries);
		assertThat(queryMethod.getAnnotatedQuery()).isEqualTo(DUMMY_SELECT_VALUE);

	}
	@Test // DATAJDBC-234
	public void returnsSqlStatementNameAndValue() throws NoSuchMethodException {

		RepositoryMetadata metadata = mock(RepositoryMetadata.class);

		doReturn(String.class).when(metadata).getReturnedDomainClass(any(Method.class));

		Properties properties = new Properties();
		properties.setProperty(DUMMY_SELECT_NAME, DUMMY_SELECT_VALUE);
		NamedQueries nameQueries = new PropertiesBasedNamedQueries(properties);

		JdbcQueryMethod queryMethod = new JdbcQueryMethod(
				JdbcQueryMethodUnitTests.class.getDeclaredMethod("queryMethodNameAndValue"), metadata,
				mock(ProjectionFactory.class), nameQueries);
		assertThat(queryMethod.getAnnotatedQuery()).isEqualTo(DUMMY_SELECT_NAME_VALUE);

	}

	@Test // DATAJDBC-234
	public void returnsNullNoSqlQuery() throws NoSuchMethodException {

		RepositoryMetadata metadata = mock(RepositoryMetadata.class);
		Properties properties = new Properties();
		properties.setProperty(DUMMY_SELECT_METHOD, DUMMY_SELECT_VALUE);
		NamedQueries nameQueries = new PropertiesBasedNamedQueries(properties);

		doReturn(String.class).when(metadata).getReturnedDomainClass(any(Method.class));

		JdbcQueryMethod queryMethod = new JdbcQueryMethod(
				JdbcQueryMethodUnitTests.class.getDeclaredMethod("queryWhitoutQueryAnnotation"), metadata,
				mock(ProjectionFactory.class), nameQueries);
		assertThat(queryMethod.getAnnotatedQuery()).isEqualTo(DUMMY_SELECT_VALUE);

	}

	@Query(value = DUMMY_SELECT_VALUE, rowMapperClass = CustomRowMapper.class)
	private void queryMethod() {
	}

	@Query(name = DUMMY_SELECT_NAME)
	private void queryMethodName() {
	}

	@Query(value = DUMMY_SELECT_NAME_VALUE, name = DUMMY_SELECT_NAME)
	private void queryMethodNameAndValue() {
	}

	private void queryWhitoutQueryAnnotation() {
	}

	private class CustomRowMapper implements RowMapper<Object> {

		@Override
		public Object mapRow(ResultSet rs, int rowNum) {
			return null;
		}
	}
}
