/*
 * Copyright 2020-2025 the original author or authors.
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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLType;
import java.sql.Types;
import java.util.Properties;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.jdbc.core.dialect.DefaultSqlTypeResolver;
import org.springframework.data.jdbc.core.dialect.SqlTypeResolver;
import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.data.relational.repository.Lock;
import org.springframework.data.jdbc.core.dialect.SqlType;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.PropertiesBasedNamedQueries;
import org.springframework.data.util.TypeInformation;
import org.springframework.jdbc.core.RowMapper;

/**
 * Unit tests for {@link JdbcQueryMethod}.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Moises Cisneros
 * @author Mark Paluch
 * @author Diego Krupitza
 * @author Mikhail Polivakha
 */
public class JdbcQueryMethodUnitTests {

	public static final String QUERY_NAME = "DUMMY.SELECT";
	public static final String QUERY = "SELECT something";
	public static final String METHOD_WITHOUT_QUERY_ANNOTATION = "methodWithImplicitlyNamedQuery";
	public static final String QUERY2 = "SELECT something NAME AND VALUE";

	JdbcMappingContext mappingContext = new JdbcMappingContext();
	NamedQueries namedQueries;
	RepositoryMetadata metadata;

	@BeforeEach
	public void before() {

		Properties properties = new Properties();
		properties.setProperty(QUERY_NAME, QUERY);
		// String is used as domain class because the methods used for testing aren't part of a repository and therefore the
		// return type is used as the domain type.
		properties.setProperty("String." + METHOD_WITHOUT_QUERY_ANNOTATION, QUERY2);
		namedQueries = new PropertiesBasedNamedQueries(properties);

		metadata = mock(RepositoryMetadata.class);
		when(metadata.getDomainTypeInformation()).then(invocationOnMock -> TypeInformation.of(Object.class));

		doReturn(String.class).when(metadata).getReturnedDomainClass(any(Method.class));
		doReturn(TypeInformation.of(String.class)).when(metadata).getReturnType(any(Method.class));
	}

	@Test // DATAJDBC-165
	public void returnsSqlStatement() throws NoSuchMethodException {

		JdbcQueryMethod queryMethod = createJdbcQueryMethod("queryMethod");

		assertThat(queryMethod.getDeclaredQuery()).isEqualTo(QUERY);
	}

	@Test // DATAJDBC-165
	public void testSqlTypeResolver() throws NoSuchMethodException {

		JdbcQueryMethod queryMethod = createJdbcQueryMethod(
				"findUserTestMethod",
				new DefaultSqlTypeResolver(),
				Integer.class, String.class, Integer[].class
		);

		JdbcParameters parameters = queryMethod.getParameters();

		SQLType first = parameters.getParameter(0).getSqlType();
		SQLType second = parameters.getParameter(1).getSqlType();
		SQLType thirdActual = parameters.getParameter(2).getActualSqlType();

		Assertions.assertThat(first.getName()).isEqualTo(JDBCType.TINYINT.getName());
		Assertions.assertThat(first.getVendorTypeNumber()).isEqualTo(Types.TINYINT);

		Assertions.assertThat(second.getName()).isEqualTo(JDBCType.VARCHAR.getName());
		Assertions.assertThat(second.getVendorTypeNumber()).isEqualTo(Types.VARCHAR);

		Assertions.assertThat(thirdActual.getName()).isEqualTo(JDBCType.SMALLINT.getName());
		Assertions.assertThat(thirdActual.getVendorTypeNumber()).isEqualTo(Types.SMALLINT);
	}

	@Test // DATAJDBC-165
	public void returnsSpecifiedRowMapperClass() throws NoSuchMethodException {

		JdbcQueryMethod queryMethod = createJdbcQueryMethod("queryMethod");

		assertThat(queryMethod.getRowMapperClass()).isEqualTo(CustomRowMapper.class);
	}

	@Test // DATAJDBC-234
	public void returnsSqlStatementName() throws NoSuchMethodException {

		JdbcQueryMethod queryMethod = createJdbcQueryMethod("queryMethodName");
		assertThat(queryMethod.getDeclaredQuery()).isEqualTo(QUERY);

	}

	@Test // DATAJDBC-234
	public void returnsSpecifiedSqlStatementIfNameAndValueAreGiven() throws NoSuchMethodException {

		JdbcQueryMethod queryMethod = createJdbcQueryMethod("queryMethodWithNameAndValue");
		assertThat(queryMethod.getDeclaredQuery()).isEqualTo(QUERY2);

	}

	@Test // DATAJDBC-234
	public void returnsImplicitlyNamedQuery() throws NoSuchMethodException {

		JdbcQueryMethod queryMethod = createJdbcQueryMethod("methodWithImplicitlyNamedQuery");
		assertThat(queryMethod.getDeclaredQuery()).isEqualTo(QUERY2);
	}

	@Test // DATAJDBC-234
	public void returnsNullIfNoQueryIsFound() throws NoSuchMethodException {

		JdbcQueryMethod queryMethod = createJdbcQueryMethod("methodWithoutAnyQuery");
		assertThat(queryMethod.getDeclaredQuery()).isEqualTo(null);
	}

	@Test // GH-1041
	void returnsQueryMethodWithCorrectLockTypeWriteLock() throws NoSuchMethodException {

		JdbcQueryMethod queryMethodWithWriteLock = createJdbcQueryMethod("queryMethodWithWriteLock");

		assertThat(queryMethodWithWriteLock.lookupLockAnnotation()).isPresent();
		assertThat(queryMethodWithWriteLock.lookupLockAnnotation().get().value()).isEqualTo(LockMode.PESSIMISTIC_WRITE);
	}

	@Test // GH-1041
	void returnsQueryMethodWithCorrectLockTypeReadLock() throws NoSuchMethodException {

		JdbcQueryMethod queryMethodWithReadLock = createJdbcQueryMethod("queryMethodWithReadLock");

		assertThat(queryMethodWithReadLock.lookupLockAnnotation()).isPresent();
		assertThat(queryMethodWithReadLock.lookupLockAnnotation().get().value()).isEqualTo(LockMode.PESSIMISTIC_READ);
	}

	@Test // GH-1041
	void returnsQueryMethodWithCorrectLockTypeNoLock() throws NoSuchMethodException {

		JdbcQueryMethod queryMethodWithWriteLock = createJdbcQueryMethod("queryMethodName");

		assertThat(queryMethodWithWriteLock.lookupLockAnnotation()).isEmpty();
	}

	private JdbcQueryMethod createJdbcQueryMethod(String methodName) throws NoSuchMethodException {
		return createJdbcQueryMethod(methodName, new DefaultSqlTypeResolver());
	}

	private JdbcQueryMethod createJdbcQueryMethod(String methodName, SqlTypeResolver sqlTypeResolver, Class<?>... args) throws NoSuchMethodException {

		Method method = JdbcQueryMethodUnitTests.class.getDeclaredMethod(methodName, args);
		return new JdbcQueryMethod(method, metadata, mock(ProjectionFactory.class), namedQueries, mappingContext, sqlTypeResolver);
	}

	@Lock(LockMode.PESSIMISTIC_WRITE)
	@Query
	private void queryMethodWithWriteLock() {}

	@Query
	private void findUserTestMethod(
			@SqlType(name = "TINYINT", vendorTypeNumber = Types.TINYINT) Integer age,
			String name,
			@SqlType(name = "SMALLINT", vendorTypeNumber = Types.SMALLINT) Integer[] statuses
	) {}

	@Lock(LockMode.PESSIMISTIC_READ)
	@Query
	private void queryMethodWithReadLock() {}

	@Query(value = QUERY, rowMapperClass = CustomRowMapper.class)
	private void queryMethod() {}

	@Query(name = QUERY_NAME)
	private void queryMethodName() {}

	@Query(value = QUERY2, name = QUERY_NAME)
	private void queryMethodWithNameAndValue() {}

	private void methodWithImplicitlyNamedQuery() {}

	private void methodWithoutAnyQuery() {}

	private class CustomRowMapper implements RowMapper<Object> {

		@Override
		public Object mapRow(ResultSet rs, int rowNum) {
			return null;
		}
	}
}
