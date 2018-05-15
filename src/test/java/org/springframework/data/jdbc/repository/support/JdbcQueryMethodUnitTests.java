/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.jdbc.repository.support;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.sql.ResultSet;

import org.junit.Test;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.jdbc.core.RowMapper;

/**
 * Unit tests for {@link JdbcQueryMethod}.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 */
public class JdbcQueryMethodUnitTests {

	public static final String DUMMY_SELECT = "SELECT something";

	@Test // DATAJDBC-165
	public void returnsSqlStatement() throws NoSuchMethodException {

		RepositoryMetadata metadata = mock(RepositoryMetadata.class);

		doReturn(String.class).when(metadata).getReturnedDomainClass(any(Method.class));

		JdbcQueryMethod queryMethod = new JdbcQueryMethod(JdbcQueryMethodUnitTests.class.getDeclaredMethod("queryMethod"),
				metadata, mock(ProjectionFactory.class));

		assertThat(queryMethod.getAnnotatedQuery()).isEqualTo(DUMMY_SELECT);
	}

	@Test // DATAJDBC-165
	public void returnsSpecifiedRowMapperClass() throws NoSuchMethodException {

		RepositoryMetadata metadata = mock(RepositoryMetadata.class);

		doReturn(String.class).when(metadata).getReturnedDomainClass(any(Method.class));

		JdbcQueryMethod queryMethod = new JdbcQueryMethod(JdbcQueryMethodUnitTests.class.getDeclaredMethod("queryMethod"),
				metadata, mock(ProjectionFactory.class));

		assertThat(queryMethod.getRowMapperClass()).isEqualTo(CustomRowMapper.class);
	}

	@Query(value = DUMMY_SELECT, rowMapperClass = CustomRowMapper.class)
	private void queryMethod() {}

	private class CustomRowMapper implements RowMapper<Object> {

		@Override
		public Object mapRow(ResultSet rs, int rowNum) {
			return null;
		}
	}
}
