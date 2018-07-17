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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.text.NumberFormat;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.jdbc.core.DataAccessStrategy;
import org.springframework.data.jdbc.repository.RowMapperMap;
import org.springframework.data.jdbc.repository.config.ConfigurableRowMapperMap;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.relational.core.conversion.BasicRelationalConverter;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * Unit tests for {@link JdbcQueryLookupStrategy}.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public class JdbcQueryLookupStrategyUnitTests {

	RelationalMappingContext mappingContext = mock(RelationalMappingContext.class, RETURNS_DEEP_STUBS);
	RelationalConverter converter = mock(BasicRelationalConverter.class);
	DataAccessStrategy accessStrategy = mock(DataAccessStrategy.class);
	ProjectionFactory projectionFactory = mock(ProjectionFactory.class);
	RepositoryMetadata metadata;
	NamedQueries namedQueries = mock(NamedQueries.class);
	NamedParameterJdbcOperations operations = mock(NamedParameterJdbcOperations.class);

	@Before
	public void setup() {

		this.metadata = mock(RepositoryMetadata.class);

		doReturn(NumberFormat.class).when(metadata).getReturnedDomainClass(any(Method.class));

	}

	@Test // DATAJDBC-166
	@SuppressWarnings("unchecked")
	public void typeBasedRowMapperGetsUsedForQuery() {

		RowMapper<? extends NumberFormat> numberFormatMapper = mock(RowMapper.class);
		RowMapperMap rowMapperMap = new ConfigurableRowMapperMap().register(NumberFormat.class, numberFormatMapper);

		RepositoryQuery repositoryQuery = getRepositoryQuery("returningNumberFormat", rowMapperMap);

		repositoryQuery.execute(new Object[] {});

		verify(operations).queryForObject(anyString(), any(SqlParameterSource.class), eq(numberFormatMapper));
	}

	private RepositoryQuery getRepositoryQuery(String name, RowMapperMap rowMapperMap) {

		JdbcQueryLookupStrategy queryLookupStrategy = new JdbcQueryLookupStrategy(mappingContext, converter, accessStrategy,
				rowMapperMap, operations);

		return queryLookupStrategy.resolveQuery(getMethod(name), metadata, projectionFactory, namedQueries);
	}

	// NumberFormat is just used as an arbitrary non simple type.
	@Query("some SQL")
	private NumberFormat returningNumberFormat() {
		return null;
	}

	private static Method getMethod(String name) {

		try {
			return JdbcQueryLookupStrategyUnitTests.class.getDeclaredMethod(name);
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
	}

}
