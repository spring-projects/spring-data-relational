/*
 * Copyright 2018-2024 the original author or authors.
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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.text.NumberFormat;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.repository.QueryMappingConfiguration;
import org.springframework.data.jdbc.repository.config.DefaultQueryMappingConfiguration;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.relational.core.dialect.H2Dialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.data.util.TypeInformation;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.util.ReflectionUtils;

/**
 * Unit tests for {@link JdbcQueryLookupStrategy}.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Maciej Walkowiak
 * @author Evgeni Dimitrov
 * @author Mark Paluch
 * @author Hebert Coelho
 * @author Diego Krupitza
 * @author Christopher Klein
 */
class JdbcQueryLookupStrategyUnitTests {

	private ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);
	private EntityCallbacks callbacks = mock(EntityCallbacks.class);
	private RelationalMappingContext mappingContext = mock(RelationalMappingContext.class, RETURNS_DEEP_STUBS);
	private JdbcConverter converter = mock(JdbcConverter.class);
	private ProjectionFactory projectionFactory = mock(ProjectionFactory.class);
	private RepositoryMetadata metadata;
	private NamedQueries namedQueries = mock(NamedQueries.class);
	private NamedParameterJdbcOperations operations = mock(NamedParameterJdbcOperations.class);
	QueryMethodEvaluationContextProvider evaluationContextProvider = mock(QueryMethodEvaluationContextProvider.class);

	@BeforeEach
	void setup() {

		this.metadata = mock(RepositoryMetadata.class);

		doReturn(NumberFormat.class).when(metadata).getReturnedDomainClass(any(Method.class));
		doReturn(TypeInformation.of(NumberFormat.class)).when(metadata).getReturnType(any(Method.class));
		doReturn(TypeInformation.of(NumberFormat.class)).when(metadata).getDomainTypeInformation();
	}

	@Test // DATAJDBC-166
	@SuppressWarnings("unchecked")
	void typeBasedRowMapperGetsUsedForQuery() {

		RowMapper<? extends NumberFormat> numberFormatMapper = mock(RowMapper.class);
		QueryMappingConfiguration mappingConfiguration = new DefaultQueryMappingConfiguration()
				.registerRowMapper(NumberFormat.class, numberFormatMapper);

		RepositoryQuery repositoryQuery = getRepositoryQuery(QueryLookupStrategy.Key.CREATE_IF_NOT_FOUND,
				"returningNumberFormat", mappingConfiguration);

		repositoryQuery.execute(new Object[] {});

		verify(operations).queryForObject(anyString(), any(SqlParameterSource.class), any(RowMapper.class));
	}

	@Test // GH-1061
	void prefersDeclaredQuery() {

		RowMapper<? extends NumberFormat> numberFormatMapper = mock(RowMapper.class);
		QueryMappingConfiguration mappingConfiguration = new DefaultQueryMappingConfiguration()
				.registerRowMapper(NumberFormat.class, numberFormatMapper);

		RepositoryQuery repositoryQuery = getRepositoryQuery(QueryLookupStrategy.Key.CREATE_IF_NOT_FOUND,
				"annotatedQueryWithQueryAndQueryName", mappingConfiguration);

		repositoryQuery.execute(new Object[] {});

		verify(operations).queryForObject(eq("some SQL"), any(SqlParameterSource.class), any(RowMapper.class));
	}

	@Test // GH-1043
	void shouldFailOnMissingDeclaredQuery() {

		RowMapper<? extends NumberFormat> numberFormatMapper = mock(RowMapper.class);
		QueryMappingConfiguration mappingConfiguration = new DefaultQueryMappingConfiguration()
				.registerRowMapper(NumberFormat.class, numberFormatMapper);

		assertThatThrownBy(
				() -> getRepositoryQuery(QueryLookupStrategy.Key.USE_DECLARED_QUERY, "findByName", mappingConfiguration))
						.isInstanceOf(IllegalStateException.class)
						.hasMessageContaining("Did neither find a NamedQuery nor an annotated query for method")
						.hasMessageContaining("findByName");
	}

	@ParameterizedTest
	@MethodSource("correctLookUpStrategyForKeySource")
	void correctLookUpStrategyForKey(QueryLookupStrategy.Key key, Class expectedClass) {

		RowMapper<? extends NumberFormat> numberFormatMapper = mock(RowMapper.class);
		QueryMappingConfiguration mappingConfiguration = new DefaultQueryMappingConfiguration()
				.registerRowMapper(NumberFormat.class, numberFormatMapper);

		QueryLookupStrategy queryLookupStrategy = JdbcQueryLookupStrategy.create(key, publisher, callbacks, mappingContext,
				converter, H2Dialect.INSTANCE, mappingConfiguration, operations, null, ValueExpressionDelegate.create());

		assertThat(queryLookupStrategy).isInstanceOf(expectedClass);
	}

	private static Stream<Arguments> correctLookUpStrategyForKeySource() {

		return Stream.of( //
				Arguments.of(QueryLookupStrategy.Key.CREATE_IF_NOT_FOUND,
						JdbcQueryLookupStrategy.CreateIfNotFoundQueryLookupStrategy.class), //
				Arguments.of(QueryLookupStrategy.Key.CREATE, JdbcQueryLookupStrategy.CreateQueryLookupStrategy.class), //
				Arguments.of(QueryLookupStrategy.Key.USE_DECLARED_QUERY,
						JdbcQueryLookupStrategy.DeclaredQueryLookupStrategy.class) //
		);
	}

	private RepositoryQuery getRepositoryQuery(QueryLookupStrategy.Key key, String name,
			QueryMappingConfiguration mappingConfiguration) {

		QueryLookupStrategy queryLookupStrategy = JdbcQueryLookupStrategy.create(key, publisher, callbacks, mappingContext,
				converter, H2Dialect.INSTANCE, mappingConfiguration, operations, null, ValueExpressionDelegate.create());

		Method method = ReflectionUtils.findMethod(MyRepository.class, name);
		return queryLookupStrategy.resolveQuery(method, metadata, projectionFactory, namedQueries);
	}

	interface MyRepository {

		// NumberFormat is just used as an arbitrary non simple type.
		@Query("some SQL")
		NumberFormat returningNumberFormat();

		@Query(value = "some SQL", name = "query-name")
		void annotatedQueryWithQueryAndQueryName();

		NumberFormat findByName();
	}
}
