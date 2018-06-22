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
package org.springframework.data.r2dbc.repository.query;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.r2dbc.function.DatabaseClient;
import org.springframework.data.r2dbc.function.DatabaseClient.GenericExecuteSpec;
import org.springframework.data.r2dbc.function.convert.MappingR2dbcConverter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.AbstractRepositoryMetadata;
import org.springframework.data.repository.query.ExtensionAwareQueryMethodEvaluationContextProvider;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.ReflectionUtils;

/**
 * Unit tests for {@link StringBasedR2dbcQuery}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class StringBasedR2dbcQueryUnitTests {

	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	@Mock private DatabaseClient databaseClient;
	@Mock private GenericExecuteSpec bindSpec;

	private RelationalMappingContext mappingContext;
	private MappingR2dbcConverter converter;
	private ProjectionFactory factory;
	private RepositoryMetadata metadata;

	@Before
	@SuppressWarnings("unchecked")
	public void setUp() {

		this.mappingContext = new RelationalMappingContext();
		this.converter = new MappingR2dbcConverter(this.mappingContext);
		this.metadata = AbstractRepositoryMetadata.getMetadata(SampleRepository.class);
		this.factory = new SpelAwareProxyProjectionFactory();

		when(bindSpec.bind(anyString(), any())).thenReturn(bindSpec);
	}

	@Test
	public void bindsSimplePropertyCorrectly() {

		StringBasedR2dbcQuery query = getQueryMethod("findByLastname", String.class);
		R2dbcParameterAccessor accessor = new R2dbcParameterAccessor(query.getQueryMethod(), "White");

		BindableQuery stringQuery = query.createQuery(accessor);

		assertThat(stringQuery.get()).isEqualTo("SELECT * FROM person WHERE lastname = $1");
		assertThat(stringQuery.bind(bindSpec)).isNotNull();

		verify(bindSpec).bind("$1", "White");
	}

	private StringBasedR2dbcQuery getQueryMethod(String name, Class<?>... args) {

		Method method = ReflectionUtils.findMethod(SampleRepository.class, name, args);

		R2dbcQueryMethod queryMethod = new R2dbcQueryMethod(method, metadata, factory, converter.getMappingContext());

		return new StringBasedR2dbcQuery(queryMethod, databaseClient, converter, PARSER,
				ExtensionAwareQueryMethodEvaluationContextProvider.DEFAULT);
	}

	@SuppressWarnings("unused")
	private interface SampleRepository extends Repository<Person, String> {

		@Query("SELECT * FROM person WHERE lastname = $1")
		Person findByLastname(String lastname);
	}

	static class Person {

	}
}
