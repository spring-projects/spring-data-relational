/*
 * Copyright 2017-2018 the original author or authors.
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

import java.util.HashMap;

import lombok.RequiredArgsConstructor;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.mapping.model.DefaultNamingStrategy;
import org.springframework.data.jdbc.mapping.model.JdbcMappingContext;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.KeyHolder;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author Jens Schauder
 */
public class DefaultDataAccessStrategyUnitTests {

	public static final long ID_FROM_ADDITIONAL_VALUES = 23L;
	public static final long ORIGINAL_ID = 4711L;

	JdbcMappingContext context = new JdbcMappingContext(new DefaultNamingStrategy(), __ -> {});
	NamedParameterJdbcOperations jdbcOperations = mock(NamedParameterJdbcOperations.class);
	HashMap<String, Object> additionalParameters = new HashMap<>();
	ArgumentCaptor<SqlParameterSource> captor = ArgumentCaptor.forClass(SqlParameterSource.class);

	DefaultDataAccessStrategy accessStrategy = new DefaultDataAccessStrategy( //
			new SqlGeneratorSource(context), //
			jdbcOperations, //
			context //
	);

	@Test // DATAJDBC-146
	public void additionalParameterForIdDoesNotLeadToDuplicateParameters() {

		additionalParameters.put("id", ID_FROM_ADDITIONAL_VALUES);

		accessStrategy.insert(new DummyEntity(ORIGINAL_ID), DummyEntity.class, additionalParameters);

		verify(jdbcOperations).update(eq("insert into DummyEntity (id) values (:id)"), captor.capture(),
				any(KeyHolder.class));
		assertThat(captor.getValue().getValue("id")).isEqualTo(ID_FROM_ADDITIONAL_VALUES);
	}

	@Test // DATAJDBC-146
	public void additionalParametersGetAddedToStatement() {

		additionalParameters.put("reference", ID_FROM_ADDITIONAL_VALUES);

		accessStrategy.insert(new DummyEntity(ORIGINAL_ID), DummyEntity.class, additionalParameters);

		verify(jdbcOperations).update(eq("insert into DummyEntity (id, reference) values (:id, :reference)"),
				captor.capture(), any(KeyHolder.class));
		assertThat(captor.getValue().getValue("id")).isEqualTo(ORIGINAL_ID);
	}

	@RequiredArgsConstructor
	private static class DummyEntity {

		@Id private final Long id;
	}

}
