/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.r2dbc.repository.query;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.PreparedOperation;
import org.springframework.r2dbc.core.binding.BindTarget;

/**
 * Unit tests for {@link PreparedOperationBindableQuery}.
 *
 * @author Roman Chigvintsev
 * @author Marl Paluch
 */
@ExtendWith(MockitoExtension.class)
class PreparedOperationBindableQueryUnitTests {

	@Mock PreparedOperation<?> preparedOperation;

	@Test // gh-282, gh-587
	void bindsQueryParameterValues() {

		DatabaseClient.GenericExecuteSpec bindSpecMock = mock(DatabaseClient.GenericExecuteSpec.class, RETURNS_SELF);

		doAnswer(it -> {

			BindTarget target = it.getArgument(0);

			target.bind(0, "hello");
			target.bind("foo", "world");
			target.bindNull(1, String.class);
			target.bindNull("bar", Integer.class);

			return null;
		}).when(preparedOperation).bindTo(any());

		PreparedOperationBindableQuery query = new PreparedOperationBindableQuery(preparedOperation);
		DatabaseClient.GenericExecuteSpec bind = query.bind(bindSpecMock);

		verify(preparedOperation, times(1)).bindTo(any());
		verify(bindSpecMock).bind(0, "hello");
		verify(bindSpecMock).bind("foo", "world");
		verify(bindSpecMock).bindNull(1, String.class);
		verify(bindSpecMock).bindNull("bar", Integer.class);
	}

	@Test // gh-282
	void returnsSqlQuery() {

		when(preparedOperation.get()).thenReturn("SELECT * FROM test");

		PreparedOperationBindableQuery query = new PreparedOperationBindableQuery(preparedOperation);
		assertThat(query.get()).isEqualTo("SELECT * FROM test");
	}
}
