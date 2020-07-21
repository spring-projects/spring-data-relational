/*
 * Copyright 2020 the original author or authors.
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

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.PreparedOperation;

/**
 * Unit tests for {@link PreparedOperationBindableQuery}.
 *
 * @author Roman Chigvintsev
 * @author Marl Paluch
 */
@RunWith(MockitoJUnitRunner.class)
@Ignore
public class PreparedOperationBindableQueryUnitTests {

	@Mock PreparedOperation<?> preparedOperation;

	@Test // gh-282
	public void bindsQueryParameterValues() {

		DatabaseClient.GenericExecuteSpec bindSpecMock = mock(DatabaseClient.GenericExecuteSpec.class);

		PreparedOperationBindableQuery query = new PreparedOperationBindableQuery(preparedOperation);
		query.bind(bindSpecMock);
		verify(preparedOperation, times(1)).bindTo(any());
	}

	@Test // gh-282
	public void returnsSqlQuery() {

		when(preparedOperation.get()).thenReturn("SELECT * FROM test");

		PreparedOperationBindableQuery query = new PreparedOperationBindableQuery(preparedOperation);
		assertThat(query.get()).isEqualTo("SELECT * FROM test");
	}
}
