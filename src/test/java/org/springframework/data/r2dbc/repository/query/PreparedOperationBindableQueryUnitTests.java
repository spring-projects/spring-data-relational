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
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.data.r2dbc.core.PreparedOperation;

/**
 * @author Roman Chigvintsev
 */
@RunWith(MockitoJUnitRunner.class)
@Ignore
public class PreparedOperationBindableQueryUnitTests {
	@Mock private PreparedOperation<?> preparedOperation;

	@Test(expected = IllegalArgumentException.class)
	public void throwsExceptionWhenPreparedOperationIsNull() {
		new PreparedOperationBindableQuery(null);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void bindsQueryParameterValues() {
		DatabaseClient.BindSpec bindSpecMock = mock(DatabaseClient.BindSpec.class);

		PreparedOperationBindableQuery query = new PreparedOperationBindableQuery(preparedOperation);
		query.bind(bindSpecMock);
		verify(preparedOperation, times(1)).bindTo(any());
	}

	@Test
	public void returnsSqlQuery() {
		String sql = "SELECT * FROM test";
		when(preparedOperation.get()).thenReturn(sql);

		PreparedOperationBindableQuery query = new PreparedOperationBindableQuery(preparedOperation);
		assertThat(query.get()).isEqualTo(sql);
	}
}
