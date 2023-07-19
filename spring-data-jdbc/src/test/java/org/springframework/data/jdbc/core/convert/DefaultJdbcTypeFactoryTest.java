/*
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.sql.Array;
import java.sql.SQLException;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.postgresql.core.BaseConnection;
import org.springframework.data.jdbc.core.dialect.JdbcPostgresDialect;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcOperations;

/**
 * Unit tests for {@link DefaultJdbcTypeFactory}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
class DefaultJdbcTypeFactoryTest {

	@Mock JdbcOperations operations;
	@Mock BaseConnection connection;

	@Test // GH-1567
	void shouldProvidePostgresArrayType() throws SQLException {

		DefaultJdbcTypeFactory sut = new DefaultJdbcTypeFactory(operations, JdbcPostgresDialect.INSTANCE.getArraySupport());

		when(operations.execute(any(ConnectionCallback.class))).thenAnswer(invocation -> {

			ConnectionCallback callback = invocation.getArgument(0, ConnectionCallback.class);
			return callback.doInConnection(connection);
		});

		UUID uuids[] = new UUID[] { UUID.randomUUID(), UUID.randomUUID() };
		when(connection.createArrayOf("uuid", uuids)).thenReturn(mock(Array.class));
		Array array = sut.createArray(uuids);

		assertThat(array).isNotNull();
	}

}
