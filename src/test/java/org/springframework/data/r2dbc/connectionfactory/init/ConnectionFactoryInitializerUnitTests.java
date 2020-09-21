/*
 * Copyright 2019-2020 the original author or authors.
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
package org.springframework.data.r2dbc.connectionfactory.init;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.spi.test.MockConnection;
import io.r2dbc.spi.test.MockConnectionFactory;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ConnectionFactoryInitializer}.
 *
 * @author Mark Paluch
 */
class ConnectionFactoryInitializerUnitTests {

	private final AtomicBoolean called = new AtomicBoolean();
	private final DatabasePopulator populator = mock(DatabasePopulator.class);
	private final MockConnection connection = MockConnection.builder().build();
	private final MockConnectionFactory connectionFactory = MockConnectionFactory.builder().connection(connection)
			.build();

	@Test // gh-216
	void shouldInitializeConnectionFactory() {

		when(populator.populate(any())).thenReturn(Mono.<Void> empty().doOnSubscribe(subscription -> called.set(true)));

		ConnectionFactoryInitializer initializer = new ConnectionFactoryInitializer();
		initializer.setConnectionFactory(connectionFactory);
		initializer.setDatabasePopulator(populator);

		initializer.afterPropertiesSet();

		assertThat(called).isTrue();
	}

	@Test // gh-216
	void shouldCleanConnectionFactory() {

		when(populator.populate(any())).thenReturn(Mono.<Void> empty().doOnSubscribe(subscription -> called.set(true)));

		ConnectionFactoryInitializer initializer = new ConnectionFactoryInitializer();
		initializer.setConnectionFactory(connectionFactory);
		initializer.setDatabaseCleaner(populator);

		initializer.afterPropertiesSet();
		initializer.destroy();

		assertThat(called).isTrue();
	}
}
