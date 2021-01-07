/*
 * Copyright 2019-2021 the original author or authors.
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

import static org.mockito.Mockito.*;

import io.r2dbc.spi.Connection;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CompositeDatabasePopulator}.
 *
 * @author Mark Paluch
 */
class CompositeDatabasePopulatorTests {

	private final Connection mockedConnection = mock(Connection.class);

	private final DatabasePopulator mockedDatabasePopulator1 = mock(DatabasePopulator.class);

	private final DatabasePopulator mockedDatabasePopulator2 = mock(DatabasePopulator.class);

	@BeforeEach
	void before() {

		when(mockedDatabasePopulator1.populate(mockedConnection)).thenReturn(Mono.empty());
		when(mockedDatabasePopulator2.populate(mockedConnection)).thenReturn(Mono.empty());
	}

	@Test
	void addPopulators() {

		CompositeDatabasePopulator populator = new CompositeDatabasePopulator();
		populator.addPopulators(mockedDatabasePopulator1, mockedDatabasePopulator2);

		populator.populate(mockedConnection).as(StepVerifier::create).verifyComplete();

		verify(mockedDatabasePopulator1, times(1)).populate(mockedConnection);
		verify(mockedDatabasePopulator2, times(1)).populate(mockedConnection);
	}

	@Test
	void setPopulatorsWithMultiple() {

		CompositeDatabasePopulator populator = new CompositeDatabasePopulator();
		populator.setPopulators(mockedDatabasePopulator1, mockedDatabasePopulator2); // multiple

		populator.populate(mockedConnection).as(StepVerifier::create).verifyComplete();

		verify(mockedDatabasePopulator1, times(1)).populate(mockedConnection);
		verify(mockedDatabasePopulator2, times(1)).populate(mockedConnection);
	}

	@Test
	void setPopulatorsForOverride() {

		CompositeDatabasePopulator populator = new CompositeDatabasePopulator();
		populator.setPopulators(mockedDatabasePopulator1);
		populator.setPopulators(mockedDatabasePopulator2); // override

		populator.populate(mockedConnection).as(StepVerifier::create).verifyComplete();

		verify(mockedDatabasePopulator1, times(0)).populate(mockedConnection);
		verify(mockedDatabasePopulator2, times(1)).populate(mockedConnection);
	}

	@Test
	void constructWithVarargs() {

		CompositeDatabasePopulator populator = new CompositeDatabasePopulator(mockedDatabasePopulator1,
				mockedDatabasePopulator2);

		populator.populate(mockedConnection).as(StepVerifier::create).verifyComplete();

		verify(mockedDatabasePopulator1, times(1)).populate(mockedConnection);
		verify(mockedDatabasePopulator2, times(1)).populate(mockedConnection);
	}

	@Test
	void constructWithCollection() {

		Set<DatabasePopulator> populators = new LinkedHashSet<>();
		populators.add(mockedDatabasePopulator1);
		populators.add(mockedDatabasePopulator2);

		CompositeDatabasePopulator populator = new CompositeDatabasePopulator(populators);
		populator.populate(mockedConnection).as(StepVerifier::create).verifyComplete();

		verify(mockedDatabasePopulator1, times(1)).populate(mockedConnection);
		verify(mockedDatabasePopulator2, times(1)).populate(mockedConnection);
	}
}
