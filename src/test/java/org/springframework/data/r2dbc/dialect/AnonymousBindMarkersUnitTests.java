/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.r2dbc.dialect;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.spi.Statement;

import org.junit.Test;

/**
 * Unit tests for {@link AnonymousBindMarkers}.
 *
 * @author Mark Paluch
 */
public class AnonymousBindMarkersUnitTests {

	@Test // gh-75
	public void shouldCreateNewBindMarkers() {

		BindMarkersFactory factory = BindMarkersFactory.anonymous("?");

		BindMarkers bindMarkers1 = factory.create();
		BindMarkers bindMarkers2 = factory.create();

		assertThat(bindMarkers1.next().getPlaceholder()).isEqualTo("?");
		assertThat(bindMarkers2.next().getPlaceholder()).isEqualTo("?");
	}

	@Test // gh-75
	public void shouldBindByIndex() {

		Statement statement = mock(Statement.class);

		BindMarkers bindMarkers = BindMarkersFactory.anonymous("?").create();

		BindMarker first = bindMarkers.next();
		BindMarker second = bindMarkers.next();

		second.bind(statement, "foo");
		first.bindNull(statement, Object.class);

		verify(statement).bindNull(0, Object.class);
		verify(statement).bind(1, "foo");
	}
}
