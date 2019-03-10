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
package org.springframework.data.r2dbc.function.convert;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.spi.Row;
import lombok.AllArgsConstructor;

import org.junit.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

/**
 * Unit tests for {@link MappingR2dbcConverter}.
 *
 * @author Mark Paluch
 */
public class MappingR2dbcConverterUnitTests {

	MappingR2dbcConverter converter = new MappingR2dbcConverter(new RelationalMappingContext());

	@Test // gh-61
	public void shouldIncludeAllPropertiesInOutboundRow() {

		OutboundRow row = new OutboundRow();

		converter.write(new Person("id", "Walter", "White"), row);

		assertThat(row).containsEntry("id", new SettableValue("id", String.class));
		assertThat(row).containsEntry("firstname", new SettableValue("Walter", String.class));
		assertThat(row).containsEntry("lastname", new SettableValue("White", String.class));
	}

	@Test // gh-41
	public void shouldPassThroughRow() {

		Row rowMock = mock(Row.class);

		Row result = converter.read(Row.class, rowMock);

		assertThat(result).isSameAs(rowMock);
	}

	@Test // gh-41
	public void shouldConvertRowToNumber() {

		Row rowMock = mock(Row.class);
		when(rowMock.get(0, Integer.class)).thenReturn(42);

		Integer result = converter.read(Integer.class, rowMock);

		assertThat(result).isEqualTo(42);
	}

	@AllArgsConstructor
	static class Person {
		@Id String id;
		String firstname, lastname;
	}
}
