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
package org.springframework.data.jdbc.mapping.model;

import static org.assertj.core.api.AssertionsForClassTypes.*;
import static org.mockito.Mockito.*;

import lombok.Data;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Date;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * Unit tests for the {@link BasicJdbcPersistentProperty}.
 *
 * @author Jens Schauder
 */
public class BasicJdbcPersistentPropertyUnitTests {

	@Test // DATAJDBC-104
	public void enumGetsStoredAsString() {

		JdbcPersistentEntity<?> persistentEntity = new JdbcMappingContext(mock(NamedParameterJdbcOperations.class))
			.getRequiredPersistentEntity(DummyEntity.class);

		persistentEntity.doWithProperties((PropertyHandler<JdbcPersistentProperty>) p -> {
			switch (p.getName()) {
				case "someEnum":
					assertThat(p.getColumnType()).isEqualTo(String.class);
					break;
				case "localDateTime":
					assertThat(p.getColumnType()).isEqualTo(Date.class);
					break;
				case "zonedDateTime":
					assertThat(p.getColumnType()).isEqualTo(String.class);
					break;
				default:
					Assertions.fail("property with out assert: " + p.getName());
			}
		});

	}

	@Data
	private static class DummyEntity {

		private final SomeEnum someEnum;
		private final LocalDateTime localDateTime;
		private final ZonedDateTime zonedDateTime;
	}

	private enum SomeEnum {
		@SuppressWarnings("unused")
		ALPHA
	}
}
