/*
 * Copyright 2023-2024 the original author or authors.
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
package org.springframework.data.jdbc.core.mapping.schema;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.charset.Charset;
import java.time.Duration;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * Unit tests for {@link SqlTypeMapping}.
 *
 * @author Mark Paluch
 */
class SqlTypeMappingUnitTests {

	SqlTypeMapping typeMapping = new DefaultSqlTypeMapping() //
			.and(property -> property.getActualType().equals(ZoneId.class) ? "ZONEID" : null)
			.and(property -> property.getActualType().equals(Duration.class) ? "INTERVAL" : null);

	@Test // GH-1480
	void shouldComposeTypeMapping() {

		RelationalPersistentProperty p = mock(RelationalPersistentProperty.class);
		doReturn(String.class).when(p).getActualType();

		assertThat(typeMapping.getColumnType(p)).isEqualTo("VARCHAR(255 BYTE)");
		assertThat(typeMapping.getRequiredColumnType(p)).isEqualTo("VARCHAR(255 BYTE)");
	}

	@Test // GH-1480
	void shouldDelegateToCompositeTypeMapping() {

		RelationalPersistentProperty p = mock(RelationalPersistentProperty.class);
		doReturn(Duration.class).when(p).getActualType();

		assertThat(typeMapping.getColumnType(p)).isEqualTo("INTERVAL");
		assertThat(typeMapping.getRequiredColumnType(p)).isEqualTo("INTERVAL");
	}

	@Test // GH-1480
	void shouldPassThruNullValues() {

		RelationalPersistentProperty p = mock(RelationalPersistentProperty.class);
		doReturn(Charset.class).when(p).getActualType();

		assertThat(typeMapping.getColumnType(p)).isNull();
		assertThatIllegalArgumentException().isThrownBy(() -> typeMapping.getRequiredColumnType(p));
	}
}
