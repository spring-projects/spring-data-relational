/*
 * Copyright 2019-2024 the original author or authors.
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
package org.springframework.data.r2dbc.dialect;

import static org.assertj.core.api.Assertions.*;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.core.dialect.ArrayColumns;
import org.springframework.r2dbc.core.binding.BindMarker;
import org.springframework.r2dbc.core.binding.BindMarkers;

/**
 * Unit tests for {@link SqlServerDialect}.
 *
 * @author Mark Paluch
 */
class SqlServerDialectUnitTests {

	@Test // gh-20
	void shouldUseNamedPlaceholders() {

		BindMarkers bindMarkers = SqlServerDialect.INSTANCE.getBindMarkersFactory().create();

		BindMarker first = bindMarkers.next();
		BindMarker second = bindMarkers.next("'foo!bar");

		assertThat(first.getPlaceholder()).isEqualTo("@P0");
		assertThat(second.getPlaceholder()).isEqualTo("@P1_foobar");
	}

	@Test // gh-30
	void shouldConsiderUuidAsSimple() {

		SimpleTypeHolder holder = SqlServerDialect.INSTANCE.getSimpleTypeHolder();

		assertThat(holder.isSimpleType(UUID.class)).isTrue();
	}

	@Test // gh-30
	void shouldNotSupportArrays() {

		ArrayColumns arrayColumns = SqlServerDialect.INSTANCE.getArraySupport();

		assertThat(arrayColumns.isSupported()).isFalse();
		assertThatThrownBy(() -> arrayColumns.getArrayType(String.class)).isInstanceOf(UnsupportedOperationException.class);
	}
}
