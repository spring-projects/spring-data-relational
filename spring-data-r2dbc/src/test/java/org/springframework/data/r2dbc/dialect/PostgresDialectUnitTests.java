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
import static org.assertj.core.api.SoftAssertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.core.dialect.ArrayColumns;
import org.springframework.r2dbc.core.binding.BindMarker;
import org.springframework.r2dbc.core.binding.BindMarkers;

/**
 * Unit tests for {@link PostgresDialect}.
 *
 * @author Mark Paluch
 */
class PostgresDialectUnitTests {

	@Test // gh-20
	void shouldUsePostgresPlaceholders() {

		BindMarkers bindMarkers = PostgresDialect.INSTANCE.getBindMarkersFactory().create();

		BindMarker first = bindMarkers.next();
		BindMarker second = bindMarkers.next("foo");

		assertThat(first.getPlaceholder()).isEqualTo("$1");
		assertThat(second.getPlaceholder()).isEqualTo("$2");
	}

	@Test // gh-30
	void shouldConsiderSimpleTypes() {

		SimpleTypeHolder holder = PostgresDialect.INSTANCE.getSimpleTypeHolder();

		assertSoftly(it -> {
			it.assertThat(holder.isSimpleType(String.class)).isTrue();
			it.assertThat(holder.isSimpleType(int.class)).isTrue();
			it.assertThat(holder.isSimpleType(Integer.class)).isTrue();
		});
	}

	@Test // gh-30
	void shouldSupportArrays() {

		ArrayColumns arrayColumns = PostgresDialect.INSTANCE.getArraySupport();

		assertThat(arrayColumns.isSupported()).isTrue();
	}

	@Test // gh-30
	void shouldUseBoxedArrayTypesForPrimitiveTypes() {

		ArrayColumns arrayColumns = PostgresDialect.INSTANCE.getArraySupport();

		assertSoftly(it -> {
			it.assertThat(arrayColumns.getArrayType(int.class)).isEqualTo(Integer.class);
			it.assertThat(arrayColumns.getArrayType(double.class)).isEqualTo(Double.class);
			it.assertThat(arrayColumns.getArrayType(String.class)).isEqualTo(String.class);
		});
	}

	@Test // gh-30
	void shouldRejectNonSimpleArrayTypes() {

		ArrayColumns arrayColumns = PostgresDialect.INSTANCE.getArraySupport();

		assertThatThrownBy(() -> arrayColumns.getArrayType(getClass())).isInstanceOf(IllegalArgumentException.class);
	}

	@Test // gh-30
	void shouldRejectNestedCollections() {

		ArrayColumns arrayColumns = PostgresDialect.INSTANCE.getArraySupport();

		assertThatThrownBy(() -> arrayColumns.getArrayType(List.class)).isInstanceOf(IllegalArgumentException.class);
	}
}
