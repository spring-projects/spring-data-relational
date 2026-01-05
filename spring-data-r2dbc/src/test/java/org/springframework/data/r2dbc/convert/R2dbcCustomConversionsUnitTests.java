/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.r2dbc.convert;

import static org.assertj.core.api.Assertions.*;

import io.r2dbc.spi.Blob;

import org.junit.jupiter.api.Test;

import org.springframework.data.r2dbc.dialect.H2Dialect;

/**
 * Unit tests for {@link R2dbcCustomConversions}.
 *
 * @author Mark Paluch
 */
class R2dbcCustomConversionsUnitTests {

	@Test // GH-2147
	void shouldReportR2dbcSimpleTypes() {

		R2dbcCustomConversions conversions = R2dbcCustomConversions.of(H2Dialect.INSTANCE);

		assertThat(conversions.isSimpleType(Blob.class)).isTrue();
	}
}
