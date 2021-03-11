/*
 * Copyright 2021 the original author or authors.
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

import org.jmolecules.ddd.types.Association;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link JdbcCustomConversions}.
 *
 * @author Oliver Drotbohm
 */
class JdbcCustomConversionsUnitTests {

	@Test // #937
	void registersNonDateDefaultConverter() {

		JdbcCustomConversions conversions = new JdbcCustomConversions();

		assertThat(conversions.hasCustomWriteTarget(Association.class)).isTrue();
		assertThat(conversions.getSimpleTypeHolder().isSimpleType(Association.class));
	}
}
