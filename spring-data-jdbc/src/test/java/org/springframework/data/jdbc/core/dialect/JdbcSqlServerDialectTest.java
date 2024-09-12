/*
 * Copyright 2024 the original author or authors.
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
package org.springframework.data.jdbc.core.dialect;

import java.time.Instant;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;

/**
 * Tests for {@link JdbcSqlServerDialect}
 *
 * @author Mikhail Polivakha
 */
class JdbcSqlServerDialectTest {

    @Test // GH-1873
    void testCustomConversions() {

        JdbcCustomConversions jdbcCustomConversions = new JdbcCustomConversions(
          (List<?>) JdbcSqlServerDialect.INSTANCE.getConverters());

        Assertions
          .assertThat(jdbcCustomConversions.hasCustomReadTarget(microsoft.sql.DateTimeOffset.class, Instant.class))
          .isTrue();
    }
}