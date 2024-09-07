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

    @Test
    void testCustomConversions() {
        JdbcCustomConversions jdbcCustomConversions = new JdbcCustomConversions(
          (List<?>) JdbcSqlServerDialect.INSTANCE.getConverters());

        Assertions
          .assertThat(jdbcCustomConversions.hasCustomReadTarget(microsoft.sql.DateTimeOffset.class, Instant.class))
          .isTrue();
    }
}