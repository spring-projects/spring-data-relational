package org.springframework.data.jdbc.core;

import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;

/**
 * Unit tests for {@link JdbcExceptionTranslator}
 *
 * @author Mikhail Polivakha
 */
class JdbcExceptionTranslatorTest {

  @ParameterizedTest
  @MethodSource(value = "passThroughExceptionsSource")
  void testPassThroughExceptions(DataAccessException exception) {

    // when
    DataAccessException translated = new JdbcExceptionTranslator().translateExceptionIfPossible(exception);

    // then.
    Assertions.assertThat(translated).isSameAs(exception);
  }

  @Test
  void testUnrecognizedException() {

    // when
    DataAccessException translated = new JdbcExceptionTranslator().translateExceptionIfPossible(new IllegalArgumentException());

    // then.
    Assertions.assertThat(translated).isNull();
  }

  static Stream<Arguments> passThroughExceptionsSource() {
    return Stream.of(Arguments.of(new OptimisticLockingFailureException("")), Arguments.of(new DuplicateKeyException("")));
  }
}