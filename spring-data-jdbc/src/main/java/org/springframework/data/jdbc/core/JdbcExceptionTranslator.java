package org.springframework.data.jdbc.core;

import java.util.Set;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.support.PersistenceExceptionTranslator;

/**
 * {@link PersistenceExceptionTranslator} that is capable to translate exceptions for JDBC module.
 *
 * @author Mikhail Polivakha
 */
public class JdbcExceptionTranslator implements PersistenceExceptionTranslator {

  private static final Set<Class<? extends DataAccessException>> PASS_THROUGH_EXCEPTIONS = Set.of(
      OptimisticLockingFailureException.class,
      DuplicateKeyException.class
  );

  @Override
  public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
    if (PASS_THROUGH_EXCEPTIONS.contains(ex.getClass())) {
      return (DataAccessException) ex;
    }

    return null;
  }
}
