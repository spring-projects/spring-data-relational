package org.springframework.data.jdbc.core;

import org.springframework.data.repository.Repository;

/**
 * Jdbc repository for dedicated insert(), update() and upsert() sql functions.
 * Other than {@link org.springframework.data.jdbc.repository.support.SimpleJdbcRepository}
 * there should be bypassing of the isNew check.
 *
 * @author Thomas Lang
 * @see <a href="https://jira.spring.io/browse/DATAJDBC-282">DATAJDBC-282</a>
 */
public interface JdbcRepository<T, ID> extends Repository<T, ID> {

    <S extends T> S insert(S var1);

    <S extends T> S update(S var1);
}
