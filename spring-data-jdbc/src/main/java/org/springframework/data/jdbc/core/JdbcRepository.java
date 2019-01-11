package org.springframework.data.jdbc.core;

import org.springframework.data.repository.Repository;

/**
 * Jdbc repository for dedicated insert(), update() and upsert() sql functions. Other than
 * {@link org.springframework.data.jdbc.repository.support.SimpleJdbcRepository} there should be bypassing of the isNew
 * check.
 *
 * @author Thomas Lang
 * @since 1.1
 */
public interface JdbcRepository<T, ID> extends Repository<T, ID> {

	/**
	 * Dedicated insert function. This skips the test if the aggregate root is new and makes an insert.
	 * <p>
	 * This is useful if the client provides an id for new aggregate roots.
	 * </p>
	 *
	 * @param aggregateRoot the aggregate root to be saved in the database. Must not be {@code null}.
	 * @param <S> Type of the aggregate root.
	 * @return the saved aggregate root. If the provided aggregate root was immutable and a value needed changing, e.g.
	 *         the id this will be a new instance.
	 */
	<S extends T> S insert(S aggregateRoot);

	/**
	 * Dedicated update function. This skips the test if the aggregate root is new or not and always performs an update
	 * operation.
	 * 
	 * @param aggregateRoot the aggregate root to be saved in the database. Must not be {@code null}.
	 * @param <S> Type of the aggregate root.
	 * @return the saved aggregate root. If the provided aggregate root was immutable and a value needed changing, e.g.
	 *         the id this will be a new instance.
	 */
	<S extends T> S update(S aggregateRoot);
}
