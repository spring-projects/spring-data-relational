package org.springframework.data.jdbc.repository.support;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Abstract {@link RowMapper} that delegates the actual mapping logic to a {@link AbstractDelegatingRowMapper#delegate delegate}
 *
 * @author Mikhail Polivakha
 */
public abstract class AbstractDelegatingRowMapper<T> implements RowMapper<T> {

    private final RowMapper<T> delegate;

    protected AbstractDelegatingRowMapper(RowMapper<T> delegate) {
        Assert.notNull(delegate, "Delegating RowMapper cannot be null");

        this.delegate = delegate;
    }

    @Override
    public T mapRow(ResultSet rs, int rowNum) throws SQLException {
        T intermediate = delegate.mapRow(rs, rowNum);
        return postProcessMapping(intermediate);
    }

    /**
     * The post-processing callback for implementations.
     *
     * @return the mapped entity after applying post-processing logic
     */
    protected T postProcessMapping(@Nullable T object) {
        return object;
    }
}
