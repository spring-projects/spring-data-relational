package org.springframework.data.jdbc.repository.support;

import org.springframework.core.convert.converter.Converter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.Nullable;

/**
 * Delegating {@link RowMapper} that reads a row into {@code T} and converts it afterwards into {@code Object}.
 *
 * @author Mark Paluch
 * @author Mikhail Polivakha
 *
 * @since 2.3
 */
public class ConvertingRowMapper extends AbstractDelegatingRowMapper<Object> {

    private final Converter<Object, Object> converter;

    public ConvertingRowMapper(RowMapper<Object> delegate, Converter<Object, Object> converter) {
        super(delegate);
        this.converter = converter;
    }

    @Override
    public Object postProcessMapping(@Nullable Object object) {
        return object != null ? converter.convert(object) : null;
    }
}
