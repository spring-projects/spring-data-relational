package org.springframework.data.jdbc.repository.support;

import java.sql.ResultSet;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.relational.core.mapping.event.AfterConvertCallback;
import org.springframework.data.relational.core.mapping.event.AfterConvertEvent;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.Nullable;

/**
 * Delegating {@link RowMapper} implementation that applies post-processing logic
 * after the {@link RowMapper#mapRow(ResultSet, int)}. In particular, it emits the
 * {@link AfterConvertEvent} event and invokes the {@link AfterConvertCallback} callbacks.
 *
 * @author Mark Paluch
 * @author Mikhail Polivakha
 */
public class CallbackAwareRowMapper<T> extends AbstractDelegatingRowMapper<T> {

    private final ApplicationEventPublisher publisher;
    private final @Nullable EntityCallbacks callbacks;

    public CallbackAwareRowMapper(RowMapper<T> delegate, ApplicationEventPublisher publisher, @Nullable EntityCallbacks callbacks) {
        super(delegate);
        this.publisher = publisher;
        this.callbacks = callbacks;
    }

    @Override
    public T postProcessMapping(@Nullable T object) {
        if (object != null) {

            publisher.publishEvent(new AfterConvertEvent<>(object));

            if (callbacks != null) {
                return callbacks.callback(AfterConvertCallback.class, object);
            }

        }
        return object;
    }
}
