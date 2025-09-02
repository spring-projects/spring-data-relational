/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.jdbc.repository.query;

import java.sql.ResultSet;

import org.jspecify.annotations.Nullable;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.relational.core.mapping.event.AfterConvertCallback;
import org.springframework.data.relational.core.mapping.event.AfterConvertEvent;
import org.springframework.jdbc.core.RowMapper;

/**
 * Delegating {@link RowMapper} implementation that applies post-processing logic after the
 * {@link RowMapper#mapRow(ResultSet, int)}. In particular, it emits the {@link AfterConvertEvent} event and invokes the
 * {@link AfterConvertCallback} callbacks.
 *
 * @author Mark Paluch
 * @author Mikhail Polivakha
 * @since 4.0
 */
public class CallbackCapableRowMapper<T> extends AbstractDelegatingRowMapper<T> {

	private final ApplicationEventPublisher publisher;
	private final @Nullable EntityCallbacks callbacks;

	public CallbackCapableRowMapper(RowMapper<T> delegate, ApplicationEventPublisher publisher,
			@Nullable EntityCallbacks callbacks) {

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
