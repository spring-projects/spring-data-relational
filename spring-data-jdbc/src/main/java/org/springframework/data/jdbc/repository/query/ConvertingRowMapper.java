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

import org.jspecify.annotations.Nullable;
import org.springframework.core.convert.converter.Converter;
import org.springframework.jdbc.core.RowMapper;

/**
 * Delegating {@link RowMapper} that reads a row into {@code T} and converts it afterward into {@code Object}.
 *
 * @author Mark Paluch
 * @author Mikhail Polivakha
 * @since 4.0
 */
class ConvertingRowMapper extends AbstractDelegatingRowMapper<Object> {

	private final Converter<Object, Object> converter;

	public ConvertingRowMapper(RowMapper<Object> delegate, Converter<Object, Object> converter) {

		super(delegate);

		this.converter = converter;
	}

	@Override
	@SuppressWarnings("NullAway")
	public @Nullable Object postProcessMapping(@Nullable Object object) {
		return object != null ? converter.convert(object) : null;
	}
}
