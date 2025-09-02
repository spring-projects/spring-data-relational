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
import java.sql.SQLException;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.Assert;

/**
 * Abstract {@link RowMapper} that delegates the actual mapping logic to a {@link AbstractDelegatingRowMapper#delegate
 * delegate}
 *
 * @author Mikhail Polivakha
 * @since 4.0
 */
public abstract class AbstractDelegatingRowMapper<T extends @Nullable Object> implements RowMapper<T> {

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
	protected T postProcessMapping(T object) {
		return object;
	}
}
