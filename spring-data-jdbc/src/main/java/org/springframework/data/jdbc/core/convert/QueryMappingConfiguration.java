/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.RowMapper;

/**
 * Configures a {@link org.springframework.jdbc.core.RowMapper} for each type to be used for extracting entities of that
 * type from a {@link java.sql.ResultSet}.
 *
 * @author Jens Schauder
 * @author Evgeni Dimitrov
 * @since 4.0
 */
public interface QueryMappingConfiguration {

	/**
	 * Returns the {@link RowMapper} to be used for the given type or {@literal null} if no specific mapper is configured.
	 *
	 * @param type
	 * @return
	 * @param <T extends @Nullable Object>
	 */
	<T> @Nullable RowMapper<? extends T> getRowMapper(Class<T> type);

	/**
	 * An immutable empty instance that will return {@literal null} for all arguments.
	 */
	QueryMappingConfiguration EMPTY = new QueryMappingConfiguration() {

		@Override
		public <T extends @Nullable Object> @Nullable RowMapper<? extends T> getRowMapper(Class<T> type) {
			return null;
		}

	};

}
