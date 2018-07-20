/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.repository;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.Nullable;

/**
 * A map from a type to a {@link RowMapper} to be used for extracting that type from {@link java.sql.ResultSet}s.
 *
 * @author Jens Schauder
 */
public interface RowMapperMap {

	/**
	 * An immutable empty instance that will return {@literal null} for all arguments.
	 */
	RowMapperMap EMPTY = new RowMapperMap() {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.jdbc.repository.RowMapperMap#rowMapperFor(java.lang.Class)
		 */
		public <T> RowMapper<? extends T> rowMapperFor(Class<T> type) {
			return null;
		}
	};

	@Nullable
	<T> RowMapper<? extends T> rowMapperFor(Class<T> type);
}
