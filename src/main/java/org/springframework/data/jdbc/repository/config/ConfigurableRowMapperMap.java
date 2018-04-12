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
package org.springframework.data.jdbc.repository.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.data.jdbc.repository.RowMapperMap;
import org.springframework.jdbc.core.RowMapper;

/**
 * A {@link RowMapperMap} that allows for registration of {@link RowMapper}s via a fluent Api.
 *
 * @author Jens Schauder
 * @since 1.0
 */
public class ConfigurableRowMapperMap implements RowMapperMap {

	private Map<Class<?>, RowMapper<?>> rowMappers = new LinkedHashMap<>();

	/**
	 * Registers a the given {@link RowMapper} as to be used for the given type.
	 *
	 * @return this instance, so this can be used as a fluent interface.
	 */
	public <T> ConfigurableRowMapperMap register(Class<T> type, RowMapper<? extends T> rowMapper) {

		rowMappers.put(type, rowMapper);
		return this;
	}

	@SuppressWarnings("unchecked")
	public <T> RowMapper<? extends T> rowMapperFor(Class<T> type) {

		RowMapper<? extends T> candidate = (RowMapper<? extends T>) rowMappers.get(type);

		if (candidate == null) {

			for (Map.Entry<Class<?>, RowMapper<?>> entry : rowMappers.entrySet()) {

				if (type.isAssignableFrom(entry.getKey())) {
					candidate = (RowMapper<? extends T>) entry.getValue();
				}
			}
		}

		return candidate;
	}
}
