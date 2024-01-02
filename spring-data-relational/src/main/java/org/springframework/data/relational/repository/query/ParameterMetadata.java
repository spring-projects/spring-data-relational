/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.relational.repository.query;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Helper class for holding information about query parameter.
 *
 * @since 2.0
 */
class ParameterMetadata {

	private final String name;
	private final @Nullable Object value;
	private final Class<?> type;

	public ParameterMetadata(String name, @Nullable Object value, Class<?> type) {

		Assert.notNull(type, "Parameter type must not be null");
		this.name = name;
		this.value = value;
		this.type = type;
	}

	public String getName() {
		return name;
	}

	@Nullable
	public Object getValue() {
		return value;
	}

	public Class<?> getType() {
		return type;
	}
}
