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
package org.springframework.data.relational.core.query;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.data.relational.core.sql.BindMarker;

/**
 * @author Mark Paluch
 */
class SimpleBindings implements Bindings {

	private final Map<BindMarker, Object> values = new LinkedHashMap<>();

	@Override
	public void bind(BindMarker bindMarker, Object value) {
		values.put(bindMarker, value);
	}

	@Override
	public void bind(BindMarker bindMarker, Object value, QueryExpression.ExpressionTypeContext typeContext) {
		values.put(bindMarker, value);
	}

	public Map<BindMarker, Object> getValues() {
		return values;
	}
}
