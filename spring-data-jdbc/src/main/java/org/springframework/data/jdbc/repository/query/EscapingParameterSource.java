/*
 * Copyright 2023 the original author or authors.
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

import org.springframework.data.relational.core.dialect.Escaper;
import org.springframework.data.relational.core.query.ValueFunction;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * This {@link SqlParameterSource} will apply escaping to its values.
 *
 * @author Jens Schauder
 * @since 3.2
 */
class EscapingParameterSource implements SqlParameterSource {

	private final SqlParameterSource parameterSource;
	private final Escaper escaper;

	public EscapingParameterSource(SqlParameterSource parameterSource, Escaper escaper) {

		this.parameterSource = parameterSource;
		this.escaper = escaper;
	}

	@Override
	public boolean hasValue(String paramName) {
		return parameterSource.hasValue(paramName);
	}

	@Override
	public Object getValue(String paramName) throws IllegalArgumentException {

		Object value = parameterSource.getValue(paramName);
		if (value instanceof ValueFunction<?> valueFunction) {
			return valueFunction.apply(escaper);
		}
		return value;
	}
}
