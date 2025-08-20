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
package org.springframework.data.jdbc.repository.aot;

import org.jspecify.annotations.Nullable;

import org.springframework.data.jdbc.core.mapping.JdbcValue;
import org.springframework.data.jdbc.repository.query.ParameterBinding;
import org.springframework.data.relational.repository.query.ParameterMetadataProvider;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.repository.query.parser.Part;

/**
 * Extension to {@link ParameterMetadataProvider} that captures the {@link ParameterBinding} for each parameter along
 * with its post-processed value.
 *
 * @author Mark Paluch
 */
class CapturingParameterMetadataProvider extends ParameterMetadataProvider {

	public CapturingParameterMetadataProvider(RelationalParameterAccessor accessor) {
		super(accessor);
	}

	@Nullable
	@Override
	protected Object prepareParameterValue(@Nullable Object value, Class<?> valueType, Part.Type partType) {

		Object prepared = super.prepareParameterValue(value, valueType, partType);

		return JdbcValue.of(value instanceof JdbcValue jv && jv.getValue() instanceof ParameterBinding pb
				? new CapturingJdbcValue(prepared, pb)
				: prepared, null);
	}

	static class CapturingJdbcValue extends JdbcValue {

		private final ParameterBinding binding;

		protected CapturingJdbcValue(@Nullable Object value, ParameterBinding binding) {
			super(value, null);
			this.binding = binding;
		}

		public static CapturingJdbcValue unwrap(@Nullable Object value) {

			if (!(value instanceof CapturingParameterMetadataProvider.CapturingJdbcValue) && value instanceof JdbcValue jv) {
				value = jv.getValue();
			}

			if (value instanceof CapturingParameterMetadataProvider.CapturingJdbcValue cp) {
				return cp;
			}

			throw new IllegalArgumentException("Cannot unwrap value: '%s' to CapturingJdbcValue".formatted(value));
		}

		public ParameterBinding getBinding() {
			return binding;
		}

		@Override
		public String toString() {
			return "s";
		}
	}
}
