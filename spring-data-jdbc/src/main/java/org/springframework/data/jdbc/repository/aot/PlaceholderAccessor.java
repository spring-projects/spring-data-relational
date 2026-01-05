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
package org.springframework.data.jdbc.repository.aot;

import java.sql.JDBCType;
import java.util.Collection;

import org.jspecify.annotations.Nullable;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.mapping.JdbcValue;
import org.springframework.data.jdbc.repository.query.JdbcParameters;
import org.springframework.data.jdbc.repository.query.JdbcQueryMethod;
import org.springframework.data.jdbc.repository.query.ParameterBinding;
import org.springframework.data.relational.repository.query.ParameterMetadataProvider;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.relational.repository.query.RelationalParameters;
import org.springframework.data.relational.repository.query.RelationalParametersParameterAccessor;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.lang.Contract;
import org.springframework.util.Assert;

/**
 * Utility to access placeholders in AOT processing.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 4.0
 */
class PlaceholderAccessor {

	/**
	 * Create a new {@link CapturingJdbcValue} with the given binding.
	 *
	 * @param binding
	 * @return
	 */
	public static CapturingJdbcValue boundParameter(ParameterBinding binding) {
		return boundParameter(null, binding);
	}

	/**
	 * Create a new {@link CapturingJdbcValue} with the given value and binding.
	 *
	 * @param value
	 * @param binding
	 * @return
	 */
	public static CapturingJdbcValue boundParameter(@Nullable Object value, ParameterBinding binding) {
		return new CapturingJdbcValue(value, binding);
	}

	/**
	 * Unwrap a {@link CapturingJdbcValue} from the given value.
	 *
	 * @param value
	 * @return
	 */
	@Contract("null -> fail")
	public static CapturingJdbcValue unwrap(@Nullable Object value) {

		if (!(value instanceof CapturingJdbcValue) && value instanceof JdbcValue jv) {
			value = jv.getValue();
		}

		if (value instanceof CapturingJdbcValue cp) {
			return cp;
		}

		if(value instanceof Collection<?> c && c.iterator().hasNext()) {
			return unwrap(c.iterator().next());
		}

		throw new IllegalArgumentException("Cannot unwrap value: '%s' to CapturingJdbcValue".formatted(value));
	}

	/**
	 * Create a {@link ParameterMetadataProvider} that enhances {@link ParameterBinding} based on
	 * {@link org.springframework.data.repository.query.parser.PartTree} details for each parameter.
	 *
	 * @param accessor
	 * @return
	 */
	public static ParameterMetadataProvider metadata(RelationalParameterAccessor accessor) {
		return new CapturingParameterMetadataProvider(accessor);
	}

	/**
	 * Create a {@link RelationalParametersParameterAccessor} that captures the {@link ParameterBinding} for bindable
	 * parameters.
	 *
	 * @param queryMethod
	 * @param parameterValues
	 * @param parameters
	 * @param bindable
	 * @return
	 */
	public static RelationalParametersParameterAccessor capture(JdbcQueryMethod queryMethod, Object[] parameterValues,
			JdbcParameters parameters, RelationalParameters bindable) {
		return new CapturingParameterAccessor(queryMethod, parameterValues, parameters, bindable);
	}

	/**
	 * Extension to {@link ParameterMetadataProvider} that captures the {@link ParameterBinding} for each parameter along
	 * with its post-processed value.
	 *
	 * @author Mark Paluch
	 * @since 4.0
	 */
	static class CapturingParameterMetadataProvider extends ParameterMetadataProvider {

		public CapturingParameterMetadataProvider(RelationalParameterAccessor accessor) {
			super(accessor);
		}

		@Nullable
		@Override
		protected Object prepareParameterValue(@Nullable Object value, Class<?> valueType, Part.Type partType) {

			Assert.notNull(value, "Value must not be null");

			// apply double-wrapping as JdbcConverter unwraps JdbcValue; We don't want our placeholders to be converted.
			CapturingJdbcValue capturingJdbcValue = (CapturingJdbcValue) value;

			if (partType == Part.Type.STARTING_WITH || partType == Part.Type.ENDING_WITH || partType == Part.Type.CONTAINING
					|| partType == Part.Type.NOT_CONTAINING) {
				return JdbcValue.of(
						capturingJdbcValue.withBinding(ParameterBinding.like(capturingJdbcValue.getBinding(), partType)),
						JDBCType.OTHER);
			}

			return JdbcValue.of(capturingJdbcValue.withValue(super.prepareParameterValue(value, valueType, partType)),
					JDBCType.OTHER);
		}

	}

	/**
	 * {@link JdbcValue} that captures the {@link ParameterBinding} along with the value.
	 */
	static class CapturingJdbcValue extends JdbcValue {

		private final ParameterBinding binding;

		private CapturingJdbcValue(@Nullable Object value, ParameterBinding binding) {
			super(value, JDBCType.OTHER);
			Assert.notNull(binding, "Parameter binding must not be null");
			this.binding = binding;
		}

		public ParameterBinding getBinding() {
			return binding;
		}

		public CapturingJdbcValue withValue(@Nullable Object value) {

			if (value == this) {
				return this;
			}

			return new CapturingJdbcValue(value, binding);
		}

		public CapturingJdbcValue withBinding(ParameterBinding binding) {
			return new CapturingJdbcValue(getValue(), binding);
		}
	}

	static class CapturingParameterAccessor extends RelationalParametersParameterAccessor {

		private final JdbcParameters parameters;
		private final RelationalParameters bindable;

		public CapturingParameterAccessor(JdbcQueryMethod queryMethod, Object[] parameterValues, JdbcParameters parameters,
				RelationalParameters bindable) {
			super(queryMethod, parameterValues);
			this.parameters = parameters;
			this.bindable = bindable;
		}

		@Override
		public Sort getSort() {
			return Sort.unsorted();
		}

		@Override
		public Pageable getPageable() {
			return Pageable.unpaged();
		}

		@Override
		public @Nullable Object[] getValues() {
			return super.getValues();
		}

		@Override
		protected <T> @Nullable T getValue(int index) {
			return (T) capture(parameters.getParameter(index));
		}

		@Override
		public @Nullable Object getBindableValue(int index) {
			return capture(bindable.getParameter(index));
		}

		private CapturingJdbcValue capture(RelationalParameters.RelationalParameter parameter) {
			return boundParameter(ParameterBinding.named(parameter.getRequiredName(),
					ParameterBinding.ParameterOrigin.ofParameter(parameter.getIndex())));
		}

	}

}
