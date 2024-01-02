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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.springframework.data.relational.core.dialect.Escaper;
import org.springframework.data.relational.core.query.ValueFunction;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Helper class to allow easy creation of {@link ParameterMetadata}s.
 *
 * @author Roman Chigvintsev
 * @author Mark Paluch
 * @since 2.0
 */
class ParameterMetadataProvider implements Iterable<ParameterMetadata> {

	private static final Object VALUE_PLACEHOLDER = new Object();

	private final Iterator<? extends Parameter> bindableParameterIterator;
	private final Iterator<Object> bindableParameterValueIterator;
	private final List<ParameterMetadata> parameterMetadata = new ArrayList<>();

	/**
	 * Creates new instance of this class with the given {@link RelationalParameterAccessor} and {@link Escaper}.
	 *
	 * @param accessor relational parameter accessor (must not be {@literal null}).
	 */
	public ParameterMetadataProvider(RelationalParameterAccessor accessor) {
		this(accessor.getBindableParameters(), accessor.iterator());
	}

	/**
	 * Creates new instance of this class with the given {@link Parameters}, {@link Iterator} over all bindable parameter
	 * values and {@link Escaper}.
	 *
	 * @param bindableParameterValueIterator iterator over bindable parameter values
	 * @param parameters method parameters (must not be {@literal null})
	 */
	private ParameterMetadataProvider(Parameters<?, ?> parameters,
			@Nullable Iterator<Object> bindableParameterValueIterator) {

		Assert.notNull(parameters, "Parameters must not be null");

		this.bindableParameterIterator = parameters.getBindableParameters().iterator();
		this.bindableParameterValueIterator = bindableParameterValueIterator;
	}

	@Override
	public Iterator<ParameterMetadata> iterator() {
		return parameterMetadata.iterator();
	}

	/**
	 * Creates new instance of {@link ParameterMetadata} for the given {@link Part} and next {@link Parameter}.
	 */
	public ParameterMetadata next(Part part) {

		Assert.isTrue(bindableParameterIterator.hasNext(),
				() -> String.format("No parameter available for part %s.", part));

		Parameter parameter = bindableParameterIterator.next();
		String parameterName = getParameterName(parameter, part.getProperty().getSegment());
		Object parameterValue = getParameterValue();
		Part.Type partType = part.getType();

		checkNullIsAllowed(parameterName, parameterValue, partType);
		Class<?> parameterType = parameter.getType();
		Object preparedParameterValue = prepareParameterValue(parameterValue, parameterType, partType);

		ParameterMetadata metadata = new ParameterMetadata(parameterName, preparedParameterValue, parameterType);
		parameterMetadata.add(metadata);

		return metadata;
	}

	private String getParameterName(Parameter parameter, String defaultName) {

		if (parameter.isExplicitlyNamed()) {
			return parameter.getName().orElseThrow(() -> new IllegalArgumentException("Parameter needs to be named"));
		}
		return defaultName;
	}

	@Nullable
	private Object getParameterValue() {
		return bindableParameterValueIterator == null ? VALUE_PLACEHOLDER : bindableParameterValueIterator.next();
	}

	/**
	 * Checks whether {@literal null} is allowed as parameter value.
	 *
	 * @param parameterName parameter name
	 * @param parameterValue parameter value
	 * @param partType method name part type (must not be {@literal null})
	 * @throws IllegalArgumentException if {@literal null} is not allowed as parameter value
	 */
	private void checkNullIsAllowed(String parameterName, @Nullable Object parameterValue, Part.Type partType) {

		if (parameterValue == null && !Part.Type.SIMPLE_PROPERTY.equals(partType)) {
			throw new IllegalArgumentException(
					String.format("Value of parameter with name %s must not be null", parameterName));
		}
	}

	/**
	 * Prepares parameter value before it's actually bound to the query.
	 *
	 * @param value must not be {@literal null}
	 * @return prepared query parameter value
	 */
	@Nullable
	protected Object prepareParameterValue(@Nullable Object value, Class<?> valueType, Part.Type partType) {

		if (value == null || !CharSequence.class.isAssignableFrom(valueType)) {
			return value;
		}

		return switch (partType) {
			case STARTING_WITH -> (ValueFunction<String>) escaper -> escaper.escape(value.toString()) + "%";
			case ENDING_WITH -> (ValueFunction<String>) escaper -> "%" + escaper.escape(value.toString());
			case CONTAINING, NOT_CONTAINING -> (ValueFunction<String>) escaper -> "%" + escaper.escape(value.toString())
					+ "%";
			default -> value;
		};
	}
}
