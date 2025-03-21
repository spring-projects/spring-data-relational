/*
 * Copyright 2018-2025 the original author or authors.
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

import java.util.List;
import java.util.function.Function;

import org.springframework.core.MethodParameter;
import org.springframework.data.relational.repository.query.RelationalParameters;
import org.springframework.data.relational.repository.query.RelationalParameters.RelationalParameter;
import org.springframework.data.repository.query.ParametersSource;

/**
 * Custom extension of {@link RelationalParameters}.
 *
 * @author Mark Paluch
 * @author Mikhail Polivakha
 * @since 3.2.6
 */
public class JdbcParameters extends RelationalParameters {

	/**
	 * Creates a new {@link JdbcParameters} instance from the given {@link ParametersSource}.
	 *
	 * @param parametersSource must not be {@literal null}.
	 */
	public JdbcParameters(ParametersSource parametersSource) {
		this(parametersSource, methodParameter -> new JdbcParameter(methodParameter, parametersSource.getDomainTypeInformation()));
	}

	/**
	 * Creates a new {@link JdbcParameters} instance from the given {@link ParametersSource} and function
	 * of turning the {@link MethodParameter} into {@link RelationalParameter}.
	 *
	 * @param parametersSource must not be {@literal null}.
	 */
	public JdbcParameters(ParametersSource parametersSource, Function<MethodParameter, RelationalParameter> parameterFactory) {
		super(parametersSource, parameterFactory);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private JdbcParameters(List<JdbcParameter> parameters) {
		super((List) parameters);
	}

	@Override
	public JdbcParameter getParameter(int index) {
		return (JdbcParameter) super.getParameter(index);
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected JdbcParameters createFrom(List<RelationalParameter> parameters) {
		return new JdbcParameters((List) parameters);
	}

}
