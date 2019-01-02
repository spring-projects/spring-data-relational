/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.relational.repository.query;

import java.lang.reflect.Method;
import java.util.List;

import org.springframework.core.MethodParameter;
import org.springframework.data.relational.repository.query.RelationalParameters.RelationalParameter;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;

/**
 * Custom extension of {@link Parameters}.
 *
 * @author Mark Paluch
 */
public class RelationalParameters extends Parameters<RelationalParameters, RelationalParameter> {

	/**
	 * Creates a new {@link RelationalParameters} instance from the given {@link Method}.
	 *
	 * @param method must not be {@literal null}.
	 */
	public RelationalParameters(Method method) {
		super(method);
	}

	private RelationalParameters(List<RelationalParameter> parameters) {
		super(parameters);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.Parameters#createParameter(org.springframework.core.MethodParameter)
	 */
	@Override
	protected RelationalParameter createParameter(MethodParameter parameter) {
		return new RelationalParameter(parameter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.Parameters#createFrom(java.util.List)
	 */
	@Override
	protected RelationalParameters createFrom(List<RelationalParameter> parameters) {
		return new RelationalParameters(parameters);
	}

	/**
	 * Custom {@link Parameter} implementation.
	 *
	 * @author Mark Paluch
	 */
	public static class RelationalParameter extends Parameter {

		/**
		 * Creates a new {@link RelationalParameter}.
		 *
		 * @param parameter must not be {@literal null}.
		 */
		RelationalParameter(MethodParameter parameter) {
			super(parameter);
		}
	}
}
