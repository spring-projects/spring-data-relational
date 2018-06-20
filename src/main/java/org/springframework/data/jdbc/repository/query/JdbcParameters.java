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
package org.springframework.data.jdbc.repository.query;

import java.lang.reflect.Method;
import java.util.List;

import org.springframework.core.MethodParameter;
import org.springframework.data.jdbc.repository.query.JdbcParameters.JdbcParameter;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;

/**
 * Custom extension of {@link Parameters}.
 *
 * @author Mark Paluch
 */
public class JdbcParameters extends Parameters<JdbcParameters, JdbcParameter> {

	/**
	 * Creates a new {@link JdbcParameters} instance from the given {@link Method}.
	 *
	 * @param method must not be {@literal null}.
	 */
	public JdbcParameters(Method method) {
		super(method);
	}

	private JdbcParameters(List<JdbcParameter> parameters) {
		super(parameters);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.Parameters#createParameter(org.springframework.core.MethodParameter)
	 */
	@Override
	protected JdbcParameter createParameter(MethodParameter parameter) {
		return new JdbcParameter(parameter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.Parameters#createFrom(java.util.List)
	 */
	@Override
	protected JdbcParameters createFrom(List<JdbcParameter> parameters) {
		return new JdbcParameters(parameters);
	}

	/**
	 * Custom {@link Parameter} implementation.
	 *
	 * @author Mark Paluch
	 */
	class JdbcParameter extends Parameter {

		/**
		 * Creates a new {@link JdbcParameter}.
		 *
		 * @param parameter must not be {@literal null}.
		 */
		JdbcParameter(MethodParameter parameter) {
			super(parameter);
		}
	}
}
