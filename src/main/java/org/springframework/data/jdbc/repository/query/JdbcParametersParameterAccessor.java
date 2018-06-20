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

import java.util.Arrays;
import java.util.List;

import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;

/**
 * JDBC-specific {@link ParametersParameterAccessor}.
 *
 * @author Mark Paluch
 */
public class JdbcParametersParameterAccessor extends ParametersParameterAccessor implements JdbcParameterAccessor {

	private final List<Object> values;

	/**
	 * Creates a new {@link JdbcParametersParameterAccessor}.
	 *
	 * @param method must not be {@literal null}.
	 * @param values must not be {@literal null}.
	 */
	public JdbcParametersParameterAccessor(QueryMethod method, Object[] values) {

		super(method.getParameters(), values);
		this.values = Arrays.asList(values);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.jdbc.repository.query.JdbcParameterAccessor#getValues()
	 */
	@Override
	public Object[] getValues() {
		return values.toArray();
	}
}
