/*
 * Copyright 2019-2025 the original author or authors.
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
package org.springframework.data.r2dbc.core;

import org.jspecify.annotations.Nullable;
import org.springframework.data.util.Streamable;

/**
 * Interface that defines common functionality for objects that can offer parameter values for named bind parameters,
 * serving as argument for {@link NamedParameterExpander} operations.
 * <p>
 * This interface allows for the specification of the type in addition to parameter values. All parameter values and
 * types are identified by specifying the name of the parameter.
 * <p>
 * Intended to wrap various implementations like a {@link java.util.Map} with a consistent interface.
 *
 * @author Mark Paluch
 * @see MapBindParameterSource
 * @deprecated since 1.2, without replacement.
 */
@Deprecated
interface BindParameterSource {

	/**
	 * Determine whether there is a value for the specified named parameter.
	 *
	 * @param paramName the name of the parameter.
	 * @return {@literal true} if there is a value defined; {@literal false} otherwise.
	 */
	boolean hasValue(String paramName);

	/**
	 * Return the parameter value for the requested named parameter.
	 *
	 * @param paramName the name of the parameter.
	 * @return the value of the specified parameter, can be {@literal null}.
	 * @throws IllegalArgumentException if there is no value for the requested parameter.
	 */
	@Nullable
	Object getValue(String paramName) throws IllegalArgumentException;

	/**
	 * Determine the type for the specified named parameter.
	 *
	 * @param paramName the name of the parameter.
	 * @return the type of the specified parameter, or {@link Object#getClass()} if not known.
	 */
	default Class<?> getType(String paramName) {
		return Object.class;
	}

	/**
	 * Returns parameter names of the underlying parameter source.
	 *
	 * @return parameter names of the underlying parameter source.
	 */
	Streamable<String> getParameterNames();
}
