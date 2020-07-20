/*
 * Copyright 2019-2020 the original author or authors.
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
package org.springframework.data.r2dbc.connectionfactory.init;

import org.springframework.core.io.support.EncodedResource;

/**
 * Thrown by {@link ScriptUtils} if an SQL script cannot be read.
 *
 * @author Mark Paluch
 * @deprecated since 1.2 in favor of Spring R2DBC. Use {@link org.springframework.r2dbc.connection.init} instead.
 */
@Deprecated
public class CannotReadScriptException extends ScriptException {

	private static final long serialVersionUID = 7253084944991764250L;

	/**
	 * Creates a new {@link CannotReadScriptException}.
	 *
	 * @param resource the resource that cannot be read from.
	 * @param cause the underlying cause of the resource access failure.
	 */
	public CannotReadScriptException(EncodedResource resource, Throwable cause) {
		super("Cannot read SQL script from " + resource, cause);
	}
}
