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
import org.springframework.lang.Nullable;

/**
 * Thrown by {@link ScriptUtils} if an SQL script cannot be properly parsed.
 *
 * @author Mark Paluch
 * @deprecated since 1.2 in favor of Spring R2DBC. Use {@link org.springframework.r2dbc.connection.init} instead.
 */
@Deprecated
public class ScriptParseException extends ScriptException {

	private static final long serialVersionUID = 6130513243627087332L;

	/**
	 * Creates a new {@link ScriptParseException}.
	 *
	 * @param message detailed message.
	 * @param resource the resource from which the SQL script was read.
	 */
	public ScriptParseException(String message, @Nullable EncodedResource resource) {
		super(buildMessage(message, resource));
	}

	/**
	 * Creates a new {@link ScriptParseException}.
	 *
	 * @param message detailed message.
	 * @param resource the resource from which the SQL script was read.
	 * @param cause the underlying cause of the failure.
	 */
	public ScriptParseException(String message, @Nullable EncodedResource resource, @Nullable Throwable cause) {
		super(buildMessage(message, resource), cause);
	}

	private static String buildMessage(String message, @Nullable EncodedResource resource) {
		return String.format("Failed to parse SQL script from resource [%s]: %s",
				(resource == null ? "<unknown>" : resource), message);
	}
}
