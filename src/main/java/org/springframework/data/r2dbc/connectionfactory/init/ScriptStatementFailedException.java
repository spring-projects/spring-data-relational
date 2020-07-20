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
 * Thrown by {@link ScriptUtils} if a statement in an SQL script failed when executing it against the target database.
 *
 * @author Mark Paluch
 * @deprecated since 1.2 in favor of Spring R2DBC. Use {@link org.springframework.r2dbc.connection.init} instead.
 */
@Deprecated
public class ScriptStatementFailedException extends ScriptException {

	private static final long serialVersionUID = 912676424615782262L;

	/**
	 * Creates a new {@link ScriptStatementFailedException}.
	 *
	 * @param statement the actual SQL statement that failed.
	 * @param statementNumber the statement number in the SQL script (i.e., the n'th statement present in the resource).
	 * @param encodedResource the resource from which the SQL statement was read.
	 * @param cause the underlying cause of the failure.
	 */
	public ScriptStatementFailedException(String statement, int statementNumber, EncodedResource encodedResource,
			Throwable cause) {
		super(buildErrorMessage(statement, statementNumber, encodedResource), cause);
	}

	/**
	 * Build an error message for an SQL script execution failure, based on the supplied arguments.
	 *
	 * @param statement the actual SQL statement that failed.
	 * @param statementNumber the statement number in the SQL script (i.e., the n'th statement present in the resource).
	 * @param encodedResource the resource from which the SQL statement was read.
	 * @return an error message suitable for an exception's detail message or logging.
	 */
	public static String buildErrorMessage(String statement, int statementNumber, EncodedResource encodedResource) {
		return String.format("Failed to execute SQL script statement #%s of %s: %s", statementNumber, encodedResource,
				statement);
	}
}
