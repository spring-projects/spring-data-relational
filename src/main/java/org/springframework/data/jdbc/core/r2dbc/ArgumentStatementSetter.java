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
package org.springframework.data.jdbc.core.r2dbc;

import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.lang.Nullable;

import io.r2dbc.spi.Statement;

/**
 * Simple adapter for {@link StatementSetter} that applies a given array of arguments.
 *
 * @author Mark Paluch
 */
public class ArgumentStatementSetter implements StatementSetter {

	private final @Nullable Object[] args;

	/**
	 * Create a new ArgPreparedStatementSetter for the given arguments.
	 *
	 * @param args the arguments to set
	 */
	public ArgumentStatementSetter(@Nullable Object[] args) {
		this.args = args;
	}

	@Override
	public void setValues(Statement ps) {
		if (this.args != null) {
			for (int i = 0; i < this.args.length; i++) {
				Object arg = this.args[i];
				doSetValue(ps, i, arg);
			}
		}
	}

	/**
	 * Set the value for prepared statements specified parameter index using the passed in value. This method can be
	 * overridden by sub-classes if needed.
	 *
	 * @param ps the Statement
	 * @param parameterPosition index of the parameter position
	 * @param argValue the value to set
	 */
	protected void doSetValue(Statement ps, int parameterPosition, Object argValue) {

		if (argValue instanceof SqlParameterValue) {
			SqlParameterValue paramValue = (SqlParameterValue) argValue;
			if (!paramValue.isInputValueProvided()) {

				if (paramValue.getName() == null) {
					ps.bindNull(parameterPosition, paramValue.getSqlType());
				} else {
					ps.bindNull(paramValue.getName(), paramValue.getSqlType());
				}
			} else {

				if (paramValue.getName() == null) {
					ps.bind(parameterPosition, paramValue.getValue());
				} else {
					ps.bindNull(paramValue.getName(), paramValue.getValue());
				}
			}
		} else {

			if (argValue == null) {
				ps.bindNull(parameterPosition, SqlTypeValue.TYPE_UNKNOWN);
			} else {

				ps.bind(parameterPosition, argValue);
			}
		}
	}
}
