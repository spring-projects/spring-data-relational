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

import com.nebhale.r2dbc.spi.Statement;

/**
 * General callback interface used by the {@link R2dbcTemplate} class.
 * <p>
 * This interface sets values on a {@link Statement} provided by the R2dbcTemplate class, for each of a number of
 * updates in a batch using the same SQL. Implementations are responsible for setting any necessary parameters. SQL with
 * placeholders will already have been supplied.
 *
 * @author Mark Paluch
 */
@FunctionalInterface
public interface StatementSetter {

	/**
	 * Set parameter values on the given {@link Statement}.
	 *
	 * @param ps the {@link Statement} to invoke setter methods on.
	 */
	void setValues(Statement ps);

}
