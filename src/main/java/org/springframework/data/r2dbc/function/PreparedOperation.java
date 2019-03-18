/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.r2dbc.function;

import io.r2dbc.spi.Statement;

import java.util.function.Supplier;

/**
 * Extension to {@link QueryOperation} for a prepared SQL query {@link Supplier} with bound parameters. Contains
 * parameter bindings that can be {@link #bind(Statement)} bound to a {@link Statement}.
 * <p>
 * Can be executed with {@link DatabaseClient}.
 * </p>
 *
 * @param <T> underlying operation source.
 * @author Mark Paluch
 * @see DatabaseClient
 * @see DatabaseClient.SqlSpec#sql(Supplier)
 */
public interface PreparedOperation<T> extends QueryOperation {

	/**
	 * @return the query source, such as a statement/criteria object.
	 */
	T getSource();

	/**
	 * Bind query parameters to a {@link Statement}.
	 *
	 * @param to the target statement to bind parameters to.
	 * @return the bound statement.
	 */
	Statement bind(Statement to);
}
