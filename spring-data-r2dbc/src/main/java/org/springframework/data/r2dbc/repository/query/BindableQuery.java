/*
 * Copyright 2018-2024 the original author or authors.
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
package org.springframework.data.r2dbc.repository.query;

import java.util.function.Supplier;

import org.springframework.r2dbc.core.DatabaseClient;

/**
 * Interface declaring a query that supplies SQL and can bind parameters to a {@link DatabaseClient.GenericExecuteSpec}.
 *
 * @author Mark Paluch
 */
public interface BindableQuery extends Supplier<String> {

	/**
	 * Bind parameters to the {@link DatabaseClient.GenericExecuteSpec query}.
	 *
	 * @param bindSpec must not be {@literal null}.
	 * @return the bound query object.
	 */
	DatabaseClient.GenericExecuteSpec bind(DatabaseClient.GenericExecuteSpec bindSpec);
}
