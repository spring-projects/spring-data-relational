/*
 * Copyright 2024 the original author or authors.
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

import org.springframework.data.relational.core.query.Query;

/**
 * Interface for query wrapper implementations that allow customization of queries before execution.
 *
 * @author Your Name
 * @since 3.4
 */
public interface QueryWrapper {

	/**
	 * Wraps the given query for the specified domain type.
	 *
	 * @param query the original query to wrap, must not be {@literal null}.
	 * @param domainType the domain type for which the query is executed, must not be {@literal null}.
	 * @return the wrapped query, must not be {@literal null}.
	 */
	Query wrapper(Query query, Class<?> domainType);
}
