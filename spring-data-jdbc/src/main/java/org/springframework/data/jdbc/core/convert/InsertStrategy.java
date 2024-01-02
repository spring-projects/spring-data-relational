/*
 * Copyright 2022-2024 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.lang.Nullable;

/**
 * Strategy for executing an insert.
 *
 * @author Chirag Tailor
 * @since 2.4
 */
interface InsertStrategy {

	/**
	 * @param sql the insert sql. Must not be {@code null}.
	 * @param sqlParameterSource the sql parameters for the record to be inserted. Must not be {@code null}.
	 * @return the id corresponding to the record that was inserted, if one was generated. If an id was not generated,
	 *         this will be {@code null}.
	 * @since 2.4
	 */
	@Nullable
	Object execute(String sql, SqlParameterSource sqlParameterSource);
}
