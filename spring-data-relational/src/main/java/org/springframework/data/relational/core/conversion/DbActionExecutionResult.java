/*
 * Copyright 2019-2022 the original author or authors.
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
package org.springframework.data.relational.core.conversion;

import org.springframework.lang.Nullable;

/**
 * This class encapsulates the {@link DbAction} after its execution with possibly adjusted
 * entity, see {@link DbAction.WithEntity#getEntity()}
 *
 * @author Jens Schauder
 * @author Mikhail Polivakha
 * @since 2.0
 */
public class DbActionExecutionResult {

	private final DbAction<?> action;

	public DbActionExecutionResult(DbAction<?> action) {
		this.action = action;
	}

	/**
	 * @deprecated This API is not longer used in spring-data-jdbc and will be removed in future releases
	 */
	@Deprecated
	public DbActionExecutionResult() {
		this.action = null;
	}

	/**
	 * @deprecated DbActionExecutionResult should not have any reference to id, only {@link InsertExecutionResult}
	 * can possibly have generated id
	 *
	 * @see InsertExecutionResult
	 */
	@Deprecated
	@Nullable
	public Object getId() {
		return null;
	}

	public DbAction<?> getAction() {
		return action;
	}
}
