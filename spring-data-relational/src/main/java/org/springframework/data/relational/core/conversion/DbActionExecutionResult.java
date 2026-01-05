/*
 * Copyright 2019-present the original author or authors.
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

import org.jspecify.annotations.Nullable;

/**
 * @author Jens Schauder
 * @author Chirag Tailor
 * @since 2.0
 */
public class DbActionExecutionResult {

	private final @Nullable Object generatedId;
	private final DbAction.WithEntity<?> action;

	public DbActionExecutionResult(DbAction.WithEntity<?> action) {

		this.action = action;
		this.generatedId = null;
	}

	public DbActionExecutionResult(DbAction.WithEntity<?> action, @Nullable Object generatedId) {

		this.action = action;
		this.generatedId = generatedId;
	}

	/**
	 * @deprecated Use one of the other constructors.
	 */
	@SuppressWarnings("NullAway")
	@Deprecated(since = "4.0", forRemoval = true)
	public DbActionExecutionResult() {

		action = null;
		generatedId = null;
	}

	@Nullable
	public Object getGeneratedId() {
		return generatedId;
	}

	public DbAction.WithEntity<?> getAction() {
		return action;
	}
}
