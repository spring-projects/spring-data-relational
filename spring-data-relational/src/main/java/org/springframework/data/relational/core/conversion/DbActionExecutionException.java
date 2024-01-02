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
package org.springframework.data.relational.core.conversion;

/**
 * Exception thrown when during the execution of a {@link DbAction} an exception gets thrown. Provides additional
 * context information about the action and the entity.
 *
 * @author Jens Schauder
 */
public class DbActionExecutionException extends RuntimeException {

	/**
	 * @param action the {@link DbAction} which triggered the exception. Must not be {@code null}.
	 * @param cause the underlying exception. May not be {@code null}.
	 */
	public DbActionExecutionException(DbAction<?> action, Throwable cause) {
		super("Failed to execute " + action, cause);
	}
}
