/*
 * Copyright 2019-2024 the original author or authors.
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
package org.springframework.data.relational.core.sql;

/**
 * Factory for common {@link Assignment}s.
 *
 * @author Mark Paluch
 * @since 1.1
 * @see SQL
 * @see Expressions
 * @see Functions
 */
public abstract class Assignments {

	/**
	 * Creates a {@link AssignValue value} assignment to a {@link Column} given an {@link Expression}.
	 *
	 * @param target target column, must not be {@literal null}.
	 * @param value assignment value, must not be {@literal null}.
	 * @return the {@link AssignValue}.
	 */
	public static AssignValue value(Column target, Expression value) {
		return AssignValue.create(target, value);
	}

	// Utility constructor.
	private Assignments() {}
}
