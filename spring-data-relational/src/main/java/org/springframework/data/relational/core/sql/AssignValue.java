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

import org.springframework.util.Assert;

/**
 * Assign a {@link Expression} to a {@link Column}.
 * <p>
 * Results in a rendered assignment: {@code <column> = <value>} (e.g. {@code col = 'foo'}.
 * </p>
 * @author Mark Paluch
 * @since 1.1
 */
public class AssignValue extends AbstractSegment implements Assignment {

	private final Column column;
	private final Expression value;

	private AssignValue(Column column, Expression value) {
		super(column, value);
		this.column = column;
		this.value = value;
	}

	/**
	 * Creates a {@link AssignValue value} assignment to a {@link Column} given an {@link Expression}.
	 *
	 * @param target target column, must not be {@literal null}.
	 * @param value assignment value, must not be {@literal null}.
	 * @return the {@link AssignValue}.
	 */
	public static AssignValue create(Column target, Expression value) {

		Assert.notNull(target, "Target column must not be null");
		Assert.notNull(value, "Value must not be null");

		return new AssignValue(target, value);
	}

	/**
	 * @return the target {@link Column}.
	 */
	public Column getColumn() {
		return column;
	}

	/**
	 * @return the value to assign.
	 */
	public Expression getValue() {
		return value;
	}

	@Override
	public String toString() {

		return this.column + " = " + this.value;
	}
}
