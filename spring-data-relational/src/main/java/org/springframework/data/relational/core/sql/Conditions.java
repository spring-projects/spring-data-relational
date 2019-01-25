/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.relational.core.sql;

/**
 * Factory for common {@link Condition}s.
 *
 * @author Mark Paluch
 * @see SQL
 * @see Expressions
 * @see Functions
 */
public abstract class Conditions {

	/**
	 * @return a new {@link Equals} condition.
	 */
	public static Equals equals(Expression left, Expression right) {
		return Equals.create(left, right);
	}

	/**
	 * Creates a plain {@code sql} {@link Condition}.
	 *
	 * @param sql the SQL, must not be {@literal null} or empty.
	 * @return a SQL {@link Expression}.
	 */
	public static Condition just(String sql) {
		return new ConstantCondition(sql);
	}

	// Utility constructor.
	private Conditions() {
	}

	public static Condition isNull(Expression expression) {
		return new IsNull(expression);
	}

	public static Condition isEqual(Column bar, Expression param) {
		return new Equals(bar, param);
	}

	public static Condition in(Column bar, Expression subselectExpression) {
		return new In(bar, subselectExpression);
	}

	static class ConstantCondition extends AbstractSegment implements Condition {

		private final String condition;

		ConstantCondition(String condition) {
			this.condition = condition;
		}

		@Override
		public String toString() {
			return condition;
		}
	}
}




