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
 * {@link Segment} for a {@code JOIN} declaration.
 * <p>
 * Renders to: {@code JOIN
 *
<table>
 *  ON <condition>}.
 * </p>
 * 
 * @author Mark Paluch
 * @since 1.1
 */
public class Join extends AbstractSegment {

	private final JoinType type;
	private final TableLike joinTable;
	private final Condition on;

	Join(JoinType type, TableLike joinTable, Condition on) {

		super(joinTable, on);

		this.joinTable = joinTable;
		this.type = type;
		this.on = on;
	}

	/**
	 * @return join type.
	 */
	public JoinType getType() {
		return type;
	}

	/**
	 * @return the joined {@link Table}.
	 */
	public TableLike getJoinTable() {
		return joinTable;
	}

	/**
	 * @return join condition (the ON or USING part).
	 */
	public Condition getOn() {
		return on;
	}

	@Override
	public String toString() {
		return type + " " + joinTable + " ON " + on;
	}

	public enum JoinType {

		/**
		 * {@code INNER JOIN} for two tables.
		 */

		JOIN("JOIN"),

		/**
		 * {@code CROSS JOIN} for two tables.
		 */

		CROSS_JOIN("CROSS JOIN"),

		/**
		 * {@code LEFT OUTER JOIN} two tables.
		 */

		LEFT_OUTER_JOIN("LEFT OUTER JOIN"),

		/**
		 * {@code RIGHT OUTER JOIN} two tables.
		 */

		RIGHT_OUTER_JOIN("RIGHT OUTER JOIN"),

		/**
		 * {@code FULL OUTER JOIN} two tables.
		 */

		FULL_OUTER_JOIN("FULL OUTER JOIN");

		private final String sql;

		JoinType(String sql) {
			this.sql = sql;
		}

		public String getSql() {
			return sql;
		}
	}
}
