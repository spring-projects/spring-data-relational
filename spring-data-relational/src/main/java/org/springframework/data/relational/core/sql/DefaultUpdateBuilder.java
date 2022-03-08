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
package org.springframework.data.relational.core.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.springframework.data.relational.core.sql.UpdateBuilder.UpdateAssign;
import org.springframework.data.relational.core.sql.UpdateBuilder.UpdateWhere;
import org.springframework.data.relational.core.sql.UpdateBuilder.UpdateWhereAndOr;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default {@link UpdateBuilder} implementation.
 *
 * @author Mark Paluch
 * @since 1.1
 */
class DefaultUpdateBuilder implements UpdateBuilder, UpdateWhere, UpdateWhereAndOr, UpdateAssign {

	private @Nullable Table table;
	private List<Assignment> assignments = new ArrayList<>();
	private @Nullable Condition where;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.UpdateBuilder#table(org.springframework.data.relational.core.sql.Table)
	 */
	@Override
	public UpdateAssign table(Table table) {

		Assert.notNull(table, "Table must not be null!");

		this.table = table;

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.UpdateBuilder.UpdateAssign#set(org.springframework.data.relational.core.sql.Assignment)
	 */
	@Override
	public DefaultUpdateBuilder set(Assignment assignment) {

		Assert.notNull(assignment, "Assignment must not be null!");

		this.assignments.add(assignment);

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.UpdateBuilder.UpdateAssign#set(org.springframework.data.relational.core.sql.Assignment...)
	 */
	@Override
	public UpdateWhere set(Assignment... assignments) {

		Assert.notNull(assignments, "Assignment must not be null!");

		return set(Arrays.asList(assignments));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.UpdateBuilder.UpdateAssign#set(java.util.Collection)
	 */
	@Override
	public UpdateWhere set(Collection<? extends Assignment> assignments) {

		Assert.notNull(assignments, "Assignment must not be null!");

		this.assignments.addAll(assignments);

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.UpdateBuilder.UpdateWhere#where(org.springframework.data.relational.core.sql.Condition)
	 */
	@Override
	public UpdateWhereAndOr where(Condition condition) {

		Assert.notNull(condition, "Condition must not be null!");

		this.where = condition;

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.UpdateBuilder.UpdateWhereAndOr#and(org.springframework.data.relational.core.sql.Condition)
	 */
	@Override
	public UpdateWhereAndOr and(Condition condition) {

		Assert.notNull(condition, "Condition must not be null!");

		this.where = this.where.and(condition);

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.UpdateBuilder.UpdateWhereAndOr#or(org.springframework.data.relational.core.sql.Condition)
	 */
	@Override
	public UpdateWhereAndOr or(Condition condition) {

		Assert.notNull(condition, "Condition must not be null!");

		this.where = this.where.and(condition);

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.UpdateBuilder.BuildUpdate#build()
	 */
	@Override
	public Update build() {
		return new DefaultUpdate(this.table, this.assignments, this.where);
	}
}
