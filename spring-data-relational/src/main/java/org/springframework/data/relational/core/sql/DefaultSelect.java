/*
 * Copyright 2019-2023 the original author or authors.
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
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default {@link Select} implementation.
 *
 * @author Mark Paluch
 * @author Myeonghyeon Lee
 * @since 1.1
 */
class DefaultSelect implements Select {

	private final boolean distinct;
	private final SelectList selectList;
	private final From from;
	private final long limit;
	private final long offset;
	private final List<Join> joins;
	private final @Nullable Where where;
	private final List<OrderByField> orderBy;
	private final @Nullable LockMode lockMode;

	DefaultSelect(boolean distinct, List<Expression> selectList, List<TableLike> from, long limit, long offset,
			List<Join> joins, @Nullable Condition where, List<OrderByField> orderBy, @Nullable LockMode lockMode) {

		this.distinct = distinct;
		this.selectList = new SelectList(new ArrayList<>(selectList));
		this.from = new From(new ArrayList<>(from));
		this.limit = limit;
		this.offset = offset;
		this.joins = new ArrayList<>(joins);
		this.orderBy = Collections.unmodifiableList(new ArrayList<>(orderBy));
		this.where = where != null ? new Where(where) : null;
		this.lockMode = lockMode;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.Select#getFrom()
	 */
	@Override
	public From getFrom() {
		return this.from;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.Select#getOrderBy()
	 */
	@Override
	public List<OrderByField> getOrderBy() {
		return this.orderBy;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.Select#getLimit()
	 */
	@Override
	public OptionalLong getLimit() {
		return limit == -1 ? OptionalLong.empty() : OptionalLong.of(limit);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.Select#getOffset()
	 */
	@Override
	public OptionalLong getOffset() {
		return offset == -1 ? OptionalLong.empty() : OptionalLong.of(offset);
	}

	@Override
	public boolean isDistinct() {
		return distinct;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.Select#getLockMode()
	 */
	@Nullable
	@Override
	public LockMode getLockMode() {
		return lockMode;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.Visitable#visit(org.springframework.data.relational.core.sql.Visitor)
	 */
	@Override
	public void visit(Visitor visitor) {

		Assert.notNull(visitor, "Visitor must not be null!");

		visitor.enter(this);

		selectList.visit(visitor);
		from.visit(visitor);
		joins.forEach(it -> it.visit(visitor));

		visitIfNotNull(where, visitor);

		orderBy.forEach(it -> it.visit(visitor));

		visitor.leave(this);
	}

	private void visitIfNotNull(@Nullable Visitable visitable, Visitor visitor) {

		if (visitable != null) {
			visitable.visit(visitor);
		}
	}
}
