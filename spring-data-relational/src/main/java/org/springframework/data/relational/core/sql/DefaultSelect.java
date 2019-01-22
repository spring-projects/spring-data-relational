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

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default {@link Select} implementation.
 *
 * @author Mark Paluch
 */
class DefaultSelect implements Select {

	private final @Nullable SelectTop top;
	private final List<Expression> selectList;
	private final From from;
	private final long limit;
	private final long offset;
	private final List<Join> joins;
	private final @Nullable Where where;
	private final List<OrderByField> orderBy;

	DefaultSelect(@Nullable SelectTop top, List<Expression> selectList, List<Table> from, long limit, long offset,
				  List<Join> joins, @Nullable Condition where, List<OrderByField> orderBy) {

		this.top = top;
		this.selectList = new ArrayList<>(selectList);
		this.from = new From(from);
		this.limit = limit;
		this.offset = offset;
		this.joins = new ArrayList<>(joins);
		this.orderBy = new ArrayList<>(orderBy);
		this.where = where != null ? new Where(where) : null;
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.Visitable#visit(org.springframework.data.relational.core.sql.Visitor)
	 */
	@Override
	public void visit(Visitor visitor) {

		Assert.notNull(visitor, "Visitor must not be null!");

		visitor.enter(this);

		visitIfNotNull(top, visitor);

		selectList.forEach(it -> it.visit(visitor));
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
