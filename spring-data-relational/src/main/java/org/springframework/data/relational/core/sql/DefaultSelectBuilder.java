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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.springframework.data.relational.core.sql.Join.JoinType;
import org.springframework.data.relational.core.sql.SelectBuilder.SelectAndFrom;
import org.springframework.data.relational.core.sql.SelectBuilder.SelectFromAndJoin;
import org.springframework.data.relational.core.sql.SelectBuilder.SelectWhereAndOr;
import org.springframework.lang.Nullable;

/**
 * Default {@link SelectBuilder} implementation.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Myeonghyeon Lee
 * @since 1.1
 */
class DefaultSelectBuilder implements SelectBuilder, SelectAndFrom, SelectFromAndJoin, SelectWhereAndOr {

	private boolean distinct = false;
	private final List<Expression> selectList = new ArrayList<>();
	private final List<TableLike> from = new ArrayList<>();
	private long limit = -1;
	private long offset = -1;
	private final List<Join> joins = new ArrayList<>();
	private @Nullable Condition where;
	private final List<OrderByField> orderBy = new ArrayList<>();
	private @Nullable LockMode lockMode;

	@Override
	public SelectBuilder top(int count) {

		limit = count;
		return this;
	}

	@Override
	public DefaultSelectBuilder select(Expression expression) {
		selectList.add(expression);
		return this;
	}

	@Override
	public DefaultSelectBuilder select(Expression... expressions) {
		selectList.addAll(Arrays.asList(expressions));
		return this;
	}

	@Override
	public DefaultSelectBuilder select(Collection<? extends Expression> expressions) {
		selectList.addAll(expressions);
		return this;
	}

	@Override
	public DefaultSelectBuilder distinct() {
		distinct = true;
		return this;
	}

	@Override
	public SelectFromAndJoin from(String table) {
		return from(Table.create(table));
	}

	@Override
	public SelectFromAndJoin from(TableLike table) {
		from.add(table);
		return this;
	}

	@Override
	public SelectFromAndJoin from(TableLike... tables) {
		from.addAll(Arrays.asList(tables));
		return this;
	}

	@Override
	public SelectFromAndJoin from(Collection<? extends TableLike> tables) {
		from.addAll(tables);
		return this;
	}

	@Override
	public SelectFromAndJoin limitOffset(long limit, long offset) {
		this.limit = limit;
		this.offset = offset;
		return this;
	}

	@Override
	public SelectFromAndJoin limit(long limit) {
		this.limit = limit;
		return this;
	}

	@Override
	public SelectFromAndJoin offset(long offset) {
		this.offset = offset;
		return this;
	}

	@Override
	public DefaultSelectBuilder orderBy(OrderByField... orderByFields) {

		this.orderBy.addAll(Arrays.asList(orderByFields));

		return this;
	}

	@Override
	public DefaultSelectBuilder orderBy(Collection<? extends OrderByField> orderByFields) {

		this.orderBy.addAll(orderByFields);

		return this;
	}

	@Override
	public DefaultSelectBuilder orderBy(Expression... columns) {

		for (Expression column : columns) {
			this.orderBy.add(OrderByField.from(column));
		}

		return this;
	}

	@Override
	public SelectWhereAndOr where(Condition condition) {

		where = condition;
		return this;
	}

	@Override
	public SelectWhereAndOr and(Condition condition) {

		where = where.and(condition);
		return this;
	}

	@Override
	public SelectWhereAndOr or(Condition condition) {

		where = where.or(condition);
		return this;
	}

	@Override
	public SelectOn join(String table) {
		return join(Table.create(table));
	}

	@Override
	public SelectOn join(TableLike table) {
		return new JoinBuilder(table, this);
	}

	@Override
	public SelectOn leftOuterJoin(TableLike table) {
		return new JoinBuilder(table, this, JoinType.LEFT_OUTER_JOIN);
	}

	@Override
	public SelectOn join(TableLike table, JoinType joinType) {
		return new JoinBuilder(table, this, joinType);
	}

	public DefaultSelectBuilder join(Join join) {
		this.joins.add(join);

		return this;
	}

	@Override
	public SelectLock lock(LockMode lockMode) {

		this.lockMode = lockMode;
		return this;
	}

	@Override
	public Select build(boolean validate) {

		DefaultSelect select = new DefaultSelect(distinct, selectList, from, limit, offset, joins, where, orderBy,
				lockMode);

		if (validate) {
			SelectValidator.validate(select);
		}
		return select;
	}

	/**
	 * Delegation builder to construct JOINs.
	 */
	static class JoinBuilder implements SelectOn, SelectOnConditionComparison, SelectFromAndJoinCondition {

		private final TableLike table;
		private final DefaultSelectBuilder selectBuilder;
		private final JoinType joinType;
		private @Nullable Expression from;
		private @Nullable Expression to;
		private @Nullable Condition condition;

		JoinBuilder(TableLike table, DefaultSelectBuilder selectBuilder, JoinType joinType) {

			this.table = table;
			this.selectBuilder = selectBuilder;
			this.joinType = joinType;
		}

		JoinBuilder(TableLike table, DefaultSelectBuilder selectBuilder) {
			this(table, selectBuilder, JoinType.JOIN);
		}

		@Override
		public SelectOnConditionComparison on(Expression column) {

			this.from = column;
			return this;
		}

		@Override
		public SelectFromAndJoinCondition on(Condition condition) {

			if (this.condition == null) {
				this.condition = condition;
			} else {
				this.condition = this.condition.and(condition);
			}

			return this;
		}

		@Override
		public JoinBuilder equals(Expression column) {
			this.to = column;
			return this;
		}

		@Override
		public SelectOnConditionComparison and(Expression column) {

			finishCondition();
			this.from = column;
			return this;
		}

		private void finishCondition() {

			// Nothing to do if a complete join condition was used.
			if (from == null && to == null) {
				return;
			}

			Comparison comparison = Comparison.create(from, "=", to);

			if (condition == null) {
				condition = comparison;
			} else {
				condition = condition.and(comparison);
			}

		}

		private Join finishJoin() {
			finishCondition();
			return new Join(joinType, table, condition);
		}

		@Override
		public SelectOrdered orderBy(OrderByField... orderByFields) {
			selectBuilder.join(finishJoin());
			return selectBuilder.orderBy(orderByFields);
		}

		@Override
		public SelectOrdered orderBy(Collection<? extends OrderByField> orderByFields) {
			selectBuilder.join(finishJoin());
			return selectBuilder.orderBy(orderByFields);
		}

		@Override
		public SelectOrdered orderBy(Expression... columns) {
			selectBuilder.join(finishJoin());
			return selectBuilder.orderBy(columns);
		}

		@Override
		public SelectWhereAndOr where(Condition condition) {
			selectBuilder.join(finishJoin());
			return selectBuilder.where(condition);
		}

		@Override
		public SelectOn join(String table) {
			selectBuilder.join(finishJoin());
			return selectBuilder.join(table);
		}

		@Override
		public SelectOn join(TableLike table) {
			selectBuilder.join(finishJoin());
			return selectBuilder.join(table);
		}

		@Override
		public SelectOn leftOuterJoin(TableLike table) {
			selectBuilder.join(finishJoin());
			return selectBuilder.leftOuterJoin(table);
		}

		@Override
		public SelectOn join(TableLike table, JoinType joinType) {
			selectBuilder.join(finishJoin());
			return selectBuilder.join(table, joinType);
		}

		@Override
		public SelectFromAndJoin limitOffset(long limit, long offset) {
			selectBuilder.join(finishJoin());
			return selectBuilder.limitOffset(limit, offset);
		}

		@Override
		public SelectFromAndJoin limit(long limit) {
			selectBuilder.join(finishJoin());
			return selectBuilder.limit(limit);
		}

		@Override
		public SelectFromAndJoin offset(long offset) {
			selectBuilder.join(finishJoin());
			return selectBuilder.offset(offset);
		}

		@Override
		public SelectLock lock(LockMode lockMode) {
			selectBuilder.join(finishJoin());
			return selectBuilder.lock(lockMode);
		}

		@Override
		public Select build(boolean validate) {
			selectBuilder.join(finishJoin());
			return selectBuilder.build(validate);
		}
	}
}
