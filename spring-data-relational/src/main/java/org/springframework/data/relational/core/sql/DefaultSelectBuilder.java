/*
 * Copyright 2019-2021 the original author or authors.
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
	private List<Expression> selectList = new ArrayList<>();
	private List<TableLike> from = new ArrayList<>();
	private long limit = -1;
	private long offset = -1;
	private List<Join> joins = new ArrayList<>();
	private @Nullable Condition where;
	private List<OrderByField> orderBy = new ArrayList<>();
	private @Nullable LockMode lockMode;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder#top(int)
	 */
	@Override
	public SelectBuilder top(int count) {

		limit = count;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder#select(org.springframework.data.relational.core.sql.Expression)
	 */
	@Override
	public DefaultSelectBuilder select(Expression expression) {
		selectList.add(expression);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder#select(org.springframework.data.relational.core.sql.Expression[])
	 */
	@Override
	public DefaultSelectBuilder select(Expression... expressions) {
		selectList.addAll(Arrays.asList(expressions));
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder#select(java.util.Collection)
	 */
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectFrom#from(java.lang.String)
	 */
	@Override
	public SelectFromAndJoin from(String table) {
		return from(Table.create(table));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectAndFrom#from(org.springframework.data.relational.core.sql.Table)
	 */
	@Override
	public SelectFromAndJoin from(TableLike table) {
		from.add(table);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectAndFrom#from(org.springframework.data.relational.core.sql.Table[])
	 */
	@Override
	public SelectFromAndJoin from(TableLike... tables) {
		from.addAll(Arrays.asList(tables));
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectAndFrom#from(java.util.Collection)
	 */
	@Override
	public SelectFromAndJoin from(Collection<? extends TableLike> tables) {
		from.addAll(tables);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectFromAndJoin#limitOffset(long, long)
	 */
	@Override
	public SelectFromAndJoin limitOffset(long limit, long offset) {
		this.limit = limit;
		this.offset = offset;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectFromAndJoin#limit(long)
	 */
	@Override
	public SelectFromAndJoin limit(long limit) {
		this.limit = limit;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectFromAndJoin#offset(long)
	 */
	@Override
	public SelectFromAndJoin offset(long offset) {
		this.offset = offset;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectFromAndOrderBy#orderBy(org.springframework.data.relational.core.sql.OrderByField[])
	 */
	@Override
	public DefaultSelectBuilder orderBy(OrderByField... orderByFields) {

		this.orderBy.addAll(Arrays.asList(orderByFields));

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectFromAndOrderBy#orderBy(java.util.Collection)
	 */
	@Override
	public DefaultSelectBuilder orderBy(Collection<? extends OrderByField> orderByFields) {

		this.orderBy.addAll(orderByFields);

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectFromAndOrderBy#orderBy(org.springframework.data.relational.core.sql.Column[])
	 */
	@Override
	public DefaultSelectBuilder orderBy(Column... columns) {

		for (Column column : columns) {
			this.orderBy.add(OrderByField.from(column));
		}

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectWhere#where(org.springframework.data.relational.core.sql.Condition)
	 */
	@Override
	public SelectWhereAndOr where(Condition condition) {

		where = condition;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectWhereAndOr#and(org.springframework.data.relational.core.sql.Condition)
	 */
	@Override
	public SelectWhereAndOr and(Condition condition) {

		where = where.and(condition);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectWhereAndOr#or(org.springframework.data.relational.core.sql.Condition)
	 */
	@Override
	public SelectWhereAndOr or(Condition condition) {

		where = where.or(condition);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectJoin#join(java.lang.String)
	 */
	@Override
	public SelectOn join(String table) {
		return join(Table.create(table));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectJoin#join(org.springframework.data.relational.core.sql.Table)
	 */
	@Override
	public SelectOn join(TableLike table) {
		return new JoinBuilder(table, this);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectJoin#join(org.springframework.data.relational.core.sql.Table)
	 */
	@Override
	public SelectOn leftOuterJoin(TableLike table) {
		return new JoinBuilder(table, this, JoinType.LEFT_OUTER_JOIN);
	}

	public DefaultSelectBuilder join(Join join) {
		this.joins.add(join);

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectLock#lock(org.springframework.data.relational.core.sql.LockMode)
	 */
	@Override
	public SelectLock lock(LockMode lockMode) {

		this.lockMode = lockMode;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.SelectBuilder.BuildSelect#build()
	 */
	@Override
	public Select build() {

		DefaultSelect select = new DefaultSelect(distinct, selectList, from, limit, offset, joins, where, orderBy,
				lockMode);
		SelectValidator.validate(select);
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

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectOn#on(org.springframework.data.relational.core.sql.Expression)
		 */
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

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectOnConditionComparison#equals(org.springframework.data.relational.core.sql.Expression)
		 */
		@Override
		public JoinBuilder equals(Expression column) {
			this.to = column;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectOnCondition#and(org.springframework.data.relational.core.sql.Expression)
		 */
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

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectOrdered#orderBy(org.springframework.data.relational.core.sql.OrderByField[])
		 */
		@Override
		public SelectOrdered orderBy(OrderByField... orderByFields) {
			selectBuilder.join(finishJoin());
			return selectBuilder.orderBy(orderByFields);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectOrdered#orderBy(java.util.Collection)
		 */
		@Override
		public SelectOrdered orderBy(Collection<? extends OrderByField> orderByFields) {
			selectBuilder.join(finishJoin());
			return selectBuilder.orderBy(orderByFields);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectOrdered#orderBy(org.springframework.data.relational.core.sql.Column[])
		 */
		@Override
		public SelectOrdered orderBy(Column... columns) {
			selectBuilder.join(finishJoin());
			return selectBuilder.orderBy(columns);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectWhere#where(org.springframework.data.relational.core.sql.Condition)
		 */
		@Override
		public SelectWhereAndOr where(Condition condition) {
			selectBuilder.join(finishJoin());
			return selectBuilder.where(condition);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectJoin#join(java.lang.String)
		 */
		@Override
		public SelectOn join(String table) {
			selectBuilder.join(finishJoin());
			return selectBuilder.join(table);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectJoin#join(org.springframework.data.relational.core.sql.Table)
		 */
		@Override
		public SelectOn join(TableLike table) {
			selectBuilder.join(finishJoin());
			return selectBuilder.join(table);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectJoin#leftOuterJoin(org.springframework.data.relational.core.sql.Table)
		 */
		@Override
		public SelectOn leftOuterJoin(TableLike table) {
			selectBuilder.join(finishJoin());
			return selectBuilder.leftOuterJoin(table);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectFromAndJoinCondition#limitOffset(long, long)
		 */
		@Override
		public SelectFromAndJoin limitOffset(long limit, long offset) {
			selectBuilder.join(finishJoin());
			return selectBuilder.limitOffset(limit, offset);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectFromAndJoinCondition#limit(long)
		 */
		@Override
		public SelectFromAndJoin limit(long limit) {
			selectBuilder.join(finishJoin());
			return selectBuilder.limit(limit);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectFromAndJoinCondition#offset(long)
		 */
		@Override
		public SelectFromAndJoin offset(long offset) {
			selectBuilder.join(finishJoin());
			return selectBuilder.offset(offset);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.SelectBuilder.SelectLock#lock(org.springframework.data.relational.core.sql.LockMode)
		 */
		@Override
		public SelectLock lock(LockMode lockMode) {
			selectBuilder.join(finishJoin());
			return selectBuilder.lock(lockMode);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.SelectBuilder.BuildSelect#build()
		 */
		@Override
		public Select build() {
			selectBuilder.join(finishJoin());
			return selectBuilder.build();
		}
	}
}
