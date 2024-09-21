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
package org.springframework.data.relational.core.dialect;

import java.util.OptionalLong;
import java.util.function.Function;

import org.springframework.data.domain.Sort;
import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.data.relational.core.sql.LockOptions;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.render.SelectRenderContext;

/**
 * Base class for {@link Dialect} implementations.
 *
 * @author Mark Paluch
 * @author Myeonghyeon Lee
 * @author Chirag Tailor
 * @since 1.1
 */
public abstract class AbstractDialect implements Dialect {

	@Override
	public SelectRenderContext getSelectContext() {

		Function<Select, ? extends CharSequence> afterFromTable = getAfterFromTable();
		Function<Select, ? extends CharSequence> afterOrderBy = getAfterOrderBy();

		return new DialectSelectRenderContext(afterFromTable, afterOrderBy, orderByNullHandling());
	}

	/**
	 * Returns a {@link Function afterFromTable Function}. Typically used for table hint for SQL Server.
	 *
	 * @return the {@link Function} called on {@code afterFromTable}.
	 */
	protected Function<Select, CharSequence> getAfterFromTable() {

		Function<Select, ? extends CharSequence> afterFromTable = select -> "";

		LockClause lockClause = lock();
		if (lockClause.getClausePosition() == LockClause.Position.AFTER_FROM_TABLE) {
			afterFromTable = new LockRenderFunction(lockClause);
		}

		return afterFromTable.andThen(PrependWithLeadingWhitespace.INSTANCE);
	}

	/**
	 * Returns a {@link Function afterOrderBy Function}. Typically used for pagination.
	 *
	 * @return the {@link Function} called on {@code afterOrderBy}.
	 */
	protected Function<Select, CharSequence> getAfterOrderBy() {

		Function<Select, ? extends CharSequence> afterOrderByLimit = getAfterOrderByLimit();
		Function<Select, ? extends CharSequence> afterOrderByLock = getAfterOrderByLock();

		return select -> String.valueOf(afterOrderByLimit.apply(select)) + afterOrderByLock.apply(select);
	}

	private Function<Select, ? extends CharSequence> getAfterOrderByLimit() {
		LimitClause limit = limit();

		if (limit.getClausePosition() == LimitClause.Position.AFTER_ORDER_BY) {
			return new AfterOrderByLimitRenderFunction(limit) //
					.andThen(PrependWithLeadingWhitespace.INSTANCE);
		} else {
			throw new UnsupportedOperationException(String.format("Clause position %s not supported!", limit));
		}
	}

	private Function<Select, ? extends CharSequence> getAfterOrderByLock() {
		LockClause lock = lock();

		Function<Select, ? extends CharSequence> afterOrderByLock = select -> "";

		if (lock.getClausePosition() == LockClause.Position.AFTER_ORDER_BY) {
			afterOrderByLock = new LockRenderFunction(lock);
		}

		return afterOrderByLock.andThen(PrependWithLeadingWhitespace.INSTANCE);
	}

	/**
	 * {@link SelectRenderContext} derived from {@link Dialect} specifics.
	 */
	static class DialectSelectRenderContext implements SelectRenderContext {

		private final Function<Select, ? extends CharSequence> afterFromTable;
		private final Function<Select, ? extends CharSequence> afterOrderBy;
		private final OrderByNullPrecedence orderByNullPrecedence;

		DialectSelectRenderContext(Function<Select, ? extends CharSequence> afterFromTable,
				Function<Select, ? extends CharSequence> afterOrderBy, OrderByNullPrecedence orderByNullPrecedence) {

			this.afterFromTable = afterFromTable;
			this.afterOrderBy = afterOrderBy;
			this.orderByNullPrecedence = orderByNullPrecedence;
		}

		@Override
		public Function<Select, ? extends CharSequence> afterFromTable() {
			return afterFromTable;
		}

		@Override
		public Function<Select, ? extends CharSequence> afterOrderBy(boolean hasOrderBy) {
			return afterOrderBy;
		}

		@Override
		public String evaluateOrderByNullHandling(Sort.NullHandling nullHandling) {
			return orderByNullPrecedence.evaluate(nullHandling);
		}
	}

	/**
	 * After {@code ORDER BY} function rendering the {@link LimitClause}.
	 */
	static class AfterOrderByLimitRenderFunction implements Function<Select, CharSequence> {

		private final LimitClause clause;

		public AfterOrderByLimitRenderFunction(LimitClause clause) {
			this.clause = clause;
		}

		@Override
		public CharSequence apply(Select select) {

			OptionalLong limit = select.getLimit();
			OptionalLong offset = select.getOffset();

			if (limit.isPresent() && offset.isPresent()) {
				return clause.getLimitOffset(limit.getAsLong(), offset.getAsLong());
			}

			if (limit.isPresent()) {
				return clause.getLimit(limit.getAsLong());
			}

			if (offset.isPresent()) {
				return clause.getOffset(offset.getAsLong());
			}

			return "";
		}
	}

	/**
	 * {@code LOCK} function rendering the {@link LockClause}.
	 */
	static class LockRenderFunction implements Function<Select, CharSequence> {

		private final LockClause clause;

		public LockRenderFunction(LockClause clause) {
			this.clause = clause;
		}

		@Override
		public CharSequence apply(Select select) {

			LockMode lockMode = select.getLockMode();

			if (lockMode == null) {
				return "";
			}

			return clause.getLock(new LockOptions(lockMode, select.getFrom()));
		}
	}

	/**
	 * Prepends a non-empty rendering result with a leading whitespace,
	 */
	enum PrependWithLeadingWhitespace implements Function<CharSequence, CharSequence> {

		INSTANCE;

		@Override
		public CharSequence apply(CharSequence charSequence) {

			if (charSequence.isEmpty()) {
				return charSequence;
			}

			return " " + charSequence;
		}
	}
}
