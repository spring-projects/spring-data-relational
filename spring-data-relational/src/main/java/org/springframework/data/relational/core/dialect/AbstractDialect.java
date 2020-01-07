/*
 * Copyright 2019-2020 the original author or authors.
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

import lombok.RequiredArgsConstructor;

import java.util.OptionalLong;
import java.util.function.Function;

import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.render.SelectRenderContext;

/**
 * Base class for {@link Dialect} implementations.
 *
 * @author Mark Paluch
 * @since 1.1
 */
public abstract class AbstractDialect implements Dialect {

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.dialect.Dialect#getSelectContext()
	 */
	@Override
	public SelectRenderContext getSelectContext() {

		Function<Select, ? extends CharSequence> afterOrderBy = getAfterOrderBy();

		return new DialectSelectRenderContext(afterOrderBy);
	}

	/**
	 * Returns a {@link Function afterOrderBy Function}. Typically used for pagination.
	 *
	 * @return the {@link Function} called on {@code afterOrderBy}.
	 */
	protected Function<Select, CharSequence> getAfterOrderBy() {

		Function<Select, ? extends CharSequence> afterOrderBy;

		LimitClause limit = limit();

		switch (limit.getClausePosition()) {

			case AFTER_ORDER_BY:
				afterOrderBy = new AfterOrderByLimitRenderFunction(limit);
				break;

			default:
				throw new UnsupportedOperationException(String.format("Clause position %s not supported!", limit));
		}

		return afterOrderBy.andThen(PrependWithLeadingWhitespace.INSTANCE);
	}

	/**
	 * {@link SelectRenderContext} derived from {@link Dialect} specifics.
	 */
	class DialectSelectRenderContext implements SelectRenderContext {

		private final Function<Select, ? extends CharSequence> afterOrderBy;

		DialectSelectRenderContext(Function<Select, ? extends CharSequence> afterOrderBy) {
			this.afterOrderBy = afterOrderBy;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.render.SelectRenderContext#afterOrderBy(boolean)
		 */
		@Override
		public Function<Select, ? extends CharSequence> afterOrderBy(boolean hasOrderBy) {
			return afterOrderBy;
		}
	}

	/**
	 * After {@code ORDER BY} function rendering the {@link LimitClause}.
	 */
	@RequiredArgsConstructor
	static class AfterOrderByLimitRenderFunction implements Function<Select, CharSequence> {

		private final LimitClause clause;

		/*
		 * (non-Javadoc)
		 * @see java.util.function.Function#apply(java.lang.Object)
		 */
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
	 * Prepends a non-empty rendering result with a leading whitespace,
	 */
	@RequiredArgsConstructor
	enum PrependWithLeadingWhitespace implements Function<CharSequence, CharSequence> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see java.util.function.Function#apply(java.lang.Object)
		 */
		@Override
		public CharSequence apply(CharSequence charSequence) {

			if (charSequence.length() == 0) {
				return charSequence;
			}

			return " " + charSequence;
		}
	}
}
