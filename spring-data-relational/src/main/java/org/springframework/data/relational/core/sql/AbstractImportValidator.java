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

import java.util.HashSet;
import java.util.Set;

import org.springframework.lang.Nullable;

/**
 * Validator for statements to import columns.
 *
 * @author Mark Paluch
 * @since 1.1
 */
abstract class AbstractImportValidator implements Visitor {

	Set<Table> requiredByWhere = new HashSet<>();
	Set<Table> from = new HashSet<>();
	@Nullable Visitable parent;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.Visitor#enter(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	public void enter(Visitable segment) {

		if (segment instanceof Table && parent instanceof From) {
			from.add((Table) segment);
		}

		if (segment instanceof Where) {
			segment.visit(new SubselectFilteringWhereVisitor());
		}

		if (segment instanceof Join || segment instanceof OrderByField || segment instanceof From
				|| segment instanceof Select || segment instanceof Where || segment instanceof SimpleFunction) {
			parent = segment;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.Visitor#leave(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	public void leave(Visitable segment) {}

	/**
	 * {@link Visitor} that skips sub-{@link Select} and collects columns within a {@link Where} clause.
	 */
	class SubselectFilteringWhereVisitor implements Visitor {

		private @Nullable Select selectFilter;

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.Visitor#enter(org.springframework.data.relational.core.sql.Visitable)
		 */
		@Override
		public void enter(Visitable segment) {

			if (selectFilter != null) {
				return;
			}

			if (segment instanceof Select) {
				this.selectFilter = (Select) segment;
				return;
			}

			if (segment instanceof Table) {
				requiredByWhere.add((Table) segment);
			}
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.Visitor#leave(org.springframework.data.relational.core.sql.Visitable)
		 */
		@Override
		public void leave(Visitable segment) {

			if (this.selectFilter == segment) {
				this.selectFilter = null;
			}
		}
	}
}
