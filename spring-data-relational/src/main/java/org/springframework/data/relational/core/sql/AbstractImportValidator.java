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

	@Override
	public void leave(Visitable segment) {}

	/**
	 * {@link Visitor} that skips sub-{@link Select} and collects columns within a {@link Where} clause.
	 */
	class SubselectFilteringWhereVisitor implements Visitor {

		private @Nullable Select selectFilter;

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

		@Override
		public void leave(Visitable segment) {

			if (this.selectFilter == segment) {
				this.selectFilter = null;
			}
		}
	}
}
