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

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default {@link Delete} implementation.
 *
 * @author Mark Paluch
 * @since 1.1
 */
class DefaultDelete implements Delete {

	private final From from;
	private final @Nullable Where where;

	DefaultDelete(Table table, @Nullable Condition where) {

		this.from = new From(table);
		this.where = where != null ? new Where(where) : null;
	}

	@Override
	public void visit(Visitor visitor) {

		Assert.notNull(visitor, "Visitor must not be null");

		visitor.enter(this);

		from.visit(visitor);

		if (where != null) {
			where.visit(visitor);
		}

		visitor.leave(this);
	}

	@Override
	public String toString() {

		StringBuilder builder = new StringBuilder();

		builder.append("DELETE ").append(this.from);

		if (this.where != null) {
			builder.append(' ').append(this.where);
		}

		return builder.toString();
	}
}
