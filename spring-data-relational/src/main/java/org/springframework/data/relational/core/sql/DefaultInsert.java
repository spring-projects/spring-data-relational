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

import java.util.ArrayList;
import java.util.List;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Default {@link Insert} implementation.
 *
 * @author Mark Paluch
 * @since 1.1
 */
class DefaultInsert implements Insert {

	private final Into into;
	private final List<Column> columns;
	private final Values values;

	DefaultInsert(@Nullable Table into, List<Column> columns, List<Expression> values) {
		this.into = new Into(into);
		this.columns = new ArrayList<>(columns);
		this.values = new Values(new ArrayList<>(values));
	}

	@Override
	public void visit(Visitor visitor) {

		Assert.notNull(visitor, "Visitor must not be null");

		visitor.enter(this);

		into.visit(visitor);
		columns.forEach(it -> it.visit(visitor));
		values.visit(visitor);

		visitor.leave(this);
	}

	@Override
	public String toString() {

		StringBuilder builder = new StringBuilder();

		builder.append("INSERT ").append(this.into);

		if (!this.columns.isEmpty()) {
			builder.append(" (").append(StringUtils.collectionToDelimitedString(this.columns, ", ")).append(")");
		}

		builder.append(" ").append(this.values);

		return builder.toString();
	}
}
