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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default {@link InsertBuilder} implementation.
 *
 * @author Mark Paluch
 * @since 1.1
 */
class DefaultInsertBuilder
		implements InsertBuilder, InsertBuilder.InsertIntoColumnsAndValuesWithBuild, InsertBuilder.InsertValuesWithBuild {

	private @Nullable Table into;
	private List<Column> columns = new ArrayList<>();
	private List<Expression> values = new ArrayList<>();

	@Override
	public InsertIntoColumnsAndValuesWithBuild into(Table table) {

		Assert.notNull(table, "Insert Into Table must not be null");

		this.into = table;
		return this;
	}

	@Override
	public InsertIntoColumnsAndValuesWithBuild column(Column column) {

		Assert.notNull(column, "Column must not be null");

		this.columns.add(column);

		return this;
	}

	@Override
	public InsertIntoColumnsAndValuesWithBuild columns(Column... columns) {

		Assert.notNull(columns, "Columns must not be null");

		return columns(Arrays.asList(columns));
	}

	@Override
	public InsertIntoColumnsAndValuesWithBuild columns(Collection<Column> columns) {

		Assert.notNull(columns, "Columns must not be null");

		this.columns.addAll(columns);

		return this;
	}

	@Override
	public InsertValuesWithBuild value(Expression value) {

		Assert.notNull(value, "Value must not be null");

		this.values.add(value);

		return this;
	}

	@Override
	public InsertValuesWithBuild values(Expression... values) {

		Assert.notNull(values, "Values must not be null");

		return values(Arrays.asList(values));
	}

	@Override
	public InsertValuesWithBuild values(Collection<? extends Expression> values) {

		Assert.notNull(values, "Values must not be null");

		this.values.addAll(values);

		return this;
	}

	@Override
	public Insert build() {
		return new DefaultInsert(this.into, this.columns, this.values);
	}
}
