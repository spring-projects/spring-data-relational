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

import org.springframework.data.relational.core.dialect.Dialect;
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
	private final List<Column> columns = new ArrayList<>();
	private final List<Expression> values = new ArrayList<>();
	private Dialect dialect;
	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.InsertBuilder#into(org.springframework.data.relational.core.sql.Table)
	 */
	@Override
	public InsertIntoColumnsAndValuesWithBuild into(Table table) {

		Assert.notNull(table, "Insert Into Table must not be null!");

		this.into = table;
		return this;
	}

	@Override
	public InsertBuilder dialect(Dialect dialect) {

		Assert.notNull(dialect, "Dialect provided for insert must not be null");

		this.dialect = dialect;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.InsertBuilder.InsertIntoColumnsAndValues#column(org.springframework.data.relational.core.sql.Column)
	 */
	@Override
	public InsertIntoColumnsAndValuesWithBuild column(Column column) {

		Assert.notNull(column, "Column must not be null!");

		this.columns.add(column);

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.InsertBuilder.InsertIntoColumnsAndValues#columns(org.springframework.data.relational.core.sql.Column[])
	 */
	@Override
	public InsertIntoColumnsAndValuesWithBuild columns(Column... columns) {

		Assert.notNull(columns, "Columns must not be null!");

		return columns(Arrays.asList(columns));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.InsertBuilder.InsertIntoColumnsAndValues#columns(java.util.Collection)
	 */
	@Override
	public InsertIntoColumnsAndValuesWithBuild columns(Collection<Column> columns) {

		Assert.notNull(columns, "Columns must not be null!");

		this.columns.addAll(columns);

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.InsertBuilder.InsertIntoColumnsAndValuesWithBuild#value(org.springframework.data.relational.core.sql.Expression)
	 */
	@Override
	public InsertValuesWithBuild value(Expression value) {

		Assert.notNull(value, "Value must not be null!");

		this.values.add(value);

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.InsertBuilder.InsertIntoColumnsAndValuesWithBuild#values(org.springframework.data.relational.core.sql.Expression[])
	 */
	@Override
	public InsertValuesWithBuild values(Expression... values) {

		Assert.notNull(values, "Values must not be null!");

		return values(Arrays.asList(values));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.InsertBuilder.InsertIntoColumnsAndValuesWithBuild#values(java.util.Collection)
	 */
	@Override
	public InsertValuesWithBuild values(Collection<? extends Expression> values) {

		Assert.notNull(values, "Values must not be null!");

		this.values.addAll(values);

		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.InsertBuilder.BuildInsert#build()
	 */
	@Override
	public Insert build() {
		return new DefaultInsert(this.into, this.columns, this.values, dialect);
	}
}
