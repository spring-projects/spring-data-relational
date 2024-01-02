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

import java.util.Collection;

/**
 * Entry point to construct an {@link Insert} statement.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 * @see StatementBuilder
 */
public interface InsertBuilder {

	/**
	 * Declare a {@link Table} to {@code INSERT INTO}.
	 *
	 * @param table the table to {@code INSERT INTO} must not be {@literal null}.
	 * @return {@code this} builder.
	 * @see Into
	 * @see SQL#table(String)
	 */
	InsertIntoColumnsAndValuesWithBuild into(Table table);

	/**
	 * Interface exposing {@code WHERE} methods.
	 */
	interface InsertIntoColumnsAndValues extends InsertValues {

		/**
		 * Add a {@link Column} to the {@code INTO} column list. Calling this method multiple times will add the
		 * {@link Column} multiple times.
		 *
		 * @param column the column.
		 * @return {@code this} builder.
		 * @see Column
		 */
		InsertIntoColumnsAndValuesWithBuild column(Column column);

		/**
		 * Add a one or more {@link Column} to the {@code INTO} column list. Calling this method multiple times will add the
		 * {@link Column} multiple times.
		 *
		 * @param columns the columns.
		 * @return {@code this} builder.
		 * @see Column
		 */
		InsertIntoColumnsAndValuesWithBuild columns(Column... columns);

		/**
		 * Add a one or more {@link Column} to the {@code INTO} column list. Calling this method multiple times will add the
		 * {@link Column} multiple times.
		 *
		 * @param columns the columns.
		 * @return {@code this} builder.
		 * @see Column
		 */
		InsertIntoColumnsAndValuesWithBuild columns(Collection<Column> columns);
	}

	/**
	 * Interface exposing {@code value} methods to add values to the {@code INSERT} statement and the build method.
	 */
	interface InsertIntoColumnsAndValuesWithBuild extends InsertIntoColumnsAndValues, InsertValues, BuildInsert {

		/**
		 * Add a {@link Expression value} to the {@code VALUES} list. Calling this method multiple times will add a
		 * {@link Expression value} multiple times.
		 *
		 * @param value the value to use.
		 * @return {@code this} builder.
		 * @see Column
		 */
		@Override
		InsertValuesWithBuild value(Expression value);

		/**
		 * Add one or more {@link Expression values} to the {@code VALUES} list. Calling this method multiple times will add
		 * a {@link Expression values} multiple times.
		 *
		 * @param values the values.
		 * @return {@code this} builder.
		 * @see Column
		 */
		@Override
		InsertValuesWithBuild values(Expression... values);

		/**
		 * Add one or more {@link Expression values} to the {@code VALUES} list. Calling this method multiple times will add
		 * a {@link Expression values} multiple times.
		 *
		 * @param values the values.
		 * @return {@code this} builder.
		 * @see Column
		 */
		@Override
		InsertValuesWithBuild values(Collection<? extends Expression> values);
	}

	/**
	 * Interface exposing {@code value} methods to add values to the {@code INSERT} statement and the build method.
	 */
	interface InsertValuesWithBuild extends InsertValues, BuildInsert {

		/**
		 * Add a {@link Expression value} to the {@code VALUES} list. Calling this method multiple times will add a
		 * {@link Expression value} multiple times.
		 *
		 * @param value the value to use.
		 * @return {@code this} builder.
		 * @see Column
		 */
		@Override
		InsertValuesWithBuild value(Expression value);

		/**
		 * Add one or more {@link Expression values} to the {@code VALUES} list. Calling this method multiple times will add
		 * a {@link Expression values} multiple times.
		 *
		 * @param values the values.
		 * @return {@code this} builder.
		 * @see Column
		 */
		@Override
		InsertValuesWithBuild values(Expression... values);

		/**
		 * Add one or more {@link Expression values} to the {@code VALUES} list. Calling this method multiple times will add
		 * a {@link Expression values} multiple times.
		 *
		 * @param values the values.
		 * @return {@code this} builder.
		 * @see Column
		 */
		@Override
		InsertValuesWithBuild values(Collection<? extends Expression> values);
	}

	/**
	 * Interface exposing {@code value} methods to add values to the {@code INSERT} statement.
	 */
	interface InsertValues {

		/**
		 * Add a {@link Expression value} to the {@code VALUES} list. Calling this method multiple times will add a
		 * {@link Expression value} multiple times.
		 *
		 * @param value the value to use.
		 * @return {@code this} builder.
		 * @see Column
		 */
		InsertValuesWithBuild value(Expression value);

		/**
		 * Add one or more {@link Expression values} to the {@code VALUES} list. Calling this method multiple times will add
		 * a {@link Expression values} multiple times.
		 *
		 * @param values the values.
		 * @return {@code this} builder.
		 * @see Column
		 */
		InsertValuesWithBuild values(Expression... values);

		/**
		 * Add one or more {@link Expression values} to the {@code VALUES} list. Calling this method multiple times will add
		 * a {@link Expression values} multiple times.
		 *
		 * @param values the values.
		 * @return {@code this} builder.
		 * @see Column
		 */
		InsertValuesWithBuild values(Collection<? extends Expression> values);
	}

	/**
	 * Interface exposing the {@link Insert} build method.
	 */
	interface BuildInsert {

		/**
		 * Build the {@link Insert} statement.
		 *
		 * @return the build and immutable {@link Insert} statement.
		 */
		Insert build();
	}
}
