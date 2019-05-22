/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.r2dbc.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.dialect.BindMarkers;
import org.springframework.data.r2dbc.mapping.SettableValue;
import org.springframework.data.r2dbc.query.Criteria;
import org.springframework.data.r2dbc.query.Update;
import org.springframework.lang.Nullable;

/**
 * Mapper for statement specifications to {@link PreparedOperation}. Statement mapping applies a
 * {@link org.springframework.data.r2dbc.dialect.R2dbcDialect}-specific transformation considering {@link BindMarkers}
 * and vendor-specific SQL differences.
 *
 * @author Mark Paluch
 */
public interface StatementMapper {

	/**
	 * Create a typed {@link StatementMapper} that considers type-specific mapping metadata.
	 *
	 * @param type must not be {@literal null}.
	 * @param <T>
	 * @return the typed {@link StatementMapper}.
	 */
	<T> TypedStatementMapper<T> forType(Class<T> type);

	/**
	 * Map a select specification to a {@link PreparedOperation}.
	 *
	 * @param selectSpec the insert operation definition, must not be {@literal null}.
	 * @return the {@link PreparedOperation} for {@link SelectSpec}.
	 */
	PreparedOperation<?> getMappedObject(SelectSpec selectSpec);

	/**
	 * Map a insert specification to a {@link PreparedOperation}.
	 *
	 * @param insertSpec the insert operation definition, must not be {@literal null}.
	 * @return the {@link PreparedOperation} for {@link InsertSpec}.
	 */
	PreparedOperation<?> getMappedObject(InsertSpec insertSpec);

	/**
	 * Map a update specification to a {@link PreparedOperation}.
	 *
	 * @param updateSpec the update operation definition, must not be {@literal null}.
	 * @return the {@link PreparedOperation} for {@link UpdateSpec}.
	 */
	PreparedOperation<?> getMappedObject(UpdateSpec updateSpec);

	/**
	 * Map a delete specification to a {@link PreparedOperation}.
	 *
	 * @param deleteSpec the update operation definition, must not be {@literal null}.
	 * @return the {@link PreparedOperation} for {@link DeleteSpec}.
	 */
	PreparedOperation<?> getMappedObject(DeleteSpec deleteSpec);

	/**
	 * Extension to {@link StatementMapper} that is associated with a type.
	 *
	 * @param <T>
	 */
	interface TypedStatementMapper<T> extends StatementMapper {}

	/**
	 * Create a {@code SELECT} specification for {@code table}.
	 *
	 * @param table
	 * @return the {@link SelectSpec}.
	 */
	default SelectSpec createSelect(String table) {
		return SelectSpec.create(table);
	}

	/**
	 * Create an {@code INSERT} specification for {@code table}.
	 *
	 * @param table
	 * @return the {@link InsertSpec}.
	 */
	default InsertSpec createInsert(String table) {
		return InsertSpec.create(table);
	}

	/**
	 * Create an {@code UPDATE} specification for {@code table}.
	 *
	 * @param table
	 * @return the {@link UpdateSpec}.
	 */
	default UpdateSpec createUpdate(String table, Update update) {
		return UpdateSpec.create(table, update);
	}

	/**
	 * Create a {@code DELETE} specification for {@code table}.
	 *
	 * @param table
	 * @return the {@link DeleteSpec}.
	 */
	default DeleteSpec createDelete(String table) {
		return DeleteSpec.create(table);
	}

	/**
	 * {@code SELECT} specification.
	 */
	class SelectSpec {

		private final String table;
		private final List<String> projectedFields;
		private final @Nullable Criteria criteria;
		private final Sort sort;
		private final Pageable page;

		protected SelectSpec(String table, List<String> projectedFields, @Nullable Criteria criteria, Sort sort,
				Pageable page) {
			this.table = table;
			this.projectedFields = projectedFields;
			this.criteria = criteria;
			this.sort = sort;
			this.page = page;
		}

		/**
		 * Create an {@code SELECT} specification for {@code table}.
		 *
		 * @param table
		 * @return the {@link SelectSpec}.
		 */
		public static SelectSpec create(String table) {
			return new SelectSpec(table, Collections.emptyList(), null, Sort.unsorted(), Pageable.unpaged());
		}

		/**
		 * Associate {@code projectedFields} with the select and create a new {@link SelectSpec}.
		 *
		 * @param projectedFields
		 * @return the {@link SelectSpec}.
		 */
		public SelectSpec withProjection(Collection<String> projectedFields) {

			List<String> fields = new ArrayList<>(this.projectedFields);
			fields.addAll(projectedFields);

			return new SelectSpec(this.table, fields, this.criteria, this.sort, this.page);
		}

		/**
		 * Associate a {@link Criteria} with the select and return a new {@link SelectSpec}.
		 *
		 * @param criteria
		 * @return the {@link SelectSpec}.
		 */
		public SelectSpec withCriteria(Criteria criteria) {
			return new SelectSpec(this.table, this.projectedFields, criteria, this.sort, this.page);
		}

		/**
		 * Associate {@link Sort} with the select and create a new {@link SelectSpec}.
		 *
		 * @param sort
		 * @return the {@link SelectSpec}.
		 */
		public SelectSpec withSort(Sort sort) {
			return new SelectSpec(this.table, this.projectedFields, this.criteria, sort, this.page);
		}

		/**
		 * Associate a {@link Pageable} with the select and create a new {@link SelectSpec}.
		 *
		 * @param page
		 * @return the {@link SelectSpec}.
		 */
		public SelectSpec withPage(Pageable page) {

			if (page.isPaged()) {

				Sort sort = page.getSort();

				return new SelectSpec(this.table, this.projectedFields, this.criteria, sort.isSorted() ? sort : this.sort,
						page);
			}

			return new SelectSpec(this.table, this.projectedFields, this.criteria, this.sort, page);
		}

		public String getTable() {
			return this.table;
		}

		public List<String> getProjectedFields() {
			return Collections.unmodifiableList(this.projectedFields);
		}

		@Nullable
		public Criteria getCriteria() {
			return this.criteria;
		}

		public Sort getSort() {
			return this.sort;
		}

		public Pageable getPage() {
			return this.page;
		}
	}

	/**
	 * {@code INSERT} specification.
	 */
	class InsertSpec {

		private final String table;
		private final Map<String, SettableValue> assignments;

		protected InsertSpec(String table, Map<String, SettableValue> assignments) {
			this.table = table;
			this.assignments = assignments;
		}

		/**
		 * Create an {@code INSERT} specification for {@code table}.
		 *
		 * @param table
		 * @return the {@link InsertSpec}.
		 */
		public static InsertSpec create(String table) {
			return new InsertSpec(table, Collections.emptyMap());
		}

		/**
		 * Associate a column with a {@link SettableValue} and create a new {@link InsertSpec}.
		 *
		 * @param column
		 * @param value
		 * @return the {@link InsertSpec}.
		 */
		public InsertSpec withColumn(String column, SettableValue value) {

			Map<String, SettableValue> values = new LinkedHashMap<>(this.assignments);
			values.put(column, value);

			return new InsertSpec(this.table, values);
		}

		public String getTable() {
			return this.table;
		}

		public Map<String, SettableValue> getAssignments() {
			return Collections.unmodifiableMap(this.assignments);
		}
	}

	/**
	 * {@code UPDATE} specification.
	 */
	class UpdateSpec {

		private final String table;
		private final Update update;

		private final @Nullable Criteria criteria;

		protected UpdateSpec(String table, Update update, @Nullable Criteria criteria) {

			this.table = table;
			this.update = update;
			this.criteria = criteria;
		}

		/**
		 * Create an {@code INSERT} specification for {@code table}.
		 *
		 * @param table
		 * @return the {@link InsertSpec}.
		 */
		public static UpdateSpec create(String table, Update update) {
			return new UpdateSpec(table, update, null);
		}

		/**
		 * Associate a {@link Criteria} with the update and return a new {@link UpdateSpec}.
		 *
		 * @param criteria
		 * @return the {@link UpdateSpec}.
		 */
		public UpdateSpec withCriteria(Criteria criteria) {
			return new UpdateSpec(this.table, this.update, criteria);
		}

		public String getTable() {
			return this.table;
		}

		public Update getUpdate() {
			return this.update;
		}

		@Nullable
		public Criteria getCriteria() {
			return this.criteria;
		}
	}

	/**
	 * {@code DELETE} specification.
	 */
	class DeleteSpec {

		private final String table;

		private final @Nullable Criteria criteria;

		protected DeleteSpec(String table, @Nullable Criteria criteria) {
			this.table = table;
			this.criteria = criteria;
		}

		/**
		 * Create an {@code DELETE} specification for {@code table}.
		 *
		 * @param table
		 * @return the {@link DeleteSpec}.
		 */
		public static DeleteSpec create(String table) {
			return new DeleteSpec(table, null);
		}

		/**
		 * Associate a {@link Criteria} with the delete and return a new {@link DeleteSpec}.
		 *
		 * @param criteria
		 * @return the {@link DeleteSpec}.
		 */
		public DeleteSpec withCriteria(Criteria criteria) {
			return new DeleteSpec(this.table, criteria);
		}

		public String getTable() {
			return this.table;
		}

		@Nullable
		public Criteria getCriteria() {
			return this.criteria;
		}
	}
}
