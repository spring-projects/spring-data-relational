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
package org.springframework.data.r2dbc.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.relational.core.dialect.Escaper;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.query.Update;
import org.springframework.data.relational.core.query.ValueFunction;
import org.springframework.data.relational.core.sql.AssignValue;
import org.springframework.data.relational.core.sql.Assignment;
import org.springframework.data.relational.core.sql.Assignments;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.r2dbc.core.binding.BindMarker;
import org.springframework.r2dbc.core.binding.BindMarkers;
import org.springframework.r2dbc.core.binding.Bindings;
import org.springframework.r2dbc.core.binding.MutableBindings;
import org.springframework.util.Assert;

/**
 * A subclass of {@link QueryMapper} that maps {@link Update} to update assignments.
 *
 * @author Mark Paluch
 */
public class UpdateMapper extends QueryMapper {

	/**
	 * Creates a new {@link QueryMapper} with the given {@link R2dbcConverter}.
	 *
	 * @param dialect must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public UpdateMapper(R2dbcDialect dialect, R2dbcConverter converter) {
		super(dialect, converter);
	}

	/**
	 * Map a {@link org.springframework.data.relational.core.query.Update} object to {@link BoundAssignments} and consider
	 * value/{@code NULL} {@link Bindings}.
	 *
	 * @param markers bind markers object, must not be {@literal null}.
	 * @param update update definition to map, must not be {@literal null}.
	 * @param table must not be {@literal null}.
	 * @param entity related {@link RelationalPersistentEntity}, can be {@literal null}.
	 * @return the mapped {@link BoundAssignments}.
	 * @since 1.1
	 */
	public BoundAssignments getMappedObject(BindMarkers markers, Update update, Table table,
			@Nullable RelationalPersistentEntity<?> entity) {
		return getMappedObject(markers, update.getAssignments(), table, entity);
	}

	/**
	 * Map a {@code assignments} object to {@link BoundAssignments} and consider value/{@code NULL} {@link Bindings}.
	 *
	 * @param markers bind markers object, must not be {@literal null}.
	 * @param assignments update/insert definition to map, must not be {@literal null}.
	 * @param table must not be {@literal null}.
	 * @param entity related {@link RelationalPersistentEntity}, can be {@literal null}.
	 * @return the mapped {@link BoundAssignments}.
	 */
	public BoundAssignments getMappedObject(BindMarkers markers, Map<SqlIdentifier, ? extends Object> assignments,
			Table table, @Nullable RelationalPersistentEntity<?> entity) {

		Assert.notNull(markers, "BindMarkers must not be null");
		Assert.notNull(assignments, "Assignments must not be null");
		Assert.notNull(table, "Table must not be null");

		MutableBindings bindings = new MutableBindings(markers);
		List<Assignment> result = new ArrayList<>();

		assignments.forEach((column, value) -> {
			Assignment assignment = getAssignment(column, value, bindings, table, entity);
			result.add(assignment);
		});

		return new BoundAssignments(bindings, result);
	}

	private Assignment getAssignment(SqlIdentifier columnName, Object value, MutableBindings bindings, Table table,
			@Nullable RelationalPersistentEntity<?> entity) {

		Field propertyField = createPropertyField(entity, columnName, getMappingContext());
		Column column = table.column(propertyField.getMappedColumnName());
		TypeInformation<?> actualType = propertyField.getTypeHint().getRequiredActualType();

		Object mappedValue;
		Class<?> typeHint;

		if (value instanceof Parameter parameter) {

			mappedValue = convertValue(parameter.getValue(), propertyField.getTypeHint());
			typeHint = getTypeHint(mappedValue, actualType.getType(), parameter);

		} else if (value instanceof ValueFunction<?> valueFunction) {

			mappedValue = valueFunction.map(v -> convertValue(v, propertyField.getTypeHint())).apply(Escaper.DEFAULT);

			if (mappedValue == null) {
				return Assignments.value(column, SQL.nullLiteral());
			}

			typeHint = actualType.getType();
		} else {

			mappedValue = convertValue(value, propertyField.getTypeHint());

			if (mappedValue == null) {
				return Assignments.value(column, SQL.nullLiteral());
			}

			typeHint = actualType.getType();
		}

		return createAssignment(column, mappedValue, typeHint, bindings);
	}

	private Assignment createAssignment(Column column, Object value, Class<?> type, MutableBindings bindings) {

		BindMarker bindMarker = bindings.nextMarker(column.getName().getReference());
		AssignValue assignValue = Assignments.value(column, SQL.bindMarker(bindMarker.getPlaceholder()));

		if (value == null) {
			bindings.bindNull(bindMarker, type);
		} else {
			bindings.bind(bindMarker, value);
		}

		return assignValue;
	}
}
