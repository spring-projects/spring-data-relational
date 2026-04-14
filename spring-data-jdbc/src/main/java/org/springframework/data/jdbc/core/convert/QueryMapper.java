/*
 * Copyright 2020-present the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import java.sql.JDBCType;
import java.sql.SQLType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;
import org.springframework.data.core.PropertyPath;
import org.springframework.data.core.PropertyReferenceException;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.core.mapping.JdbcValue;
import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.relational.core.query.CriteriaDefinition.Comparator;
import org.springframework.data.relational.core.query.ValueFunction;
import org.springframework.data.relational.core.sql.Aliased;
import org.springframework.data.relational.core.sql.AndCondition;
import org.springframework.data.relational.core.sql.AsteriskFromTable;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Conditions;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Expressions;
import org.springframework.data.relational.core.sql.Functions;
import org.springframework.data.relational.core.sql.OrderByField;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.SimpleFunction;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.TableLike;
import org.springframework.data.relational.domain.SqlSort;
import org.springframework.data.util.Pair;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

/**
 * Maps {@link CriteriaDefinition} and {@link Sort} objects considering mapping metadata and dialect-specific
 * conversion.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Yan Qiang
 * @author Mikhail Fedorov
 * @author Christoph Strobl
 * @since 3.0
 */
public class QueryMapper {

	private final JdbcConverter converter;
	private final MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> mappingContext;

	/**
	 * Creates a new {@link QueryMapper} with the given {@link JdbcConverter}.
	 *
	 * @param converter must not be {@literal null}.
	 */
	public QueryMapper(JdbcConverter converter) {

		Assert.notNull(converter, "JdbcConverter must not be null");

		this.converter = converter;
		this.mappingContext = converter.getMappingContext();
	}

	/**
	 * Map the {@link Sort} object to apply field name mapping using {@link RelationalPersistentEntity the type to read}.
	 *
	 * @param sort must not be {@literal null}.
	 * @param entity related {@link RelationalPersistentEntity}, can be {@literal null}.
	 * @return a List of {@link OrderByField} objects guaranteed to be not {@literal null}.
	 */
	public List<OrderByField> getMappedSort(Table table, Sort sort, @Nullable RelationalPersistentEntity<?> entity) {

		List<OrderByField> mappedOrder = new ArrayList<>();

		for (Sort.Order order : sort) {

			SqlSort.validate(order);

			List<OrderByField> simpleOrderByFields = createSimpleOrderByFields(table, entity, order);

			simpleOrderByFields.forEach(field -> {

				OrderByField orderBy = field.withNullHandling(order.getNullHandling());
				mappedOrder.add(order.isAscending() ? orderBy.asc() : orderBy.desc());
			});
		}

		return mappedOrder;
	}

	private List<OrderByField> createSimpleOrderByFields(Table table, @Nullable RelationalPersistentEntity<?> entity,
			Sort.Order order) {

		if (order instanceof SqlSort.SqlOrder sqlOrder && sqlOrder.isUnsafe()) {
			return List.of(OrderByField.from(Expressions.just(sqlOrder.getProperty())));
		}

		Field field = createPropertyField(entity, SqlIdentifier.unquoted(order.getProperty()), this.mappingContext);

		if (field.isEmbedded() && entity != null) {

			RelationalPersistentEntity<?> embeddedEntity = getMappingContext()
					.getRequiredPersistentEntity(field.getRequiredProperty());

			List<OrderByField> fields = new ArrayList<>();

			for (RelationalPersistentProperty embeddedProperty : embeddedEntity) {
				fields.addAll(createSimpleOrderByFields(table, embeddedEntity, order.withProperty(embeddedProperty.getName())));
			}

			return fields;
		}

		return List.of(OrderByField.from(table.column(field.getMappedColumnName())));
	}

	/**
	 * Map the {@link Expression} object to apply field name mapping using {@link RelationalPersistentEntity the type to
	 * read}.
	 *
	 * @param expression must not be {@literal null}.
	 * @param entity related {@link RelationalPersistentEntity}, can be {@literal null}.
	 * @return the mapped {@link Expression}. Guaranteed to be not {@literal null}.
	 */
	Expression getMappedObject(Expression expression, @Nullable RelationalPersistentEntity<?> entity) {

		if (entity == null || expression instanceof AsteriskFromTable) {
			return expression;
		}

		if (expression instanceof Column column) {

			Field field = createPropertyField(entity, column.getName());
			TableLike table = column.getRequiredTable();

			Column columnFromTable = table.column(field.getMappedColumnName());
			return column instanceof Aliased aliased ? columnFromTable.as(aliased.getAlias()) : columnFromTable;
		}

		if (expression instanceof SimpleFunction function) {

			List<Expression> arguments = function.getExpressions();
			List<Expression> mappedArguments = new ArrayList<>(arguments.size());

			for (Expression argument : arguments) {
				mappedArguments.add(getMappedObject(argument, entity));
			}

			SimpleFunction mappedFunction = SimpleFunction.create(function.getFunctionName(), mappedArguments);

			return function instanceof Aliased aliased ? mappedFunction.as(aliased.getAlias()) : mappedFunction;
		}

		throw new IllegalArgumentException(String.format("Cannot map %s", expression));
	}

	/**
	 * Map a {@link CriteriaDefinition} object into {@link Condition} and consider value/{@code NULL} bindings.
	 *
	 * @param parameterSource bind parameterSource object, must not be {@literal null}.
	 * @param criteria criteria definition to map, must not be {@literal null}.
	 * @param table must not be {@literal null}.
	 * @param entity related {@link RelationalPersistentEntity}, can be {@literal null}.
	 * @return the mapped {@link Condition}.
	 */
	public Condition getMappedObject(MapSqlParameterSource parameterSource, CriteriaDefinition criteria, Table table,
			@Nullable RelationalPersistentEntity<?> entity) {

		Assert.notNull(parameterSource, "MapSqlParameterSource must not be null");
		Assert.notNull(criteria, "CriteriaDefinition must not be null");
		Assert.notNull(table, "Table must not be null");

		if (criteria.isEmpty()) {
			throw new IllegalArgumentException("Cannot map empty Criteria");
		}

		return unroll(criteria, table, entity, parameterSource);
	}

	private Condition unroll(CriteriaDefinition criteria, Table table, @Nullable RelationalPersistentEntity<?> entity,
			MapSqlParameterSource parameterSource) {

		CriteriaDefinition current = criteria;

		// reverse unroll criteria chain
		Map<CriteriaDefinition, CriteriaDefinition> forwardChain = new HashMap<>();

		while (current.hasPrevious()) {

			CriteriaDefinition previous = current.getRequiredPrevious();
			forwardChain.put(previous, current);
			current = previous;
		}

		// perform the actual mapping
		Condition mapped = getCondition(current, parameterSource, table, entity);
		while (forwardChain.containsKey(current)) {

			CriteriaDefinition criterion = forwardChain.get(current);
			Condition result = null;

			Condition condition = getCondition(criterion, parameterSource, table, entity);
			if (condition != null) {
				result = combine(mapped, criterion.getCombinator(), condition);
			}

			if (result != null) {
				mapped = result;
			}
			current = criterion;
		}

		if (mapped == null) {
			throw new IllegalStateException("Cannot map empty Criteria");
		}

		return mapped;
	}

	@Nullable
	private Condition unrollGroup(List<? extends CriteriaDefinition> criteria, Table table,
			CriteriaDefinition.Combinator combinator, @Nullable RelationalPersistentEntity<?> entity,
			MapSqlParameterSource parameterSource) {

		Condition mapped = null;
		for (CriteriaDefinition criterion : criteria) {

			if (criterion.isEmpty()) {
				continue;
			}

			Condition condition = unroll(criterion, table, entity, parameterSource);

			mapped = combine(mapped, combinator, condition);
		}

		return mapped;
	}

	@Nullable
	private Condition getCondition(CriteriaDefinition criteria, MapSqlParameterSource parameterSource, Table table,
			@Nullable RelationalPersistentEntity<?> entity) {

		if (criteria.isEmpty()) {
			return null;
		}

		if (criteria.isGroup()) {

			Condition condition = unrollGroup(criteria.getGroup(), table, criteria.getCombinator(), entity, parameterSource);

			return condition == null ? null : Conditions.nest(condition);
		}

		return mapCondition(criteria, parameterSource, table, entity, false);
	}

	private Condition combine(@Nullable Condition currentCondition, CriteriaDefinition.Combinator combinator,
			Condition nextCondition) {

		if (currentCondition == null) {
			currentCondition = nextCondition;
		} else if (combinator == CriteriaDefinition.Combinator.INITIAL) {
			currentCondition = currentCondition.and(Conditions.nest(nextCondition));
		} else if (combinator == CriteriaDefinition.Combinator.AND) {
			currentCondition = currentCondition.and(nextCondition);
		} else if (combinator == CriteriaDefinition.Combinator.OR) {
			currentCondition = currentCondition.or(nextCondition);
		} else {
			throw new IllegalStateException("Combinator " + combinator + " not supported");
		}

		return currentCondition;
	}

	private Condition mapCondition(CriteriaDefinition criteria, MapSqlParameterSource parameterSource, Table table,
			@Nullable RelationalPersistentEntity<?> entity, boolean embedded) {

		SqlIdentifier criteriaColumn = criteria.getColumn();

		Assert.notNull(criteriaColumn, "Column must not be null");

		Field propertyField = createPropertyField(entity, criteriaColumn, this.mappingContext);
		Comparator comparator = criteria.getComparator();
		Object value = criteria.getValue();

		// Single embedded entity
		if (propertyField.isEmbedded()) {

			// IN/NOT_IN with collection of composite/embedded values: expand to (… AND …) OR (…)
			if ((Comparator.IN.equals(comparator) || Comparator.NOT_IN.equals(comparator))
					&& value instanceof Collection<?> collection) {

				return expandInCollectionComparison(comparator, collection,
						element -> mapCondition(new ListElementCriteria(criteria, element), parameterSource, table, entity, true));
			}

			PersistentPropertyPath<RelationalPersistentProperty> path = ((MetadataBackedField) propertyField).getPath();
			Assert.state(path != null, "Path must not be null");
			RelationalPersistentEntity<?> embeddedEntity = mappingContext
					.getRequiredPersistentEntity(propertyField.getRequiredProperty());

			Condition condition = mapEmbeddedObjectCondition(criteria, parameterSource, table, embeddedEntity, embedded);
			return embedded || !(condition instanceof AndCondition) ? condition : Conditions.nest(condition);
		}

		// AggregateReference (and similar associations) to a composite identifier: expand IN/NOT_IN like embedded
		if (propertyField instanceof MetadataBackedField metadataBackedField && metadataBackedField.property != null) {

			RelationalPersistentProperty associationProperty = metadataBackedField.property;

			if (Association.isAssociation(associationProperty)) {

				Association association = Association.from(associationProperty, converter);

				if (association.isComplexIdentifier() //
						&& (Comparator.IN.equals(comparator) || Comparator.NOT_IN.equals(comparator))
						&& value instanceof Collection<?> collection) {

					RelationalPersistentEntity<?> identifierEntity = association.getRequiredTargetIdentifierEntity();

					return expandInCollectionComparison(comparator, collection,
							element -> mapEmbeddedObjectCondition(
									new ListElementCriteria(criteria, unwrapAssociationCriteriaValue(element)), parameterSource, table,
									identifierEntity, true));
				}
			}
		}

		TypeInformation<?> actualType = propertyField.getTypeHint().getRequiredActualType();
		Column column = table.column(propertyField.getMappedColumnName());
		Object mappedValue;
		SQLType sqlType;

		Assert.notNull(comparator, "Comparator must not be null");

		if (criteria.getValue() instanceof JdbcValue settableValue) {

			mappedValue = convertValue(comparator, settableValue.getValue(), propertyField.getTypeHint());
			sqlType = getTypeHint(mappedValue, actualType.getType(), settableValue);
		} else if (criteria.getValue() instanceof ValueFunction valueFunction) {

			mappedValue = valueFunction.map(v -> convertValue(comparator, v, propertyField.getTypeHint()));
			sqlType = propertyField.getSqlType();

		} else if (propertyField instanceof MetadataBackedField metadataBackedField //
				&& metadataBackedField.property != null //
				&& (criteria.getValue() == null || !criteria.getValue().getClass().isArray())) {

			RelationalPersistentProperty property = metadataBackedField.property;
			JdbcValue jdbcValue = convertToJdbcValue(property, criteria.getValue());
			mappedValue = jdbcValue.getValue();
			sqlType = jdbcValue.getJdbcType() != null ? jdbcValue.getJdbcType() : propertyField.getSqlType();

		} else {

			mappedValue = convertValue(comparator, criteria.getValue(), propertyField.getTypeHint());
			sqlType = propertyField.getSqlType();
		}

		return createCondition(column, mappedValue, sqlType, parameterSource, comparator, criteria.isIgnoreCase());
	}

	/**
	 * Expands {@link Comparator#IN}/{@link Comparator#NOT_IN} over a collection to a disjunction of nested conditions,
	 * one per element (used for embedded types and composite association identifiers).
	 */
	@SuppressWarnings("NullAway")
	private Condition expandInCollectionComparison(Comparator comparator, Collection<?> collection,
			Function<Object, Condition> nestedConditionFactory) {

		if (CollectionUtils.isEmpty(collection)) {
			return Comparator.IN.equals(comparator) ? Conditions.unrestricted().not() : Conditions.unrestricted();
		}

		Condition condition = null;
		for (Object element : collection) {
			Condition next = Conditions.nest(nestedConditionFactory.apply(element));
			condition = condition == null ? next : condition.or(next);
		}

		return condition;
	}

	/**
	 * Converts values while taking specific value types like arrays, {@link Iterable}, or {@link Pair}.
	 *
	 * @param property the property to which the value relates. It determines the type to convert to. Must not be
	 *          {@literal null}.
	 * @param value the value to be converted.
	 * @return a non-null {@link JdbcValue} holding the converted value and the appropriate JDBC type information.
	 */
	private JdbcValue convertToJdbcValue(RelationalPersistentProperty property, @Nullable Object value) {

		if (value == null) {
			return JdbcValue.of(null, JDBCType.NULL);
		}

		if (value instanceof Pair) {

			JdbcValue first = getWriteValue(property, ((Pair<?, ?>) value).getFirst());
			JdbcValue second = getWriteValue(property, ((Pair<?, ?>) value).getSecond());
			Object firstValue = first.getValue();
			Object secondValue = second.getValue();

			Assert.state(firstValue != null, "First value must not be null");
			Assert.state(secondValue != null, "Second value must not be null");

			return JdbcValue.of(Pair.of(firstValue, secondValue), first.getJdbcType());
		}

		if (value instanceof Iterable) {

			List<@Nullable Object> mapped = new ArrayList<>();
			SQLType jdbcType = null;

			for (Object o : (Iterable<?>) value) {

				JdbcValue jdbcValue = getWriteValue(property, o);
				if (jdbcType == null) {
					jdbcType = jdbcValue.getJdbcType();
				}

				mapped.add(jdbcValue.getValue());
			}

			return JdbcValue.of(mapped, jdbcType);
		}

		if (value.getClass().isArray()) {

			Object[] valueAsArray = (Object[]) value;
			@Nullable
			Object[] mappedValueArray = new Object @Nullable [valueAsArray.length];
			SQLType jdbcType = null;

			for (int i = 0; i < valueAsArray.length; i++) {

				JdbcValue jdbcValue = getWriteValue(property, valueAsArray[i]);
				if (jdbcType == null) {
					jdbcType = jdbcValue.getJdbcType();
				}

				mappedValueArray[i] = jdbcValue.getValue();
			}

			return JdbcValue.of(mappedValueArray, jdbcType);
		}

		return getWriteValue(property, value);
	}

	/**
	 * Converts values to a {@link JdbcValue}.
	 *
	 * @param property the property to which the value relates. It determines the type to convert to. Must not be
	 *          {@literal null}.
	 * @param value the value to be converted.
	 * @return a non null {@link JdbcValue} holding the converted value and the appropriate JDBC type information.
	 */
	private JdbcValue getWriteValue(RelationalPersistentProperty property, Object value) {

		return converter.writeJdbcValue( //
				value, //
				converter.getColumnType(property), //
				converter.getTargetSqlType(property) //
		);
	}

	private static Object unwrapAssociationCriteriaValue(Object value) {

		if (value instanceof AggregateReference<?, ?> aggregateReference) {
			return aggregateReference.getId();
		}

		return value;
	}

	private Condition mapEmbeddedObjectCondition(CriteriaDefinition criteria, MapSqlParameterSource parameterSource,
			Table table, RelationalPersistentEntity<?> embeddedEntity, boolean embedded) {

		Assert.isInstanceOf(embeddedEntity.getType(), criteria.getValue(),
				() -> "Value must be of type " + embeddedEntity.getType().getName() + " for embedded entity matching");

		PersistentPropertyAccessor<Object> embeddedAccessor = embeddedEntity.getPropertyAccessor(criteria.getValue());

		Condition condition = Conditions.unrestricted();
		for (RelationalPersistentProperty embeddedProperty : embeddedEntity) {

			Object propertyValue = embeddedAccessor.getProperty(embeddedProperty);

			CriteriaDefinition cw = new EmbeddedPropertyCriteria(criteria, embeddedProperty, propertyValue);
			Condition mapped = mapCondition(cw, parameterSource, table, embeddedEntity, embedded);
			condition = condition.and(mapped);
		}

		return condition;
	}

	@Nullable
	private Object convertValue(Comparator comparator, @Nullable Object value, TypeInformation<?> typeHint) {

		if ((Comparator.IN.equals(comparator) || Comparator.NOT_IN.equals(comparator))
				&& value instanceof Collection<?> collection && !collection.isEmpty()) {

			Collection<Object> mapped = new ArrayList<>(collection.size());

			for (Object o : collection) {
				mapped.add(convertValue(o, typeHint));
			}

			return mapped;
		}

		return convertValue(value, typeHint);
	}

	@Nullable
	protected Object convertValue(@Nullable Object value, TypeInformation<?> typeInformation) {

		if (value == null) {
			return null;
		}

		if (value instanceof Pair) {

			Pair<Object, Object> pair = (Pair<Object, Object>) value;

			Object first = convertValue(pair.getFirst(), typeInformation.getActualType() != null //
					? typeInformation.getRequiredActualType()
					: TypeInformation.OBJECT);

			Object second = convertValue(pair.getSecond(), typeInformation.getActualType() != null //
					? typeInformation.getRequiredActualType()
					: TypeInformation.OBJECT);

			Assert.state(first != null, "First value must not be null");
			Assert.state(second != null, "Second value must not be null");

			return Pair.of(first, second);
		}

		if (value.getClass().isArray()
				&& (TypeInformation.OBJECT.equals(typeInformation) || typeInformation.isCollectionLike())) {
			return value;
		}

		return this.converter.writeValue(value, typeInformation);
	}

	protected MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> getMappingContext() {
		return this.mappingContext;
	}

	private Condition createCondition(Column column, @Nullable Object mappedValue, SQLType sqlType,
			MapSqlParameterSource parameterSource, Comparator comparator, boolean ignoreCase) {

		if (comparator.equals(Comparator.IS_NULL)) {
			return column.isNull();
		}

		if (comparator.equals(Comparator.IS_NOT_NULL)) {
			return column.isNotNull();
		}

		if (comparator == Comparator.IS_TRUE) {

			Expression bind = bindBoolean(column, parameterSource,
					mappedValue instanceof Boolean ? (Boolean) mappedValue : true);
			return column.isEqualTo(bind);
		}

		if (comparator == Comparator.IS_FALSE) {

			Expression bind = bindBoolean(column, parameterSource,
					mappedValue instanceof Boolean ? (Boolean) mappedValue : false);
			return column.isEqualTo(bind);
		}

		Expression columnExpression = column;
		if (ignoreCase && (sqlType == JDBCType.VARCHAR || sqlType == JDBCType.NVARCHAR)) {
			columnExpression = Functions.upper(column);
		}

		if (comparator == Comparator.NOT_IN || comparator == Comparator.IN) {

			Condition condition;

			if (mappedValue instanceof Iterable) {

				List<Expression> expressions = new ArrayList<>(
						mappedValue instanceof Collection ? ((Collection<?>) mappedValue).size() : 10);

				for (Object o : (Iterable<?>) mappedValue) {

					expressions.add(bind(o, sqlType, parameterSource, column.getName().getReference()));
				}

				condition = Conditions.in(columnExpression, expressions.toArray(new Expression[0]));

			} else {

				Expression expression = bind(mappedValue, sqlType, parameterSource, column.getName().getReference());

				condition = Conditions.in(columnExpression, expression);
			}

			if (comparator == Comparator.NOT_IN) {
				condition = condition.not();
			}

			return condition;
		}

		if (comparator == Comparator.BETWEEN || comparator == Comparator.NOT_BETWEEN) {

			Assert.state(mappedValue != null, "Mapped value must not be null");

			Pair<Object, Object> pair = (Pair<Object, Object>) mappedValue;

			Expression begin = bind(pair.getFirst(), sqlType, parameterSource, column.getName().getReference(), ignoreCase);
			Expression end = bind(pair.getSecond(), sqlType, parameterSource, column.getName().getReference(), ignoreCase);

			return comparator == Comparator.BETWEEN ? Conditions.between(columnExpression, begin, end)
					: Conditions.notBetween(columnExpression, begin, end);
		}

		String refName = column.getName().getReference();

		switch (comparator) {
			case EQ -> {
				Expression expression = bind(mappedValue, sqlType, parameterSource, refName, ignoreCase);
				return Conditions.isEqual(columnExpression, expression);
			}
			case NEQ -> {
				Expression expression = bind(mappedValue, sqlType, parameterSource, refName, ignoreCase);
				return Conditions.isEqual(columnExpression, expression).not();
			}
			case LT -> {
				Expression expression = bind(mappedValue, sqlType, parameterSource, refName);
				return column.isLess(expression);
			}
			case LTE -> {
				Expression expression = bind(mappedValue, sqlType, parameterSource, refName);
				return column.isLessOrEqualTo(expression);
			}
			case GT -> {
				Expression expression = bind(mappedValue, sqlType, parameterSource, refName);
				return column.isGreater(expression);
			}
			case GTE -> {
				Expression expression = bind(mappedValue, sqlType, parameterSource, refName);
				return column.isGreaterOrEqualTo(expression);
			}
			case LIKE -> {
				Expression expression = bind(mappedValue, sqlType, parameterSource, refName, ignoreCase);
				return Conditions.like(columnExpression, expression);
			}
			case NOT_LIKE -> {
				Expression expression = bind(mappedValue, sqlType, parameterSource, refName, ignoreCase);
				return Conditions.notLike(columnExpression, expression);
			}
			default -> throw new UnsupportedOperationException("Comparator " + comparator + " not supported");
		}
	}

	private Expression bindBoolean(Column column, MapSqlParameterSource parameterSource, boolean value) {

		Object converted = converter.writeValue(value, TypeInformation.OBJECT);
		return bind(converted, JDBCType.BIT, parameterSource, column.getName().getReference());
	}

	Field createPropertyField(@Nullable RelationalPersistentEntity<?> entity, SqlIdentifier key) {
		return entity == null ? new Field(key) : new MetadataBackedField(key, entity, mappingContext, converter);
	}

	Field createPropertyField(@Nullable RelationalPersistentEntity<?> entity, SqlIdentifier key,
			MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> mappingContext) {
		return entity == null ? new Field(key) : new MetadataBackedField(key, entity, mappingContext, converter);
	}

	SQLType getTypeHint(@Nullable Object mappedValue, Class<?> propertyType, JdbcValue settableValue) {

		if (mappedValue == null || propertyType.equals(Object.class)) {
			return JdbcUtil.TYPE_UNKNOWN;
		}

		Object value = settableValue.getValue();
		if (value instanceof JdbcValue jdbcValue) {
			return jdbcValue.getJdbcType();
		}

		Assert.state(value != null, "Settable value must not be null");

		if (mappedValue.getClass().equals(value.getClass())) {
			return JdbcUtil.TYPE_UNKNOWN;
		}

		SQLType jdbcType = settableValue.getJdbcType();

		Assert.state(jdbcType != null, "JDBC type must not be null");

		return jdbcType;
	}

	private Expression bind(@Nullable Object mappedValue, SQLType sqlType, MapSqlParameterSource parameterSource,
			String name) {
		return bind(mappedValue, sqlType, parameterSource, name, false);
	}

	private Expression bind(@Nullable Object mappedValue, SQLType sqlType, MapSqlParameterSource parameterSource,
			String name, boolean ignoreCase) {

		String uniqueName = getUniqueName(parameterSource, name);

		parameterSource.addValue(uniqueName, mappedValue, sqlType.getVendorTypeNumber());

		return ignoreCase ? Functions.upper(SQL.bindMarker(":" + uniqueName)) : SQL.bindMarker(":" + uniqueName);
	}

	private static String getUniqueName(MapSqlParameterSource parameterSource, String name) {

		Map<String, Object> values = parameterSource.getValues();

		if (!values.containsKey(name)) {
			return name;
		}

		int counter = values.size();
		String uniqueName;

		do {
			uniqueName = name + (counter++);
		} while (values.containsKey(uniqueName));

		return uniqueName;
	}

	/**
	 * {@link CriteriaDefinition} view of one element when expanding {@code IN}/{@code NOT IN} over embedded or composite
	 * identifier values.
	 */
	private static final class ListElementCriteria extends CriteriaWrapper {

		// private final SqlIdentifier column;
		// private final Comparator comparator;
		private final Object elementValue;

		ListElementCriteria(CriteriaDefinition delegate, Object elementValue) {
			super(delegate);
			// this.column = column;
			// this.comparator = comparator;
			this.elementValue = elementValue;
		}

		@Override
		public @Nullable Comparator getComparator() {
			Comparator elementComparator = getDelegate().getComparator();
			return Comparator.IN.equals(elementComparator) ? Comparator.EQ : Comparator.NEQ;
		}

		@Override
		public @Nullable SqlIdentifier getColumn() {
			return getDelegate().getColumn();
		}

		@Override
		public @Nullable Object getValue() {
			return this.elementValue;
		}
	}

	/**
	 * {@link CriteriaDefinition} for a single property of an embedded object, delegating flags to the outer criteria.
	 */
	private static final class EmbeddedPropertyCriteria extends CriteriaWrapper {

		private final SqlIdentifier propertyColumn;
		private final @Nullable Object propertyValue;

		EmbeddedPropertyCriteria(CriteriaDefinition delegate, RelationalPersistentProperty embeddedProperty,
				@Nullable Object propertyValue) {
			super(delegate);
			this.propertyColumn = SqlIdentifier.unquoted(embeddedProperty.getName());
			this.propertyValue = propertyValue;
		}

		@Override
		public SqlIdentifier getColumn() {
			return this.propertyColumn;
		}

		@Override
		public @Nullable Object getValue() {
			return this.propertyValue;
		}
	}

	abstract static class CriteriaWrapper implements CriteriaDefinition {

		private final CriteriaDefinition delegate;

		public CriteriaWrapper(CriteriaDefinition delegate) {
			this.delegate = delegate;
		}

		protected CriteriaDefinition getDelegate() {
			return delegate;
		}

		@Nullable
		@Override
		public Comparator getComparator() {
			return delegate.getComparator();
		}

		@Override
		public boolean isIgnoreCase() {
			return delegate.isIgnoreCase();
		}

		@Override
		public boolean isGroup() {
			return false;
		}

		@Override
		public List<CriteriaDefinition> getGroup() {
			return List.of();
		}

		@Nullable
		@Override
		public SqlIdentifier getColumn() {
			return null;
		}

		@Nullable
		@Override
		public Object getValue() {
			return null;
		}

		@Nullable
		@Override
		public CriteriaDefinition getPrevious() {
			return null;
		}

		@Override
		public boolean hasPrevious() {
			return false;
		}

		@Override
		public boolean isEmpty() {
			return false;
		}

		@Override
		public Combinator getCombinator() {
			throw new UnsupportedOperationException("No combinator for AbstractCriteria");
		}
	}

	/**
	 * Value object to represent a field and its meta-information.
	 */
	protected static class Field {

		protected final SqlIdentifier name;

		/**
		 * Creates a new {@link Field} without meta-information but the given name.
		 *
		 * @param name must not be {@literal null} or empty.
		 */
		Field(SqlIdentifier name) {

			Assert.notNull(name, "Name must not be null");
			this.name = name;
		}

		/**
		 * Returns the key to be used in the mapped document eventually.
		 *
		 * @return the key to be used in the mapped document eventually.
		 */
		public SqlIdentifier getMappedColumnName() {
			return this.name;
		}

		public TypeInformation<?> getTypeHint() {
			return TypeInformation.OBJECT;
		}

		public boolean isEmbedded() {
			return false;
		}

		public @Nullable RelationalPersistentProperty getProperty() {
			return null;
		}

		public RelationalPersistentProperty getRequiredProperty() {

			RelationalPersistentProperty property = getProperty();

			if (property == null) {
				throw new IllegalStateException("No property found for field: " + this.name);
			}

			return property;
		}

		public SQLType getSqlType() {
			return JdbcUtil.TYPE_UNKNOWN;
		}
	}

	/**
	 * Extension of {@link Field} to be backed with mapping metadata.
	 */
	protected static class MetadataBackedField extends Field {

		private final RelationalPersistentEntity<?> entity;
		private final MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> mappingContext;
		private final @Nullable RelationalPersistentProperty property;
		private final @Nullable PersistentPropertyPath<RelationalPersistentProperty> path;
		private final SQLType sqlType;

		/**
		 * Creates a new {@link MetadataBackedField} with the given name, {@link RelationalPersistentEntity} and
		 * {@link MappingContext}.
		 *
		 * @param name must not be {@literal null} or empty.
		 * @param entity must not be {@literal null}.
		 * @param context must not be {@literal null}.
		 * @param converter must not be {@literal null}.
		 */
		protected MetadataBackedField(SqlIdentifier name, RelationalPersistentEntity<?> entity,
				MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> context,
				JdbcConverter converter) {

			super(name);

			Assert.notNull(entity, "RelationalPersistentEntity must not be null");

			this.entity = entity;
			this.mappingContext = context;

			this.path = getPath(name.getReference());

			RelationalPersistentProperty persistentProperty = null;
			if (this.path != null) {

				RelationalPersistentEntity<?> currentEntity = entity;
				RelationalPersistentProperty currentProperty = null;
				for (RelationalPersistentProperty p : path) {

					currentProperty = currentEntity.getPersistentProperty(p.getName());

					if (currentProperty == null) {
						break;
					}

					if (currentProperty.isEntity()) {
						currentEntity = mappingContext.getRequiredPersistentEntity(currentProperty);
					}
				}

				persistentProperty = currentProperty;
			}

			this.property = persistentProperty;
			this.sqlType = this.property != null ? converter.getTargetSqlType(this.property) : JdbcUtil.TYPE_UNKNOWN;
		}

		@Override
		public SqlIdentifier getMappedColumnName() {
			return this.property == null ? super.getMappedColumnName() : this.property.getColumnName();
		}

		/**
		 * Returns the {@link PersistentPropertyPath} for the given {@code pathExpression}.
		 */
		@Nullable
		private PersistentPropertyPath<RelationalPersistentProperty> getPath(String pathExpression) {

			try {

				PropertyPath path = forName(pathExpression);

				if (isPathToJavaLangClassProperty(path)) {
					return null;
				}

				return this.mappingContext.getPersistentPropertyPath(path);
			} catch (MappingException | PropertyReferenceException e) {
				return null;
			}
		}

		private PropertyPath forName(String path) {

			if (entity.getPersistentProperty(path) != null) {
				return PropertyPath.from(Pattern.quote(path), entity.getTypeInformation());
			}

			return PropertyPath.from(path, entity.getTypeInformation());
		}

		private boolean isPathToJavaLangClassProperty(PropertyPath path) {
			return path.getType().equals(Class.class) && path.getLeafProperty().getOwningType().getType().equals(Class.class);
		}

		@Nullable
		public PersistentPropertyPath<RelationalPersistentProperty> getPath() {
			return path;
		}

		@Override
		public TypeInformation<?> getTypeHint() {

			if (this.property == null) {
				return super.getTypeHint();
			}

			if (this.property.getType().isPrimitive()) {
				return TypeInformation.of(ClassUtils.resolvePrimitiveIfNecessary(this.property.getType()));
			}

			if (this.property.getType().isArray()) {
				return this.property.getTypeInformation();
			}

			return this.property.getTypeInformation();
		}

		@Override
		public boolean isEmbedded() {
			return this.property != null && this.property.isEmbedded();
		}

		@Override
		public @Nullable RelationalPersistentProperty getProperty() {
			return property;
		}

		@Override
		public SQLType getSqlType() {
			return this.sqlType;
		}
	}
}
