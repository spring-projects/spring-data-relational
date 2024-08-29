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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.PropertyReferenceException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.relational.core.dialect.Escaper;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.relational.core.query.CriteriaDefinition.Comparator;
import org.springframework.data.relational.core.query.ValueFunction;
import org.springframework.data.relational.core.sql.*;
import org.springframework.data.relational.domain.SqlSort;
import org.springframework.data.util.Pair;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.r2dbc.core.binding.BindMarker;
import org.springframework.r2dbc.core.binding.BindMarkers;
import org.springframework.r2dbc.core.binding.Bindings;
import org.springframework.r2dbc.core.binding.MutableBindings;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Maps {@link CriteriaDefinition} and {@link Sort} objects considering mapping metadata and dialect-specific
 * conversion.
 *
 * @author Mark Paluch
 * @author Roman Chigvintsev
 * @author Manousos Mathioudakis
 * @author Jens Schauder
 * @author Yan Qiang
 */
public class QueryMapper {

	private final R2dbcConverter converter;
	private final R2dbcDialect dialect;
	private final MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> mappingContext;

	/**
	 * Creates a new {@link QueryMapper} with the given {@link R2dbcConverter}.
	 *
	 * @param dialect
	 * @param converter must not be {@literal null}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public QueryMapper(R2dbcDialect dialect, R2dbcConverter converter) {

		Assert.notNull(converter, "R2dbcConverter must not be null");
		Assert.notNull(dialect, "R2dbcDialect must not be null");

		this.converter = converter;
		this.dialect = dialect;
		this.mappingContext = (MappingContext) converter.getMappingContext();
	}

	/**
	 * Render a {@link SqlIdentifier} for SQL usage. The resulting String might contain quoting characters.
	 *
	 * @param identifier the identifier to be rendered.
	 * @return an identifier String.
	 * @since 1.1
	 */
	public String toSql(SqlIdentifier identifier) {

		Assert.notNull(identifier, "SqlIdentifier must not be null");

		return identifier.toSql(this.dialect.getIdentifierProcessing());
	}

	/**
	 * Map the {@link Sort} object to apply field name mapping using {@link Class the type to read}.
	 *
	 * @param sort must not be {@literal null}.
	 * @param entity related {@link RelationalPersistentEntity}, can be {@literal null}.
	 * @return
	 * @since 1.1
	 */
	public List<OrderByField> getMappedSort(Table table, Sort sort, @Nullable RelationalPersistentEntity<?> entity) {

		List<OrderByField> mappedOrder = new ArrayList<>();

		for (Sort.Order order : sort) {

			SqlSort.validate(order);

			OrderByField simpleOrderByField = createSimpleOrderByField(table, entity, order);
			OrderByField orderBy = simpleOrderByField.withNullHandling(order.getNullHandling());
			mappedOrder.add(order.isAscending() ? orderBy.asc() : orderBy.desc());
		}

		return mappedOrder;
	}

	private OrderByField createSimpleOrderByField(Table table, RelationalPersistentEntity<?> entity, Sort.Order order) {

		if (order instanceof SqlSort.SqlOrder sqlOrder && sqlOrder.isUnsafe()) {
			return OrderByField.from(Expressions.just(sqlOrder.getProperty()));
		}

		Field field = createPropertyField(entity, SqlIdentifier.unquoted(order.getProperty()), this.mappingContext);
		return OrderByField.from(table.column(field.getMappedColumnName()));
	}

	/**
	 * Map the {@link Expression} object to apply field name mapping using {@link Class the type to read}.
	 *
	 * @param expression must not be {@literal null}.
	 * @param entity related {@link RelationalPersistentEntity}, can be {@literal null}.
	 * @return the mapped {@link Expression}.
	 * @since 1.1
	 */
	public Expression getMappedObject(Expression expression, @Nullable RelationalPersistentEntity<?> entity) {

		if (entity == null || expression instanceof AsteriskFromTable
				|| expression instanceof Expressions.SimpleExpression) {
			return expression;
		}

		if (expression instanceof Column column) {

			Field field = createPropertyField(entity, column.getName());
			TableLike table = column.getTable();

			Column columnFromTable = table.column(field.getMappedColumnName());
			return column instanceof Aliased ? columnFromTable.as(((Aliased) column).getAlias()) : columnFromTable;
		}

		if (expression instanceof SimpleFunction function) {

			List<Expression> arguments = function.getExpressions();
			List<Expression> mappedArguments = new ArrayList<>(arguments.size());

			for (Expression argument : arguments) {
				mappedArguments.add(getMappedObject(argument, entity));
			}

			SimpleFunction mappedFunction = SimpleFunction.create(function.getFunctionName(), mappedArguments);

			return function instanceof Aliased ? mappedFunction.as(((Aliased) function).getAlias()) : mappedFunction;
		}

		throw new IllegalArgumentException(String.format("Cannot map %s", expression));
	}

	/**
	 * Map a {@link CriteriaDefinition} object into {@link Condition} and consider value/{@code NULL} {@link Bindings}.
	 *
	 * @param markers bind markers object, must not be {@literal null}.
	 * @param criteria criteria definition to map, must not be {@literal null}.
	 * @param table must not be {@literal null}.
	 * @param entity related {@link RelationalPersistentEntity}, can be {@literal null}.
	 * @return the mapped {@link BoundCondition}.
	 * @since 1.1
	 */
	public BoundCondition getMappedObject(BindMarkers markers, CriteriaDefinition criteria, Table table,
			@Nullable RelationalPersistentEntity<?> entity) {

		Assert.notNull(markers, "BindMarkers must not be null");
		Assert.notNull(criteria, "CriteriaDefinition must not be null");
		Assert.notNull(table, "Table must not be null");

		MutableBindings bindings = new MutableBindings(markers);

		if (criteria.isEmpty()) {
			throw new IllegalArgumentException("Cannot map empty Criteria");
		}

		Condition mapped = unroll(criteria, table, entity, bindings);

		return new BoundCondition(bindings, mapped);
	}

	private Condition unroll(CriteriaDefinition criteria, Table table, @Nullable RelationalPersistentEntity<?> entity,
			MutableBindings bindings) {

		CriteriaDefinition current = criteria;

		// reverse unroll criteria chain
		Map<CriteriaDefinition, CriteriaDefinition> forwardChain = new HashMap<>();

		while (current.hasPrevious()) {
			forwardChain.put(current.getPrevious(), current);
			current = current.getPrevious();
		}

		// perform the actual mapping
		Condition mapped = getCondition(current, bindings, table, entity);
		while (forwardChain.containsKey(current)) {

			CriteriaDefinition criterion = forwardChain.get(current);
			Condition result = null;

			Condition condition = getCondition(criterion, bindings, table, entity);
			if (condition != null) {
				result = combine(criterion, mapped, criterion.getCombinator(), condition);
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
			MutableBindings bindings) {

		Condition mapped = null;
		for (CriteriaDefinition criterion : criteria) {

			if (criterion.isEmpty()) {
				continue;
			}

			Condition condition = unroll(criterion, table, entity, bindings);

			mapped = combine(criterion, mapped, combinator, condition);
		}

		return mapped;
	}

	@Nullable
	private Condition getCondition(CriteriaDefinition criteria, MutableBindings bindings, Table table,
			@Nullable RelationalPersistentEntity<?> entity) {

		if (criteria.isEmpty()) {
			return null;
		}

		if (criteria.isGroup()) {

			Condition condition = unrollGroup(criteria.getGroup(), table, criteria.getCombinator(), entity, bindings);

			return condition == null ? null : Conditions.nest(condition);
		}

		return mapCondition(criteria, bindings, table, entity);
	}

	private Condition combine(CriteriaDefinition criteria, @Nullable Condition currentCondition,
			CriteriaDefinition.Combinator combinator, Condition nextCondition) {

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

	private Condition mapCondition(CriteriaDefinition criteria, MutableBindings bindings, Table table,
			@Nullable RelationalPersistentEntity<?> entity) {

		Field propertyField = createPropertyField(entity, criteria.getColumn(), this.mappingContext);
		Column column = table.column(propertyField.getMappedColumnName());
		TypeInformation<?> actualType = propertyField.getTypeHint().getRequiredActualType();

		Object mappedValue;
		Class<?> typeHint;

		Comparator comparator = criteria.getComparator();
		if (criteria.getValue() instanceof Parameter parameter) {

			mappedValue = convertValue(comparator, parameter.getValue(), propertyField.getTypeHint());
			typeHint = getTypeHint(mappedValue, actualType.getType(), parameter);
		} else if (criteria.getValue() instanceof ValueFunction<?> valueFunction) {

			mappedValue = valueFunction.map(v -> convertValue(comparator, v, propertyField.getTypeHint()))
					.apply(getEscaper(comparator));

			typeHint = actualType.getType();
		} else {

			mappedValue = convertValue(comparator, criteria.getValue(), propertyField.getTypeHint());
			typeHint = actualType.getType();
		}

		return createCondition(column, mappedValue, typeHint, bindings, comparator, criteria.isIgnoreCase());
	}

	private Escaper getEscaper(Comparator comparator) {

		if (comparator == Comparator.LIKE || comparator == Comparator.NOT_LIKE) {
			return dialect.getLikeEscaper();
		}

		return Escaper.DEFAULT;
	}

	/**
	 * Potentially convert the {@link Parameter}.
	 *
	 * @param value
	 * @return
	 * @since 1.2
	 */
	public Parameter getBindValue(Parameter value) {

		if (value.isEmpty()) {
			return Parameter.empty(converter.getTargetType(value.getType()));
		}

		return Parameter.from(convertValue(value.getValue(), TypeInformation.OBJECT));
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

			Object first = convertValue(pair.getFirst(),
					typeInformation.getActualType() != null ? typeInformation.getRequiredActualType() : TypeInformation.OBJECT);

			Object second = convertValue(pair.getSecond(),
					typeInformation.getActualType() != null ? typeInformation.getRequiredActualType() : TypeInformation.OBJECT);

			return Pair.of(first, second);
		}

		return this.converter.writeValue(value, typeInformation);
	}

	protected MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> getMappingContext() {
		return this.mappingContext;
	}

	private Condition createCondition(Column column, @Nullable Object mappedValue, Class<?> valueType,
			MutableBindings bindings, Comparator comparator, boolean ignoreCase) {

		if (comparator.equals(Comparator.IS_NULL)) {
			return column.isNull();
		}

		if (comparator.equals(Comparator.IS_NOT_NULL)) {
			return column.isNotNull();
		}

		if (comparator == Comparator.IS_TRUE) {
			Expression bind = booleanBind(column, mappedValue, valueType, bindings, ignoreCase);

			return column.isEqualTo(bind);
		}

		if (comparator == Comparator.IS_FALSE) {
			Expression bind = booleanBind(column, mappedValue, valueType, bindings, ignoreCase);

			return column.isEqualTo(bind);
		}

		Expression columnExpression = column;
		if (ignoreCase) {
			columnExpression = Functions.upper(column);
		}

		if (comparator == Comparator.NOT_IN || comparator == Comparator.IN) {

			Condition condition;

			if (mappedValue instanceof Iterable) {

				List<Expression> expressions = new ArrayList<>(
						mappedValue instanceof Collection ? ((Collection<?>) mappedValue).size() : 10);

				for (Object o : (Iterable<?>) mappedValue) {

					BindMarker bindMarker = bindings.nextMarker(column.getName().getReference());
					expressions.add(bind(o, valueType, bindings, bindMarker));
				}

				condition = Conditions.in(columnExpression, expressions.toArray(new Expression[0]));

			} else {

				BindMarker bindMarker = bindings.nextMarker(column.getName().getReference());
				Expression expression = bind(mappedValue, valueType, bindings, bindMarker);

				condition = Conditions.in(columnExpression, expression);
			}

			if (comparator == Comparator.NOT_IN) {
				condition = condition.not();
			}

			return condition;
		}

		if (comparator == Comparator.BETWEEN || comparator == Comparator.NOT_BETWEEN) {

			Pair<Object, Object> pair = (Pair<Object, Object>) mappedValue;

			Expression begin = bind(pair.getFirst(), valueType, bindings,
					bindings.nextMarker(column.getName().getReference()), ignoreCase);
			Expression end = bind(pair.getSecond(), valueType, bindings, bindings.nextMarker(column.getName().getReference()),
					ignoreCase);

			return comparator == Comparator.BETWEEN ? Conditions.between(columnExpression, begin, end)
					: Conditions.notBetween(columnExpression, begin, end);
		}

		BindMarker bindMarker = bindings.nextMarker(column.getName().getReference());

		switch (comparator) {
			case EQ: {
				Expression expression = bind(mappedValue, valueType, bindings, bindMarker, ignoreCase);
				return Conditions.isEqual(columnExpression, expression);
			}
			case NEQ: {
				Expression expression = bind(mappedValue, valueType, bindings, bindMarker, ignoreCase);
				return Conditions.isEqual(columnExpression, expression).not();
			}
			case LT: {
				Expression expression = bind(mappedValue, valueType, bindings, bindMarker);
				return column.isLess(expression);
			}
			case LTE: {
				Expression expression = bind(mappedValue, valueType, bindings, bindMarker);
				return column.isLessOrEqualTo(expression);
			}
			case GT: {
				Expression expression = bind(mappedValue, valueType, bindings, bindMarker);
				return column.isGreater(expression);
			}
			case GTE: {
				Expression expression = bind(mappedValue, valueType, bindings, bindMarker);
				return column.isGreaterOrEqualTo(expression);
			}
			case LIKE: {
				Expression expression = bind(mappedValue, valueType, bindings, bindMarker, ignoreCase);
				return Conditions.like(columnExpression, expression);
			}
			case NOT_LIKE: {
				Expression expression = bind(mappedValue, valueType, bindings, bindMarker, ignoreCase);
				return Conditions.notLike(columnExpression, expression);
			}
			default:
				throw new UnsupportedOperationException("Comparator " + comparator + " not supported");
		}
	}

	Field createPropertyField(@Nullable RelationalPersistentEntity<?> entity, SqlIdentifier key) {
		return entity == null ? new Field(key) : new MetadataBackedField(key, entity, mappingContext);
	}

	Field createPropertyField(@Nullable RelationalPersistentEntity<?> entity, SqlIdentifier key,
			MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> mappingContext) {
		return entity == null ? new Field(key) : new MetadataBackedField(key, entity, mappingContext);
	}

	Class<?> getTypeHint(@Nullable Object mappedValue, Class<?> propertyType) {
		return propertyType;
	}

	Class<?> getTypeHint(@Nullable Object mappedValue, Class<?> propertyType, Parameter parameter) {

		if (mappedValue == null || propertyType.equals(Object.class)) {
			return parameter.getType();
		}

		if (mappedValue.getClass().equals(parameter.getValue().getClass())) {
			return parameter.getType();
		}

		return propertyType;
	}

	private Expression bind(@Nullable Object mappedValue, Class<?> valueType, MutableBindings bindings,
			BindMarker bindMarker) {
		return bind(mappedValue, valueType, bindings, bindMarker, false);
	}

	private Expression bind(@Nullable Object mappedValue, Class<?> valueType, MutableBindings bindings,
			BindMarker bindMarker, boolean ignoreCase) {

		if (mappedValue != null) {
			bindings.bind(bindMarker, mappedValue);
		} else {
			bindings.bindNull(bindMarker, valueType);
		}

		return ignoreCase ? Functions.upper(SQL.bindMarker(bindMarker.getPlaceholder()))
				: SQL.bindMarker(bindMarker.getPlaceholder());
	}

	private Expression booleanBind(Column column, Object mappedValue, Class<?> valueType, MutableBindings bindings,
			boolean ignoreCase) {
		BindMarker bindMarker = bindings.nextMarker(column.getName().getReference());

		return bind(mappedValue, valueType, bindings, bindMarker, ignoreCase);
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
		public Field(SqlIdentifier name) {

			Assert.notNull(name, "Name must not be null");
			this.name = name;
		}

		/**
		 * Returns the key to be used in the mapped document eventually.
		 *
		 * @return
		 */
		public SqlIdentifier getMappedColumnName() {
			return this.name;
		}

		public TypeInformation<?> getTypeHint() {
			return TypeInformation.OBJECT;
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

		/**
		 * Creates a new {@link MetadataBackedField} with the given name, {@link RelationalPersistentEntity} and
		 * {@link MappingContext}.
		 *
		 * @param name must not be {@literal null} or empty.
		 * @param entity must not be {@literal null}.
		 * @param context must not be {@literal null}.
		 */
		protected MetadataBackedField(SqlIdentifier name, RelationalPersistentEntity<?> entity,
				MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> context) {
			this(name, entity, context, null);
		}

		/**
		 * Creates a new {@link MetadataBackedField} with the given name, {@link RelationalPersistentEntity} and
		 * {@link MappingContext} with the given {@link RelationalPersistentProperty}.
		 *
		 * @param name must not be {@literal null} or empty.
		 * @param entity must not be {@literal null}.
		 * @param context must not be {@literal null}.
		 * @param property may be {@literal null}.
		 */
		protected MetadataBackedField(SqlIdentifier name, RelationalPersistentEntity<?> entity,
				MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> context,
				@Nullable RelationalPersistentProperty property) {

			super(name);

			Assert.notNull(entity, "RelationalPersistentEntity must not be null");

			this.entity = entity;
			this.mappingContext = context;

			this.path = getPath(name.getReference());
			this.property = this.path == null ? property : this.path.getLeafProperty();
		}

		@Override
		public SqlIdentifier getMappedColumnName() {
			return this.path == null || this.path.getLeafProperty() == null ? super.getMappedColumnName()
					: this.path.getLeafProperty().getColumnName();
		}

		/**
		 * Returns the {@link PersistentPropertyPath} for the given {@code pathExpression}.
		 *
		 * @param pathExpression the path expression to use.
		 * @return
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

			if (this.property.getType().isInterface()
					|| (java.lang.reflect.Modifier.isAbstract(this.property.getType().getModifiers()))) {
				return TypeInformation.OBJECT;
			}

			return this.property.getTypeInformation();
		}
	}
}
