/*
 * Copyright 2019-2025 the original author or authors.
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

import org.jspecify.annotations.Nullable;

import org.springframework.data.core.PropertyPath;
import org.springframework.data.core.PropertyReferenceException;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPath;
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
	 * Map the {@link Expression} object to apply field name mapping using {@link Class the type to read}.
	 *
	 * @param expression must not be {@literal null}.
	 * @param entity related {@link RelationalPersistentEntity}, can be {@literal null}.
	 * @return the mapped {@link Expression}.
	 * @since 1.1
	 * @deprecated since 4.0 in favor of {@link #getMappedObjects(Expression, RelationalPersistentEntity)} where usage of
	 *             {@link org.springframework.data.relational.core.mapping.Embedded embeddable properties} can return more
	 *             than one mapped result.
	 */
	@Deprecated(since = "4.0")
	public Expression getMappedObject(Expression expression, @Nullable RelationalPersistentEntity<?> entity) {

		List<Expression> mappedObjects = getMappedObjects(expression, entity);

		if (mappedObjects.isEmpty()) {
			throw new IllegalArgumentException(String.format("Cannot map %s", expression));
		}

		return mappedObjects.get(0);
	}

	/**
	 * Map the {@link Expression} object to apply field name mapping using {@link Class the type to read}.
	 *
	 * @param expression must not be {@literal null}.
	 * @param entity related {@link RelationalPersistentEntity}, can be {@literal null}.
	 * @return the mapped {@link Expression}s.
	 * @since 4.0
	 */
	public List<Expression> getMappedObjects(Expression expression, @Nullable RelationalPersistentEntity<?> entity) {

		if (entity == null || expression instanceof AsteriskFromTable
				|| expression instanceof Expressions.SimpleExpression) {
			return List.of(expression);
		}

		if (expression instanceof Column column) {

			Field field = createPropertyField(entity, column.getName());
			TableLike table = column.getRequiredTable();

			if (field.isEmbedded()) {

				RelationalPersistentEntity<?> embeddedEntity = getMappingContext()
						.getRequiredPersistentEntity(field.getRequiredProperty());

				List<Expression> expressions = new ArrayList<>();

				for (RelationalPersistentProperty embeddedProperty : embeddedEntity) {

					expressions.addAll(getMappedObjects(Column.create(embeddedProperty.getName(), table), embeddedEntity));
				}

				return expressions;
			}

			Column columnFromTable = table.column(field.getMappedColumnName());
			return List.of(column instanceof Aliased ? columnFromTable.as(((Aliased) column).getAlias()) : columnFromTable);
		}

		if (expression instanceof SimpleFunction function) {

			List<Expression> arguments = function.getExpressions();
			List<Expression> mappedArguments = new ArrayList<>(arguments.size());

			for (Expression argument : arguments) {
				mappedArguments.addAll(getMappedObjects(argument, entity));
			}

			SimpleFunction mappedFunction = SimpleFunction.create(function.getFunctionName(), mappedArguments);

			return List.of(function instanceof Aliased ? mappedFunction.as(((Aliased) function).getAlias()) : mappedFunction);
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

			CriteriaDefinition previous = current.getRequiredPrevious();
			forwardChain.put(previous, current);
			current = previous;
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

		SqlIdentifier criteriaColumn = criteria.getColumn();

		Assert.notNull(criteriaColumn, "CriteriaColumn must not be null");

		Field propertyField = createPropertyField(entity, criteriaColumn, this.mappingContext);

		if (propertyField.isEmbedded() && entity != null) {

			Object value = criteria.getValue();

			RelationalPersistentEntity<?> embeddedEntity = mappingContext
					.getRequiredPersistentEntity(propertyField.getRequiredProperty());
			PersistentPropertyAccessor<Object> propertyAccessor = getEmbeddedPropertyAccessor(value, embeddedEntity,
					propertyField);

			Condition condition = Conditions.unrestricted();

			for (RelationalPersistentProperty embeddedProperty : embeddedEntity) {

				Object propertyValue = propertyAccessor.getProperty(embeddedProperty);

				CriteriaWrapper cw = new CriteriaWrapper(criteria) {

					@Override
					public SqlIdentifier getColumn() {
						return SqlIdentifier.unquoted(embeddedProperty.getName());
					}

					@Nullable
					@Override
					public Object getValue() {
						return propertyValue;
					}
				};

				Condition mapped = mapCondition(cw, bindings, table, embeddedEntity);
				condition = condition.and(mapped);
			}

			return condition;
		}

		Column column = table.column(propertyField.getMappedColumnName());
		TypeInformation<?> actualType = propertyField.getTypeHint().getRequiredActualType();

		Object mappedValue;
		Class<?> typeHint;

		Comparator comparator = criteria.getComparator();

		Assert.state(comparator != null, "CriteriaComparator must not be null");

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

	static PersistentPropertyAccessor<Object> getEmbeddedPropertyAccessor(@Nullable Object value,
			RelationalPersistentEntity<?> embeddedEntity, Field propertyField) {

		if (value != null) {

			Class<?> propertyType = embeddedEntity.getType();
			if (!propertyType.isInstance(value)) {
				throw new IllegalArgumentException("Value of property " + propertyField.getRequiredProperty().getName()
						+ " is not an instance of " + embeddedEntity.getType().getName() + " but " + value.getClass().getName());
			}

			return embeddedEntity.getPropertyAccessor(value);
		}

		return new PersistentPropertyAccessor<>() {

			@Override
			public void setProperty(PersistentProperty<?> property, @Nullable Object value) {

			}

			@Override
			public @Nullable Object getProperty(PersistentProperty<?> property) {
				return null;
			}

			@Override
			public Object getBean() {
				throw new UnsupportedOperationException("Can't get bean for null valued embedded property");
			}
		};
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

		Object convertedValue = convertValue(value.getValue(), TypeInformation.OBJECT);

		Assert.state(convertedValue != null, "Value must not be null");

		return Parameter.from(convertedValue);
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

			Assert.state(first != null, "First value must not be null");
			Assert.state(second != null, "Second value must not be null");

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

			Assert.state(pair != null, "Pair must not be null");

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

		Object value = parameter.getValue();

		Assert.state(value != null, "Value must not be null");

		if (mappedValue.getClass().equals(value.getClass())) {
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

	private Expression booleanBind(Column column, @Nullable Object mappedValue, Class<?> valueType,
			MutableBindings bindings, boolean ignoreCase) {
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
		}

		@Override
		public SqlIdentifier getMappedColumnName() {
			return this.property == null ? super.getMappedColumnName() : this.property.getColumnName();
		}

		/**
		 * Returns the {@link PersistentPropertyPath} for the given {@code pathExpression}.
		 *
		 * @param pathExpression the path expression to use.
		 * @return
		 */
		private @Nullable PersistentPropertyPath<RelationalPersistentProperty> getPath(String pathExpression) {

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

		@Override
		public boolean isEmbedded() {
			return this.property != null && this.property.isEmbedded();
		}

		@Override
		public @Nullable RelationalPersistentProperty getProperty() {
			return this.property;
		}
	}

	abstract static class CriteriaWrapper extends AbstractCriteria {

		private final CriteriaDefinition delegate;

		public CriteriaWrapper(CriteriaDefinition delegate) {
			this.delegate = delegate;
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
	}

	abstract static class AbstractCriteria implements CriteriaDefinition {
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
		public Comparator getComparator() {
			return null;
		}

		@Nullable
		@Override
		public Object getValue() {
			return null;
		}

		@Override
		public boolean isIgnoreCase() {
			return false;
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
}
