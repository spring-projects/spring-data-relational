/*
 * Copyright 2019-2020 the original author or authors.
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
import java.util.function.UnaryOperator;

import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.PropertyReferenceException;
import org.springframework.data.mapping.context.InvalidPersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.dialect.BindMarker;
import org.springframework.data.r2dbc.dialect.BindMarkers;
import org.springframework.data.r2dbc.dialect.Bindings;
import org.springframework.data.r2dbc.dialect.MutableBindings;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.r2dbc.mapping.SettableValue;
import org.springframework.data.r2dbc.query.Criteria.Combinator;
import org.springframework.data.r2dbc.query.Criteria.Comparator;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.*;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Maps {@link Criteria} and {@link Sort} objects considering mapping metadata and dialect-specific conversion.
 *
 * @author Mark Paluch
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

		Assert.notNull(converter, "R2dbcConverter must not be null!");
		Assert.notNull(dialect, "R2dbcDialect must not be null!");

		this.converter = converter;
		this.dialect = dialect;
		this.mappingContext = (MappingContext) converter.getMappingContext();
	}

	/**
	 * Render a {@link SqlIdentifier} for SQL usage.
	 *
	 * @param identifier
	 * @return
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
	 */
	public Sort getMappedObject(Sort sort, @Nullable RelationalPersistentEntity<?> entity) {

		if (entity == null) {
			return sort;
		}

		List<Sort.Order> mappedOrder = new ArrayList<>();

		for (Sort.Order order : sort) {

			Field field = createPropertyField(entity, SqlIdentifier.unquoted(order.getProperty()), this.mappingContext);
			mappedOrder.add(
					Sort.Order.by(toSql(field.getMappedColumnName())).with(order.getNullHandling()).with(order.getDirection()));
		}

		return Sort.by(mappedOrder);
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

			Field field = createPropertyField(entity, SqlIdentifier.unquoted(order.getProperty()), this.mappingContext);
			OrderByField orderBy = OrderByField.from(table.column(field.getMappedColumnName()))
					.withNullHandling(order.getNullHandling());
			mappedOrder.add(order.isAscending() ? orderBy.asc() : orderBy.desc());
		}

		return mappedOrder;
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

		if (entity == null || expression instanceof AsteriskFromTable) {
			return expression;
		}

		if (expression instanceof Column) {

			Column column = (Column) expression;
			Field field = createPropertyField(entity, column.getName());
			Table table = column.getTable();

			Column columnFromTable = table.column(field.getMappedColumnName());
			return column instanceof Aliased ? columnFromTable.as(((Aliased) column).getAlias()) : columnFromTable;
		}

		if (expression instanceof SimpleFunction) {

			SimpleFunction function = (SimpleFunction) expression;

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
	 * Map a {@link Criteria} object into {@link Condition} and consider value/{@code NULL} {@link Bindings}.
	 *
	 * @param markers bind markers object, must not be {@literal null}.
	 * @param criteria criteria definition to map, must not be {@literal null}.
	 * @param table must not be {@literal null}.
	 * @param entity related {@link RelationalPersistentEntity}, can be {@literal null}.
	 * @return the mapped {@link BoundCondition}.
	 */
	public BoundCondition getMappedObject(BindMarkers markers, Criteria criteria, Table table,
			@Nullable RelationalPersistentEntity<?> entity) {

		Assert.notNull(markers, "BindMarkers must not be null!");
		Assert.notNull(criteria, "Criteria must not be null!");
		Assert.notNull(table, "Table must not be null!");

		MutableBindings bindings = new MutableBindings(markers);

		if (criteria.isEmpty()) {
			throw new IllegalArgumentException("Cannot map empty Criteria");
		}

		Condition mapped = unroll(criteria, table, entity, bindings);

		return new BoundCondition(bindings, mapped);
	}

	private Condition unroll(Criteria criteria, Table table, @Nullable RelationalPersistentEntity<?> entity,
			MutableBindings bindings) {

		Criteria current = criteria;

		// reverse unroll criteria chain
		Map<Criteria, Criteria> forwardChain = new HashMap<>();

		while (current.hasPrevious()) {
			forwardChain.put(current.getPrevious(), current);
			current = current.getPrevious();
		}

		// perform the actual mapping
		Condition mapped = getCondition(current, bindings, table, entity);
		while (forwardChain.containsKey(current)) {

			Criteria criterion = forwardChain.get(current);
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
	private Condition unrollGroup(List<Criteria> criteria, Table table, Combinator combinator,
			@Nullable RelationalPersistentEntity<?> entity, MutableBindings bindings) {

		Condition mapped = null;
		for (Criteria criterion : criteria) {

			if (criterion.isEmpty()) {
				continue;
			}

			Condition condition = unroll(criterion, table, entity, bindings);

			mapped = combine(criterion, mapped, combinator, condition);
		}

		return mapped;
	}

	@Nullable
	private Condition getCondition(Criteria criteria, MutableBindings bindings, Table table,
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

	private Condition combine(Criteria criteria, @Nullable Condition currentCondition, Combinator combinator,
			Condition nextCondition) {

		if (currentCondition == null) {
			currentCondition = nextCondition;
		} else if (combinator == Combinator.AND) {
			currentCondition = currentCondition.and(nextCondition);
		} else if (combinator == Combinator.OR) {
			currentCondition = currentCondition.or(nextCondition);
		} else {
			throw new IllegalStateException("Combinator " + criteria.getCombinator() + " not supported");
		}

		return currentCondition;
	}

	private Condition mapCondition(Criteria criteria, MutableBindings bindings, Table table,
			@Nullable RelationalPersistentEntity<?> entity) {

		Field propertyField = createPropertyField(entity, criteria.getColumn(), this.mappingContext);
		Column column = table.column(propertyField.getMappedColumnName());
		TypeInformation<?> actualType = propertyField.getTypeHint().getRequiredActualType();

		Object mappedValue;
		Class<?> typeHint;

		if (criteria.getValue() instanceof SettableValue) {

			SettableValue settableValue = (SettableValue) criteria.getValue();

			mappedValue = convertValue(settableValue.getValue(), propertyField.getTypeHint());
			typeHint = getTypeHint(mappedValue, actualType.getType(), settableValue);

		} else {

			mappedValue = convertValue(criteria.getValue(), propertyField.getTypeHint());
			typeHint = actualType.getType();
		}

		return createCondition(column, mappedValue, typeHint, bindings, criteria.getComparator(),
				criteria.isIgnoreCase());
	}

	/**
	 * Potentially convert the {@link SettableValue}.
	 *
	 * @param value
	 * @return
	 */
	public SettableValue getBindValue(SettableValue value) {

		if (value.isEmpty()) {
			return SettableValue.empty(converter.getTargetType(value.getType()));
		}

		return SettableValue.from(convertValue(value.getValue(), ClassTypeInformation.OBJECT));
	}

	@Nullable
	protected Object convertValue(@Nullable Object value, TypeInformation<?> typeInformation) {

		if (value == null) {
			return null;
		}

		if (value instanceof Iterable) {

			List<Object> mapped = new ArrayList<>();

			for (Object o : (Iterable<?>) value) {
				mapped.add(convertValue(o, typeInformation.getActualType() != null ? typeInformation.getRequiredActualType()
						: ClassTypeInformation.OBJECT));
			}

			return mapped;
		}

		if (value.getClass().isArray()
				&& (ClassTypeInformation.OBJECT.equals(typeInformation) || typeInformation.isCollectionLike())) {
			return value;
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
			return column.isEqualTo(SQL.literalOf((Object) ("TRUE")));
		}

		if (comparator == Comparator.IS_FALSE) {
			return column.isEqualTo(SQL.literalOf((Object) ("FALSE")));
		}

		Expression columnExpression = column;
		if (ignoreCase && String.class == valueType) {
			columnExpression = new Upper(column);
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
				return NotLike.create(columnExpression, expression);
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

	Class<?> getTypeHint(@Nullable Object mappedValue, Class<?> propertyType, SettableValue settableValue) {

		if (mappedValue == null || propertyType.equals(Object.class)) {
			return settableValue.getType();
		}

		if (mappedValue.getClass().equals(settableValue.getValue().getClass())) {
			return settableValue.getType();
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

		return ignoreCase ? new Upper(SQL.bindMarker(bindMarker.getPlaceholder()))
				: SQL.bindMarker(bindMarker.getPlaceholder());
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

			Assert.notNull(name, "Name must not be null!");
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
			return ClassTypeInformation.OBJECT;
		}
	}

	/**
	 * Extension of {@link Field} to be backed with mapping metadata.
	 */
	protected static class MetadataBackedField extends Field {

		private final RelationalPersistentEntity<?> entity;
		private final MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> mappingContext;
		private final RelationalPersistentProperty property;
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

			Assert.notNull(entity, "MongoPersistentEntity must not be null!");

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
		 * @param pathExpression
		 * @return
		 */
		@Nullable
		private PersistentPropertyPath<RelationalPersistentProperty> getPath(String pathExpression) {

			try {

				PropertyPath path = PropertyPath.from(pathExpression, this.entity.getTypeInformation());

				if (isPathToJavaLangClassProperty(path)) {
					return null;
				}

				return this.mappingContext.getPersistentPropertyPath(path);
			} catch (PropertyReferenceException | InvalidPersistentPropertyPath e) {
				return null;
			}
		}

		private boolean isPathToJavaLangClassProperty(PropertyPath path) {
			return path.getType().equals(Class.class) && path.getLeafProperty().getOwningType().getType().equals(Class.class);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.Field#getTypeHint()
		 */
		@Override
		public TypeInformation<?> getTypeHint() {

			if (this.property == null) {
				return super.getTypeHint();
			}

			if (this.property.getType().isPrimitive()) {
				return ClassTypeInformation.from(ClassUtils.resolvePrimitiveIfNecessary(this.property.getType()));
			}

			if (this.property.getType().isArray()) {
				return this.property.getTypeInformation();
			}

			if (this.property.getType().isInterface()
					|| (java.lang.reflect.Modifier.isAbstract(this.property.getType().getModifiers()))) {
				return ClassTypeInformation.OBJECT;
			}

			return this.property.getTypeInformation();
		}
	}

	static class PassThruIdentifier implements SqlIdentifier {

		final String name;

		PassThruIdentifier(String name) {
			this.name = name;
		}

		@Override
		public String getReference(IdentifierProcessing processing) {
			return name;
		}

		@Override
		public String toSql(IdentifierProcessing processing) {
			return name;
		}

		@Override
		public SqlIdentifier transform(UnaryOperator<String> transformationFunction) {
			return new PassThruIdentifier(transformationFunction.apply(name));
		}

		/*
		* (non-Javadoc)
		* @see java.lang.Object#equals(java.lang.Object)
		*/
		@Override
		public boolean equals(Object o) {

			if (this == o)
				return true;
			if (o instanceof SqlIdentifier) {
				return toString().equals(o.toString());
			}
			return false;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			return toString().hashCode();
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return toSql(IdentifierProcessing.ANSI);
		}
	}

	// TODO: include support of NOT LIKE operator into spring-data-relational
	/**
	 * Negated LIKE {@link Condition} comparing two {@link Expression}s.
	 * <p/>
	 * Results in a rendered condition: {@code <left> NOT LIKE <right>}.
	 */
	private static class NotLike implements Segment, Condition {
		private final Comparison delegate;

		private NotLike(Expression leftColumnOrExpression, Expression rightColumnOrExpression) {
			this.delegate = Comparison.create(leftColumnOrExpression, "NOT LIKE", rightColumnOrExpression);
		}

		/**
		 * Creates new instance of this class with the given {@link Expression}s.
		 *
		 * @param leftColumnOrExpression the left {@link Expression}
		 * @param rightColumnOrExpression the right {@link Expression}
		 * @return {@link NotLike} condition
		 */
		public static NotLike create(Expression leftColumnOrExpression, Expression rightColumnOrExpression) {
			Assert.notNull(leftColumnOrExpression, "Left expression must not be null!");
			Assert.notNull(rightColumnOrExpression, "Right expression must not be null!");
			return new NotLike(leftColumnOrExpression, rightColumnOrExpression);
		}

		@Override
		public void visit(Visitor visitor) {
			Assert.notNull(visitor, "Visitor must not be null!");
			delegate.visit(visitor);
		}

		@Override
		public String toString() {
			return delegate.toString();
		}
	}

	// TODO: include support of functions in WHERE conditions into spring-data-relational
	/**
	 * Models the ANSI SQL {@code UPPER} function.
	 * <p>
	 * Results in a rendered function: {@code UPPER(<expression>)}.
	 */
	private class Upper implements Expression {
		private Literal<Object> delegate;

		/**
		 * Creates new instance of this class with the given expression. Only expressions of type {@link Column} and
		 * {@link org.springframework.data.relational.core.sql.BindMarker} are supported.
		 *
		 * @param expression expression to be uppercased (must not be {@literal null})
		 */
		private Upper(Expression expression) {
			Assert.notNull(expression, "Expression must not be null!");
			String functionArgument;
			if (expression instanceof org.springframework.data.relational.core.sql.BindMarker) {
				functionArgument = expression instanceof Named ? ((Named) expression).getName().getReference()
						: expression.toString();
			} else if (expression instanceof Column) {
				functionArgument = "";
				Table table = ((Column) expression).getTable();
				if (table != null) {
					functionArgument = toSql(table.getName()) + ".";
				}
				functionArgument += toSql(((Column) expression).getName());
			} else {
				throw new IllegalArgumentException("Unable to ignore case expression of type " + expression.getClass().getName()
						+ ". Only " + Column.class.getName() + " and "
						+ org.springframework.data.relational.core.sql.BindMarker.class.getName() + " types are supported");
			}
			this.delegate = SQL.literalOf((Object) ("UPPER(" + functionArgument + ")"));
		}

		@Override
		public void visit(Visitor visitor) {
			delegate.visit(visitor);
		}

		@Override
		public String toString() {
			return delegate.toString();
		}
	}
}
