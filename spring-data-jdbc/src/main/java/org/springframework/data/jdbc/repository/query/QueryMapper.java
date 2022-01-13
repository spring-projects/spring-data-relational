/*
 * Copyright 2020-2021 the original author or authors.
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
package org.springframework.data.jdbc.repository.query;

import java.sql.JDBCType;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.mapping.JdbcValue;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.PropertyReferenceException;
import org.springframework.data.mapping.context.InvalidPersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.Escaper;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.relational.core.query.CriteriaDefinition.Comparator;
import org.springframework.data.relational.core.query.ValueFunction;
import org.springframework.data.relational.core.sql.*;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.Pair;
import org.springframework.data.util.TypeInformation;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Maps {@link CriteriaDefinition} and {@link Sort} objects considering mapping metadata and dialect-specific
 * conversion.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 2.0
 */
class QueryMapper {

	private final JdbcConverter converter;
	private final Dialect dialect;
	private final MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> mappingContext;

	/**
	 * Creates a new {@link QueryMapper} with the given {@link JdbcConverter}.
	 *
	 * @param dialect must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	QueryMapper(Dialect dialect, JdbcConverter converter) {

		Assert.notNull(dialect, "Dialect must not be null!");
		Assert.notNull(converter, "JdbcConverter must not be null!");

		this.converter = converter;
		this.dialect = dialect;
		this.mappingContext = (MappingContext) converter.getMappingContext();
	}

	/**
	 * Map the {@link Sort} object to apply field name mapping using {@link RelationalPersistentEntity the type to read}.
	 *
	 * @param sort must not be {@literal null}.
	 * @param entity related {@link RelationalPersistentEntity}, can be {@literal null}.
	 * @return
	 */
	List<OrderByField> getMappedSort(Table table, Sort sort, @Nullable RelationalPersistentEntity<?> entity) {

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

		if (expression instanceof Column) {

			Column column = (Column) expression;
			Field field = createPropertyField(entity, column.getName());
			TableLike table = column.getTable();

			Assert.state(table != null, String.format("The column %s must have a table set.", column));

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
	 * Map a {@link CriteriaDefinition} object into {@link Condition} and consider value/{@code NULL} bindings.
	 *
	 * @param parameterSource bind parameterSource object, must not be {@literal null}.
	 * @param criteria criteria definition to map, must not be {@literal null}.
	 * @param table must not be {@literal null}.
	 * @param entity related {@link RelationalPersistentEntity}, can be {@literal null}.
	 * @return the mapped {@link Condition}.
	 */
	Condition getMappedObject(MapSqlParameterSource parameterSource, CriteriaDefinition criteria, Table table,
			@Nullable RelationalPersistentEntity<?> entity) {

		Assert.notNull(parameterSource, "MapSqlParameterSource must not be null!");
		Assert.notNull(criteria, "CriteriaDefinition must not be null!");
		Assert.notNull(table, "Table must not be null!");

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
			forwardChain.put(current.getPrevious(), current);
			current = current.getPrevious();
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

		return mapCondition(criteria, parameterSource, table, entity);
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
			@Nullable RelationalPersistentEntity<?> entity) {

		Field propertyField = createPropertyField(entity, criteria.getColumn(), this.mappingContext);

		// Single embedded entity
		if (propertyField.isEmbedded()) {
			return mapEmbeddedObjectCondition(criteria, parameterSource, table,
					((MetadataBackedField) propertyField).getPath().getLeafProperty());
		}

		TypeInformation<?> actualType = propertyField.getTypeHint().getRequiredActualType();
		Column column = table.column(propertyField.getMappedColumnName());
		Object mappedValue;
		int sqlType;

		if (criteria.getValue() instanceof JdbcValue) {

			JdbcValue settableValue = (JdbcValue) criteria.getValue();

			mappedValue = convertValue(settableValue.getValue(), propertyField.getTypeHint());
			sqlType = getTypeHint(mappedValue, actualType.getType(), settableValue);
		} else if (criteria.getValue() instanceof ValueFunction) {

			ValueFunction<Object> valueFunction = (ValueFunction<Object>) criteria.getValue();
			Object value = valueFunction.apply(getEscaper(criteria.getComparator()));

			mappedValue = convertValue(value, propertyField.getTypeHint());
			sqlType = propertyField.getSqlType();

		} else if (propertyField instanceof MetadataBackedField //
				&& ((MetadataBackedField) propertyField).property != null //
				&& (criteria.getValue() == null || !criteria.getValue().getClass().isArray())) {

			RelationalPersistentProperty property = ((MetadataBackedField) propertyField).property;
			JdbcValue jdbcValue = convertToJdbcValue(property, criteria.getValue());
			mappedValue = jdbcValue.getValue();
			sqlType = jdbcValue.getJdbcType() != null ? jdbcValue.getJdbcType().getVendorTypeNumber()
					: propertyField.getSqlType();

		} else {

			mappedValue = convertValue(criteria.getValue(), propertyField.getTypeHint());
			sqlType = propertyField.getSqlType();
		}

		return createCondition(column, mappedValue, sqlType, parameterSource, criteria.getComparator(),
				criteria.isIgnoreCase());
	}

	/**
	 * Converts values while taking specific value types like arrays, {@link Iterable}, or {@link Pair}.
	 *
	 * @param property the property to which the value relates. It determines the type to convert to. Must not be
	 *          {@literal null}.
	 * @param value the value to be converted.
	 * @return a non null {@link JdbcValue} holding the converted value and the appropriate JDBC type information.
	 */
	private JdbcValue convertToJdbcValue(RelationalPersistentProperty property, @Nullable Object value) {

		if (value == null) {
			return JdbcValue.of(null, JDBCType.NULL);
		}

		if (value instanceof Pair) {

			JdbcValue first = getWriteValue(property, ((Pair<?, ?>) value).getFirst());
			JdbcValue second = getWriteValue(property, ((Pair<?, ?>) value).getSecond());
			return JdbcValue.of(Pair.of(first.getValue(), second.getValue()), first.getJdbcType());
		}

		if (value instanceof Iterable) {

			List<Object> mapped = new ArrayList<>();
			JDBCType jdbcType = null;

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
			Object[] mappedValueArray = new Object[valueAsArray.length];
			JDBCType jdbcType = null;

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
				converter.getSqlType(property) //
		);
	}

	private Condition mapEmbeddedObjectCondition(CriteriaDefinition criteria, MapSqlParameterSource parameterSource,
			Table table, RelationalPersistentProperty embeddedProperty) {

		RelationalPersistentEntity<?> persistentEntity = this.mappingContext.getRequiredPersistentEntity(embeddedProperty);

		Assert.isInstanceOf(persistentEntity.getType(), criteria.getValue(),
				() -> "Value must be of type " + persistentEntity.getType().getName() + " for embedded entity matching");

		PersistentPropertyAccessor<Object> embeddedAccessor = persistentEntity.getPropertyAccessor(criteria.getValue());

		String prefix = embeddedProperty.getEmbeddedPrefix();
		Condition condition = null;
		for (RelationalPersistentProperty nestedProperty : persistentEntity) {

			SqlIdentifier sqlIdentifier = nestedProperty.getColumnName().transform(prefix::concat);
			Object mappedNestedValue = convertValue(embeddedAccessor.getProperty(nestedProperty),
					nestedProperty.getTypeInformation());
			int sqlType = converter.getSqlType(nestedProperty);

			Condition mappedCondition = createCondition(table.column(sqlIdentifier), mappedNestedValue, sqlType,
					parameterSource, criteria.getComparator(), criteria.isIgnoreCase());

			if (condition != null) {
				condition = condition.and(mappedCondition);
			} else {
				condition = mappedCondition;
			}
		}

		return Conditions.nest(condition);
	}

	private Escaper getEscaper(Comparator comparator) {

		if (comparator == Comparator.LIKE || comparator == Comparator.NOT_LIKE) {
			return dialect.getLikeEscaper();
		}

		return Escaper.DEFAULT;
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
					: ClassTypeInformation.OBJECT);

			Object second = convertValue(pair.getSecond(), typeInformation.getActualType() != null //
					? typeInformation.getRequiredActualType()
					: ClassTypeInformation.OBJECT);

			return Pair.of(first, second);
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

	private Condition createCondition(Column column, @Nullable Object mappedValue, int sqlType,
			MapSqlParameterSource parameterSource, Comparator comparator, boolean ignoreCase) {

		if (comparator.equals(Comparator.IS_NULL)) {
			return column.isNull();
		}

		if (comparator.equals(Comparator.IS_NOT_NULL)) {
			return column.isNotNull();
		}

		if (comparator == Comparator.IS_TRUE) {

			Expression bind = bindBoolean(column, parameterSource, true);
			return column.isEqualTo(bind);
		}

		if (comparator == Comparator.IS_FALSE) {

			Expression bind = bindBoolean(column, parameterSource, false);
			return column.isEqualTo(bind);
		}

		Expression columnExpression = column;
		if (ignoreCase && (sqlType == Types.VARCHAR || sqlType == Types.NVARCHAR)) {
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

			Pair<Object, Object> pair = (Pair<Object, Object>) mappedValue;

			Expression begin = bind(pair.getFirst(), sqlType, parameterSource, column.getName().getReference(), ignoreCase);
			Expression end = bind(pair.getSecond(), sqlType, parameterSource, column.getName().getReference(), ignoreCase);

			return comparator == Comparator.BETWEEN ? Conditions.between(columnExpression, begin, end)
					: Conditions.notBetween(columnExpression, begin, end);
		}

		String refName = column.getName().getReference();

		switch (comparator) {
			case EQ: {
				Expression expression = bind(mappedValue, sqlType, parameterSource, refName, ignoreCase);
				return Conditions.isEqual(columnExpression, expression);
			}
			case NEQ: {
				Expression expression = bind(mappedValue, sqlType, parameterSource, refName, ignoreCase);
				return Conditions.isEqual(columnExpression, expression).not();
			}
			case LT: {
				Expression expression = bind(mappedValue, sqlType, parameterSource, refName);
				return column.isLess(expression);
			}
			case LTE: {
				Expression expression = bind(mappedValue, sqlType, parameterSource, refName);
				return column.isLessOrEqualTo(expression);
			}
			case GT: {
				Expression expression = bind(mappedValue, sqlType, parameterSource, refName);
				return column.isGreater(expression);
			}
			case GTE: {
				Expression expression = bind(mappedValue, sqlType, parameterSource, refName);
				return column.isGreaterOrEqualTo(expression);
			}
			case LIKE: {
				Expression expression = bind(mappedValue, sqlType, parameterSource, refName, ignoreCase);
				return Conditions.like(columnExpression, expression);
			}
			case NOT_LIKE: {
				Expression expression = bind(mappedValue, sqlType, parameterSource, refName, ignoreCase);
				return Conditions.notLike(columnExpression, expression);
			}
			default:
				throw new UnsupportedOperationException("Comparator " + comparator + " not supported");
		}
	}

	private Expression bindBoolean(Column column, MapSqlParameterSource parameterSource, boolean value) {

		Object converted = converter.writeValue(value, ClassTypeInformation.OBJECT);
		return bind(converted, Types.BIT, parameterSource, column.getName().getReference());
	}

	Field createPropertyField(@Nullable RelationalPersistentEntity<?> entity, SqlIdentifier key) {
		return entity == null ? new Field(key) : new MetadataBackedField(key, entity, mappingContext, converter);
	}

	Field createPropertyField(@Nullable RelationalPersistentEntity<?> entity, SqlIdentifier key,
			MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> mappingContext) {
		return entity == null ? new Field(key) : new MetadataBackedField(key, entity, mappingContext, converter);
	}

	int getTypeHint(@Nullable Object mappedValue, Class<?> propertyType, JdbcValue settableValue) {

		if (mappedValue == null || propertyType.equals(Object.class)) {
			return JdbcUtils.TYPE_UNKNOWN;
		}

		if (mappedValue.getClass().equals(settableValue.getValue().getClass())) {
			return JdbcUtils.TYPE_UNKNOWN;
		}

		return settableValue.getJdbcType().getVendorTypeNumber();
	}

	private Expression bind(@Nullable Object mappedValue, int sqlType, MapSqlParameterSource parameterSource,
			String name) {
		return bind(mappedValue, sqlType, parameterSource, name, false);
	}

	private Expression bind(@Nullable Object mappedValue, int sqlType, MapSqlParameterSource parameterSource, String name,
			boolean ignoreCase) {

		String uniqueName = getUniqueName(parameterSource, name);

		parameterSource.addValue(uniqueName, mappedValue, sqlType);

		return ignoreCase ? Functions.upper(SQL.bindMarker(":" + uniqueName)) : SQL.bindMarker(":" + uniqueName);
	}

	private static String getUniqueName(MapSqlParameterSource parameterSource, String name) {

		Map<String, Object> values = parameterSource.getValues();

		if (!values.containsKey(name)) {
			return name;
		}

		int counter = 1;
		String uniqueName;

		do {
			uniqueName = name + (counter++);
		} while (values.containsKey(uniqueName));

		return uniqueName;
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

			Assert.notNull(name, "Name must not be null!");
			this.name = name;
		}

		public boolean isEmbedded() {
			return false;
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

		public int getSqlType() {
			return JdbcUtils.TYPE_UNKNOWN;
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
		private final boolean embedded;
		private final int sqlType;

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
			this(name, entity, context, null, converter);
		}

		/**
		 * Creates a new {@link MetadataBackedField} with the given name, {@link RelationalPersistentEntity} and
		 * {@link MappingContext} with the given {@link RelationalPersistentProperty}.
		 *
		 * @param name must not be {@literal null} or empty.
		 * @param entity must not be {@literal null}.
		 * @param context must not be {@literal null}.
		 * @param property may be {@literal null}.
		 * @param converter may be {@literal null}.
		 */
		protected MetadataBackedField(SqlIdentifier name, RelationalPersistentEntity<?> entity,
				MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> context,
				@Nullable RelationalPersistentProperty property, JdbcConverter converter) {

			super(name);

			Assert.notNull(entity, "MongoPersistentEntity must not be null!");

			this.entity = entity;
			this.mappingContext = context;

			this.path = getPath(name.getReference());
			this.property = this.path == null ? property : this.path.getLeafProperty();
			this.sqlType = this.property != null ? converter.getSqlType(this.property) : JdbcUtils.TYPE_UNKNOWN;

			if (this.property != null) {
				this.embedded = this.property.isEmbedded();
			} else {
				this.embedded = false;
			}
		}

		@Override
		public SqlIdentifier getMappedColumnName() {

			if (isEmbedded()) {
				throw new IllegalStateException("Cannot obtain a single column name for embedded property");
			}

			if (this.property != null && this.path != null) {

				RelationalPersistentProperty owner = this.path.getParentPath().getLeafProperty();

				if (owner != null && owner.isEmbedded()) {
					return this.property.getColumnName()
							.transform(it -> Objects.requireNonNull(owner.getEmbeddedPrefix()).concat(it));
				}
			}

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

		@Nullable
		public PersistentPropertyPath<RelationalPersistentProperty> getPath() {
			return path;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.Field#isEmbedded()
		 */
		@Override
		public boolean isEmbedded() {
			return this.embedded;
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

			return this.property.getTypeInformation();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.Field#getSqlType()
		 */
		@Override
		public int getSqlType() {
			return this.sqlType;
		}
	}
}
