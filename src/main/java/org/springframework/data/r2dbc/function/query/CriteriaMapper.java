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
package org.springframework.data.r2dbc.function.query;

import static org.springframework.data.r2dbc.function.query.Criteria.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.PropertyReferenceException;
import org.springframework.data.mapping.context.InvalidPersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.r2dbc.dialect.BindMarker;
import org.springframework.data.r2dbc.dialect.BindMarkers;
import org.springframework.data.r2dbc.domain.Bindings;
import org.springframework.data.r2dbc.domain.MutableBindings;
import org.springframework.data.r2dbc.function.convert.R2dbcConverter;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Maps a {@link Criteria} to {@link Condition} considering mapping metadata.
 *
 * @author Mark Paluch
 */
public class CriteriaMapper {

	private final R2dbcConverter converter;
	private final MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> mappingContext;

	/**
	 * Creates a new {@link CriteriaMapper} with the given {@link R2dbcConverter}.
	 *
	 * @param converter must not be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	public CriteriaMapper(R2dbcConverter converter) {

		Assert.notNull(converter, "R2dbcConverter must not be null!");

		this.converter = converter;
		this.mappingContext = (MappingContext) converter.getMappingContext();
	}

	/**
	 * Map a {@link Criteria} object into {@link Condition} and consider value/{@code NULL} {@link Bindings}.
	 *
	 * @param markers bind markers object, must not be {@literal null}.
	 * @param criteria criteria to map, must not be {@literal null}.
	 * @param table must not be {@literal null}.
	 * @param entity related {@link RelationalPersistentEntity}.
	 * @return the mapped bindings.
	 */
	public BoundCondition getMappedObject(BindMarkers markers, Criteria criteria, Table table,
			@Nullable RelationalPersistentEntity<?> entity) {

		Assert.notNull(markers, "BindMarkers must not be null!");
		Assert.notNull(criteria, "Criteria must not be null!");

		Criteria current = criteria;
		MutableBindings bindings = new MutableBindings(markers);

		// reverse unroll criteria chain
		Map<Criteria, Criteria> forwardChain = new HashMap<>();

		while (current.hasPrevious()) {
			forwardChain.put(current.getPrevious(), current);
			current = current.getPrevious();
		}

		// perform the actual mapping
		Condition mapped = getCondition(current, bindings, table, entity);
		while (forwardChain.containsKey(current)) {

			Criteria nextCriteria = forwardChain.get(current);

			if (nextCriteria.getCombinator() == Combinator.AND) {
				mapped = mapped.and(getCondition(nextCriteria, bindings, table, entity));
			}

			if (nextCriteria.getCombinator() == Combinator.OR) {
				mapped = mapped.or(getCondition(nextCriteria, bindings, table, entity));
			}

			current = nextCriteria;
		}

		return new BoundCondition(bindings, mapped);
	}

	private Condition getCondition(Criteria criteria, MutableBindings bindings, Table table,
			@Nullable RelationalPersistentEntity<?> entity) {

		Field propertyField = createPropertyField(entity, criteria.getProperty(), this.mappingContext);
		Column column = table.column(propertyField.getMappedColumnName());
		Object mappedValue = convertValue(criteria.getValue(), propertyField.getTypeHint());

		TypeInformation<?> actualType = propertyField.getTypeHint().getRequiredActualType();
		return createCondition(column, mappedValue, actualType.getType(), bindings, criteria.getComparator());
	}

	@Nullable

	private Object convertValue(@Nullable Object value, TypeInformation<?> typeInformation) {

		if (value == null) {
			return null;
		}

		if (typeInformation.isCollectionLike()) {
			converter.writeValue(value, typeInformation);
		} else if (value instanceof Iterable) {

			List<Object> mapped = new ArrayList<>();

			for (Object o : (Iterable<?>) value) {

				mapped.add(converter.writeValue(o, typeInformation));
			}
			return mapped;
		}

		return converter.writeValue(value, typeInformation);
	}

	private Condition createCondition(Column column, @Nullable Object mappedValue, Class<?> valueType,
			MutableBindings bindings, Comparator comparator) {

		switch (comparator) {
			case IS_NULL:
				return column.isNull();
			case IS_NOT_NULL:
				return column.isNotNull();
		}

		if (comparator == Comparator.NOT_IN || comparator == Comparator.IN) {

			Condition condition;
			if (mappedValue instanceof Iterable) {

				List<Expression> expressions = new ArrayList<>(
						mappedValue instanceof Collection ? ((Collection) mappedValue).size() : 10);

				for (Object o : (Iterable<?>) mappedValue) {

					BindMarker bindMarker = bindings.nextMarker(column.getName());
					expressions.add(bind(o, valueType, bindings, bindMarker));
				}

				condition = column.in(expressions.toArray(new Expression[0]));

			} else {
				BindMarker bindMarker = bindings.nextMarker(column.getName());
				Expression expression = bind(mappedValue, valueType, bindings, bindMarker);

				condition = column.in(expression);
			}

			if (comparator == Comparator.NOT_IN) {
				condition = condition.not();
			}

			return condition;
		}

		BindMarker bindMarker = bindings.nextMarker(column.getName());
		Expression expression = bind(mappedValue, valueType, bindings, bindMarker);

		switch (comparator) {
			case EQ:
				return column.isEqualTo(expression);
			case NEQ:
				return column.isNotEqualTo(expression);
			case LT:
				return column.isLess(expression);
			case LTE:
				return column.isLessOrEqualTo(expression);
			case GT:
				return column.isGreater(expression);
			case GTE:
				return column.isGreaterOrEqualTo(expression);
			case LIKE:
				return column.like(expression);
		}

		throw new UnsupportedOperationException("Comparator " + comparator + " not supported");
	}

	protected Field createPropertyField(@Nullable RelationalPersistentEntity<?> entity, String key,
			MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> mappingContext) {
		return entity == null ? new Field(key) : new MetadataBackedField(key, entity, mappingContext);
	}

	private Expression bind(@Nullable Object mappedValue, Class<?> valueType, MutableBindings bindings,
			BindMarker bindMarker) {

		if (mappedValue != null) {
			bindings.bind(bindMarker, mappedValue);
		} else {
			bindings.bindNull(bindMarker, valueType);
		}

		return SQL.bindMarker(bindMarker.getPlaceholder());
	}

	/**
	 * Value object to represent a field and its meta-information.
	 */
	protected static class Field {

		protected final String name;

		/**
		 * Creates a new {@link Field} without meta-information but the given name.
		 *
		 * @param name must not be {@literal null} or empty.
		 */
		public Field(String name) {

			Assert.hasText(name, "Name must not be null!");
			this.name = name;
		}

		/**
		 * Returns the underlying {@link RelationalPersistentProperty} backing the field. For path traversals this will be
		 * the property that represents the value to handle. This means it'll be the leaf property for plain paths or the
		 * association property in case we refer to an association somewhere in the path.
		 *
		 * @return can be {@literal null}.
		 */
		@Nullable
		public RelationalPersistentProperty getProperty() {
			return null;
		}

		/**
		 * Returns the {@link RelationalPersistentEntity} that field is owned by.
		 *
		 * @return can be {@literal null}.
		 */
		@Nullable
		public RelationalPersistentEntity<?> getPropertyEntity() {
			return null;
		}

		/**
		 * Returns the key to be used in the mapped document eventually.
		 *
		 * @return
		 */
		public String getMappedColumnName() {
			return name;
		}

		public TypeInformation<?> getTypeHint() {
			return ClassTypeInformation.OBJECT;
		}
	}

	/**
	 * Extension of {@link Field} to be backed with mapping metadata.
	 */
	protected static class MetadataBackedField extends Field {

		private static final String INVALID_ASSOCIATION_REFERENCE = "Invalid path reference %s! Associations can only be pointed to directly or via their id property!";

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
		protected MetadataBackedField(String name, RelationalPersistentEntity<?> entity,
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
		protected MetadataBackedField(String name, RelationalPersistentEntity<?> entity,
				MappingContext<? extends RelationalPersistentEntity<?>, RelationalPersistentProperty> context,
				@Nullable RelationalPersistentProperty property) {

			super(name);

			Assert.notNull(entity, "MongoPersistentEntity must not be null!");

			this.entity = entity;
			this.mappingContext = context;

			this.path = getPath(name);
			this.property = path == null ? property : path.getLeafProperty();
		}

		@Override
		public RelationalPersistentProperty getProperty() {
			return property;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.Field#getEntity()
		 */
		@Override
		public RelationalPersistentEntity<?> getPropertyEntity() {
			RelationalPersistentProperty property = getProperty();
			return property == null ? null : mappingContext.getPersistentEntity(property);
		}

		@Override
		public String getMappedColumnName() {
			return path == null ? name : path.toDotPath(RelationalPersistentProperty::getColumnName);
		}

		@Nullable
		protected PersistentPropertyPath<RelationalPersistentProperty> getPath() {
			return path;
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

				PropertyPath path = PropertyPath.from(pathExpression, entity.getTypeInformation());

				if (isPathToJavaLangClassProperty(path)) {
					return null;
				}

				return mappingContext.getPersistentPropertyPath(path);
			} catch (PropertyReferenceException | InvalidPersistentPropertyPath e) {
				return null;
			}
		}

		private boolean isPathToJavaLangClassProperty(PropertyPath path) {

			if (path.getType().equals(Class.class) && path.getLeafProperty().getOwningType().getType().equals(Class.class)) {
				return true;
			}
			return false;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.Field#getTypeHint()
		 */
		@Override
		public TypeInformation<?> getTypeHint() {

			RelationalPersistentProperty property = getProperty();

			if (property == null) {
				return super.getTypeHint();
			}

			if (property.getActualType().isInterface()
					|| java.lang.reflect.Modifier.isAbstract(property.getActualType().getModifiers())) {
				return ClassTypeInformation.OBJECT;
			}

			return property.getTypeInformation();
		}
	}
}
