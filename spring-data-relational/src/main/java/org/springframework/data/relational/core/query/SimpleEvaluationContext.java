/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.relational.core.query;

import java.util.regex.Pattern;

import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.PropertyReferenceException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.binding.BindMarkers;
import org.springframework.data.relational.core.binding.BindMarkersFactory;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.BindMarker;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.TableLike;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * @author Mark Paluch
 */
public class SimpleEvaluationContext implements QueryExpression.EvaluationContext {

	private final Table table;
	private final BindMarkers bindMarkers;
	private final Bindings bindings;
	private final RelationalConverter converter;
	private final RelationalPersistentEntity<?> entity;
	private final QueryExpression.ExpressionTypeContext type;

	public SimpleEvaluationContext(Table table, BindMarkersFactory bindMarkersFactory, Bindings bindings,
			RelationalConverter converter,
			RelationalPersistentEntity<?> entity) {

		this.table = table;
		this.bindMarkers = bindMarkersFactory.create();
		this.bindings = bindings;
		this.converter = converter;
		this.entity = entity;
		this.type = QueryExpression.ExpressionTypeContext.object();
	}

	public SimpleEvaluationContext(Table table, BindMarkers bindMarkers, Bindings bindings, RelationalConverter converter,
			RelationalPersistentEntity<?> entity, QueryExpression.ExpressionTypeContext type) {

		Assert.notNull(table, "Table must not be null");
		Assert.notNull(type, "Type must not be null");

		this.table = table;
		this.bindMarkers = bindMarkers;
		this.bindings = bindings;
		this.converter = converter;
		this.entity = entity;
		this.type = type;
	}

	public Bindings getBindings() {
		return bindings;
	}

	private Field createPropertyField(SqlIdentifier key) {
		return new MetadataBackedField(key, entity, converter.getMappingContext());
	}

	@Override
	public QueryExpression.MappedColumn getColumn(SqlIdentifier identifier) {

		Field propertyField = createPropertyField(identifier);
		return new DefaultMappedColumn(table, table.column(propertyField.getMappedColumnName()), propertyField);
	}

	// TODO: Some tables can come from a JOIN, see JDBC
	@Override
	public QueryExpression.MappedColumn getColumn(String column) {

		Field propertyField = createPropertyField(SqlIdentifier.unquoted(column));
		return new DefaultMappedColumn(table, table.column(propertyField.getMappedColumnName()), propertyField);
	}

	@Override
	public QueryExpression.EvaluationContext withType(QueryExpression.ExpressionTypeContext type) {
		return new SimpleEvaluationContext(table, bindMarkers, bindings, converter, entity, type);
	}

	@Override
	public BindMarker bind(Object value) {

		BindMarker bindMarker = bindMarkers.next();

		bindings.bind(bindMarker, converter.writeValue(value, type.getTargetType()));

		return bindMarker;
	}

	@Override
	public BindMarker bind(String name, Object value) {

		BindMarker bindMarker = bindMarkers.next(name);

		bindings.bind(bindMarker, converter.writeValue(value, type.getTargetType()));

		return bindMarker;
	}

	class DefaultMappedColumn implements QueryExpression.MappedColumn {

		private final TableLike table;
		private final Column column;
		private final Field field;

		DefaultMappedColumn(TableLike table, Column column, Field field) {
			this.table = table;
			this.column = column;
			this.field = field;
		}

		@Override
		public Expression toExpression() {
			return column;
		}

		@Override
		public TypeInformation<?> getTargetType() {
			return field.getTypeHint();
		}

		@Nullable
		@Override
		public RelationalPersistentProperty getProperty() {
			return field.getProperty();
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

		public @Nullable RelationalPersistentProperty getProperty() {
			return null;
		}
	}

	/**
	 * Extension of {@link Field} to be backed with mapping metadata.
	 */
	protected static class MetadataBackedField extends Field {

		private final RelationalPersistentEntity<?> entity;
		private final MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext;
		private final @Nullable RelationalPersistentProperty property;
		private final @Nullable PersistentPropertyPath<? extends RelationalPersistentProperty> path;

		/**
		 * Creates a new {@link MetadataBackedField} with the given name, {@link RelationalPersistentEntity} and
		 * {@link MappingContext}.
		 *
		 * @param name must not be {@literal null} or empty.
		 * @param entity must not be {@literal null}.
		 * @param context must not be {@literal null}.
		 */
		protected MetadataBackedField(SqlIdentifier name, RelationalPersistentEntity<?> entity,
				MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context) {
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
				MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
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
		private PersistentPropertyPath<? extends RelationalPersistentProperty> getPath(String pathExpression) {

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

		@Nullable
		@Override
		public RelationalPersistentProperty getProperty() {
			return this.property;
		}
	}
}
