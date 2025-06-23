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
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * @author Mark Paluch
 */
public class SimpleQueryRenderContext implements QueryExpression.QueryRenderContext {

	private final Table table;
	private final BindMarkers bindMarkers;
	private final RelationalConverter converter;
	private final RelationalPersistentEntity<?> entity;
	private final @Nullable Field field;

	public SimpleQueryRenderContext(Table table, BindMarkersFactory bindMarkersFactory, RelationalConverter converter,
			RelationalPersistentEntity<?> entity) {

		this.table = table;
		this.bindMarkers = bindMarkersFactory.create();
		this.converter = converter;
		this.entity = entity;
		this.field = null;
	}

	public SimpleQueryRenderContext(Table table, BindMarkers bindMarkers, RelationalConverter converter,
			RelationalPersistentEntity<?> entity, @Nullable Field field) {
		this.table = table;
		this.bindMarkers = bindMarkers;
		this.converter = converter;
		this.entity = entity;
		this.field = field;
	}

	Field createPropertyField(SqlIdentifier key) {
		return entity == null ? new Field(key) : new MetadataBackedField(key, entity, converter.getMappingContext());
	}

	Field createPropertyField(SqlIdentifier key,
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext) {
		return entity == null ? new Field(key) : new MetadataBackedField(key, entity, mappingContext);
	}

	@Override
	public QueryExpression.QueryRenderContext withProperty(String dotPath) {
		// todo: check RelationalMappingContext.isForceQuote() and use SqlIdentifier.quoted() if necessary
		return new SimpleQueryRenderContext(table, bindMarkers, converter, entity,
				createPropertyField(SqlIdentifier.unquoted(dotPath)));
	}

	@Override
	public QueryExpression.QueryRenderContext withProperty(SqlIdentifier identifier) {
		return new SimpleQueryRenderContext(table, bindMarkers, converter, entity, createPropertyField(identifier));
	}

	@Override
	public Expression getColumnName(SqlIdentifier identifier) {

		if (field == null) {
			return withProperty(identifier).getColumnName();
		}
		return table.column(createPropertyField(identifier).getMappedColumnName());
	}

	@Override
	public Expression getColumnName(String dotPath) {

		if (field == null) {
			return withProperty(dotPath).getColumnName();
		}

		return table.column(createPropertyField(SqlIdentifier.unquoted(dotPath)).getMappedColumnName());
	}

	@Override
	public BindMarker bind(Object value) {

		// TODO
		return bindMarkers.next();
	}

	@Override
	public BindMarker bind(String name, Object value) {

		// TODO
		return bindMarkers.next(name);
	}

	@Override
	public Object writeValue(Object value) {
		return converter.writeValue(value, field == null ? TypeInformation.OBJECT : field.getTypeHint());
	}

	@Override
	public Expression getColumnName() {

		if (field == null) {
			throw new IllegalStateException("RenderContext not associated with a field. Call withProperty(…) first.");
		}

		return table.column(field.getMappedColumnName());
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
	}
}
