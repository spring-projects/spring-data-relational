/*
 * Copyright 2018-2024 the original author or authors.
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

import io.r2dbc.spi.Readable;
import io.r2dbc.spi.ReadableMetadata;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.r2dbc.convert.EntityRowMapper;
import org.springframework.data.r2dbc.convert.MappingR2dbcConverter;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.convert.R2dbcCustomConversions;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.r2dbc.query.UpdateMapper;
import org.springframework.data.r2dbc.support.ArrayUtils;
import org.springframework.data.relational.core.dialect.ArrayColumns;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.domain.RowDocument;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.r2dbc.core.PreparedOperation;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

/**
 * Default {@link ReactiveDataAccessStrategy} implementation.
 *
 * @author Mark Paluch
 * @author Louis Morgan
 * @author Jens Schauder
 */
public class DefaultReactiveDataAccessStrategy implements ReactiveDataAccessStrategy {

	private final R2dbcDialect dialect;
	private final R2dbcConverter converter;
	private final UpdateMapper updateMapper;
	private final MappingContext<RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext;
	private final StatementMapper statementMapper;
	private final NamedParameterExpander expander = new NamedParameterExpander();

	/**
	 * Creates a new {@link DefaultReactiveDataAccessStrategy} given {@link R2dbcDialect} and optional
	 * {@link org.springframework.core.convert.converter.Converter}s.
	 *
	 * @param dialect the {@link R2dbcDialect} to use.
	 */
	public DefaultReactiveDataAccessStrategy(R2dbcDialect dialect) {
		this(dialect, Collections.emptyList());
	}

	/**
	 * Creates a new {@link DefaultReactiveDataAccessStrategy} given {@link R2dbcDialect} and optional
	 * {@link org.springframework.core.convert.converter.Converter}s.
	 *
	 * @param dialect the {@link R2dbcDialect} to use.
	 * @param converters custom converters to register, must not be {@literal null}.
	 * @see R2dbcCustomConversions
	 * @see org.springframework.core.convert.converter.Converter
	 */
	public DefaultReactiveDataAccessStrategy(R2dbcDialect dialect, Collection<?> converters) {
		this(dialect, createConverter(dialect, converters));
	}

	/**
	 * Creates a new {@link R2dbcConverter} given {@link R2dbcDialect} and custom {@code converters}.
	 *
	 * @param dialect must not be {@literal null}.
	 * @param converters must not be {@literal null}.
	 * @return the {@link R2dbcConverter}.
	 */
	public static R2dbcConverter createConverter(R2dbcDialect dialect, Collection<?> converters) {

		Assert.notNull(dialect, "Dialect must not be null");
		Assert.notNull(converters, "Converters must not be null");

		R2dbcCustomConversions customConversions = R2dbcCustomConversions.of(dialect, converters);

		R2dbcMappingContext context = new R2dbcMappingContext();
		context.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());

		return new MappingR2dbcConverter(context, customConversions);
	}

	/**
	 * Creates a new {@link DefaultReactiveDataAccessStrategy} given {@link R2dbcDialect} and {@link R2dbcConverter}.
	 *
	 * @param dialect the {@link R2dbcDialect} to use.
	 * @param converter must not be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	public DefaultReactiveDataAccessStrategy(R2dbcDialect dialect, R2dbcConverter converter) {

		Assert.notNull(dialect, "Dialect must not be null");
		Assert.notNull(converter, "RelationalConverter must not be null");

		this.converter = converter;
		this.updateMapper = new UpdateMapper(dialect, converter);
		this.mappingContext = (MappingContext<RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty>) this.converter
				.getMappingContext();
		this.dialect = dialect;

		RenderContextFactory factory = new RenderContextFactory(dialect);
		this.statementMapper = new DefaultStatementMapper(dialect, factory.createRenderContext(), this.updateMapper,
				this.mappingContext);
	}

	@Override
	public List<SqlIdentifier> getAllColumns(Class<?> entityType) {

		RelationalPersistentEntity<?> persistentEntity = getPersistentEntity(entityType);

		if (persistentEntity == null) {
			return Collections.singletonList(SqlIdentifier.unquoted("*"));
		}

		List<SqlIdentifier> columnNames = new ArrayList<>();
		for (RelationalPersistentProperty property : persistentEntity) {
			columnNames.add(property.getColumnName());
		}

		return columnNames;
	}

	@Override
	public List<SqlIdentifier> getIdentifierColumns(Class<?> entityType) {

		RelationalPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(entityType);

		List<SqlIdentifier> columnNames = new ArrayList<>();
		for (RelationalPersistentProperty property : persistentEntity) {

			if (property.isIdProperty()) {
				columnNames.add(property.getColumnName());
			}
		}

		return columnNames;
	}

	public OutboundRow getOutboundRow(Object object) {

		Assert.notNull(object, "Entity object must not be null");

		OutboundRow row = new OutboundRow();

		this.converter.write(object, row);

		RelationalPersistentEntity<?> entity = getRequiredPersistentEntity(ClassUtils.getUserClass(object));

		for (RelationalPersistentProperty property : entity) {

			Parameter value = row.get(property.getColumnName());
			if (value != null && shouldConvertArrayValue(property, value)) {

				Parameter writeValue = getArrayValue(value, property);
				row.put(property.getColumnName(), writeValue);
			}
		}

		return row;
	}

	private boolean shouldConvertArrayValue(RelationalPersistentProperty property, Parameter value) {

		if (!property.isCollectionLike()) {
			return false;
		}

		if (value.hasValue() && (value.getValue() instanceof Collection || value.getValue().getClass().isArray())) {
			return true;
		}

		if (Collection.class.isAssignableFrom(value.getType()) || value.getType().isArray()) {
			return true;
		}

		return false;
	}

	private Parameter getArrayValue(Parameter value, RelationalPersistentProperty property) {

		if (value.getType().equals(byte[].class)) {
			return value;
		}

		ArrayColumns arrayColumns = this.dialect.getArraySupport();

		if (!arrayColumns.isSupported()) {
			throw new InvalidDataAccessResourceUsageException(
					"Dialect " + this.dialect.getClass().getName() + " does not support array columns");
		}

		Class<?> actualType = null;
		if (value.getValue() instanceof Collection) {
			actualType = CollectionUtils.findCommonElementType((Collection<?>) value.getValue());
		} else if (!value.isEmpty() && value.getValue().getClass().isArray()) {
			actualType = value.getValue().getClass().getComponentType();
		}

		if (actualType == null) {
			actualType = property.getActualType();
		}

		actualType = converter.getTargetType(actualType);

		if (value.isEmpty()) {

			Class<?> targetType = arrayColumns.getArrayType(actualType);
			int depth = actualType.isArray() ? ArrayUtils.getDimensionDepth(actualType) : 1;
			Class<?> targetArrayType = ArrayUtils.getArrayClass(targetType, depth);
			return Parameter.empty(targetArrayType);
		}

		return Parameter.fromOrEmpty(this.converter.getArrayValue(arrayColumns, property, value.getValue()), actualType);
	}

	@Override
	public Parameter getBindValue(Parameter value) {
		return this.updateMapper.getBindValue(value);
	}

	@Override
	public <T> BiFunction<Row, RowMetadata, T> getRowMapper(Class<T> typeToRead) {
		return new EntityRowMapper<>(typeToRead, this.converter);
	}

	@Override
	public RowDocument toRowDocument(Class<?> type, Readable row, Iterable<? extends ReadableMetadata> metadata) {
		return this.converter.toRowDocument(type, row, metadata);
	}

	@Override
	public PreparedOperation<?> processNamedParameters(String query, NamedParameterProvider parameterProvider) {

		List<String> parameterNames = this.expander.getParameterNames(query);

		Map<String, Parameter> namedBindings = new LinkedHashMap<>(parameterNames.size());
		for (String parameterName : parameterNames) {

			Parameter value = parameterProvider.getParameter(parameterNames.indexOf(parameterName), parameterName);

			if (value == null) {
				throw new InvalidDataAccessApiUsageException(
						String.format("No parameter specified for [%s] in query [%s]", parameterName, query));
			}

			namedBindings.put(parameterName, value);
		}

		return this.expander.expand(query, this.dialect.getBindMarkersFactory(), new MapBindParameterSource(namedBindings));
	}

	@Override
	public SqlIdentifier getTableName(Class<?> type) {
		return getRequiredPersistentEntity(type).getQualifiedTableName();
	}

	@Override
	public String toSql(SqlIdentifier identifier) {
		return this.updateMapper.toSql(identifier);
	}

	@Override
	public StatementMapper getStatementMapper() {
		return this.statementMapper;
	}

	@Override
	public R2dbcConverter getConverter() {
		return this.converter;
	}

	public MappingContext<RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> getMappingContext() {
		return this.mappingContext;
	}

	@Override
	public String renderForGeneratedValues(SqlIdentifier identifier) {
		return dialect.renderForGeneratedValues(identifier);
	}

	/**
	 * @since 3.4
	 */
	@Override
	public Dialect getDialect() {
		return dialect;
	}

	private RelationalPersistentEntity<?> getRequiredPersistentEntity(Class<?> typeToRead) {
		return this.mappingContext.getRequiredPersistentEntity(typeToRead);
	}

	@Nullable
	private RelationalPersistentEntity<?> getPersistentEntity(Class<?> typeToRead) {
		return this.mappingContext.getPersistentEntity(typeToRead);
	}
}
