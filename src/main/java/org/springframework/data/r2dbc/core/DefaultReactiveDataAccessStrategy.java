/*
 * Copyright 2018-2019 the original author or authors.
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

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.convert.CustomConversions.StoreConversions;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.r2dbc.convert.EntityRowMapper;
import org.springframework.data.r2dbc.convert.MappingR2dbcConverter;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.convert.R2dbcCustomConversions;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.r2dbc.mapping.SettableValue;
import org.springframework.data.r2dbc.query.UpdateMapper;
import org.springframework.data.r2dbc.support.ArrayUtils;
import org.springframework.data.relational.core.dialect.ArrayColumns;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Default {@link ReactiveDataAccessStrategy} implementation.
 *
 * @author Mark Paluch
 */
public class DefaultReactiveDataAccessStrategy implements ReactiveDataAccessStrategy {

	private final R2dbcDialect dialect;
	private final R2dbcConverter converter;
	private final UpdateMapper updateMapper;
	private final MappingContext<RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext;
	private final StatementMapper statementMapper;
	private final NamedParameterExpander expander;

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

		List<Object> storeConverters = new ArrayList<>(dialect.getConverters());
		storeConverters.addAll(R2dbcCustomConversions.STORE_CONVERTERS);

		R2dbcCustomConversions customConversions = new R2dbcCustomConversions(
				StoreConversions.of(dialect.getSimpleTypeHolder(), storeConverters), storeConverters);

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
	public DefaultReactiveDataAccessStrategy(R2dbcDialect dialect, R2dbcConverter converter) {
		this(dialect, converter, new NamedParameterExpander());
	}

	/**
	 * Creates a new {@link DefaultReactiveDataAccessStrategy} given {@link R2dbcDialect} and {@link R2dbcConverter}.
	 *
	 * @param dialect the {@link R2dbcDialect} to use.
	 * @param converter must not be {@literal null}.
	 * @param expander must not be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	public DefaultReactiveDataAccessStrategy(R2dbcDialect dialect, R2dbcConverter converter,
			NamedParameterExpander expander) {

		Assert.notNull(dialect, "Dialect must not be null");
		Assert.notNull(converter, "RelationalConverter must not be null");
		Assert.notNull(expander, "NamedParameterExpander must not be null");

		this.converter = converter;
		this.updateMapper = new UpdateMapper(converter);
		this.mappingContext = (MappingContext<RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty>) this.converter
				.getMappingContext();
		this.dialect = dialect;

		RenderContextFactory factory = new RenderContextFactory(dialect);
		this.statementMapper = new DefaultStatementMapper(dialect, factory.createRenderContext(), this.updateMapper,
				this.mappingContext);
		this.expander = expander;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getAllColumns(java.lang.Class)
	 */
	@Override
	public List<String> getAllColumns(Class<?> entityType) {

		RelationalPersistentEntity<?> persistentEntity = getPersistentEntity(entityType);

		if (persistentEntity == null) {
			return Collections.singletonList("*");
		}

		List<String> columnNames = new ArrayList<>();
		for (RelationalPersistentProperty property : persistentEntity) {
			columnNames.add(property.getColumnName());
		}

		return columnNames;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getIdentifierColumns(java.lang.Class)
	 */
	@Override
	public List<String> getIdentifierColumns(Class<?> entityType) {

		RelationalPersistentEntity<?> persistentEntity = getRequiredPersistentEntity(entityType);

		List<String> columnNames = new ArrayList<>();
		for (RelationalPersistentProperty property : persistentEntity) {

			if (property.isIdProperty()) {
				columnNames.add(property.getColumnName());
			}
		}

		return columnNames;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getOutboundRow(java.lang.Object)
	 */
	public OutboundRow getOutboundRow(Object object) {

		Assert.notNull(object, "Entity object must not be null!");

		OutboundRow row = new OutboundRow();

		this.converter.write(object, row);

		RelationalPersistentEntity<?> entity = getRequiredPersistentEntity(ClassUtils.getUserClass(object));

		for (RelationalPersistentProperty property : entity) {

			SettableValue value = row.get(property.getColumnName());
			if (shouldConvertArrayValue(property, value)) {

				SettableValue writeValue = getArrayValue(value, property);
				row.put(property.getColumnName(), writeValue);
			}
		}

		return row;
	}

	private boolean shouldConvertArrayValue(RelationalPersistentProperty property, SettableValue value) {
		return property.isCollectionLike();
	}

	private SettableValue getArrayValue(SettableValue value, RelationalPersistentProperty property) {

		ArrayColumns arrayColumns = this.dialect.getArraySupport();

		if (!arrayColumns.isSupported()) {

			throw new InvalidDataAccessResourceUsageException(
					"Dialect " + this.dialect.getClass().getName() + " does not support array columns");
		}
		Class<?> actualType = property.getActualType();

		if (value.isEmpty()) {

			Class<?> targetType = arrayColumns.getArrayType(actualType);
			int depth = actualType.isArray() ? ArrayUtils.getDimensionDepth(actualType) : 1;
			Class<?> targetArrayType = ArrayUtils.getArrayClass(targetType, depth);
			return SettableValue.empty(targetArrayType);
		}

		return SettableValue.fromOrEmpty(this.converter.getArrayValue(arrayColumns, property, value.getValue()),
				actualType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getRowMapper(java.lang.Class)
	 */
	@Override
	public <T> BiFunction<Row, RowMetadata, T> getRowMapper(Class<T> typeToRead) {
		return new EntityRowMapper<>(typeToRead, this.converter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy#processNamedParameters(java.lang.String, java.util.Map)
	 */
	@Override
	public PreparedOperation<?> processNamedParameters(String query, Map<String, SettableValue> bindings) {
		return this.expander.expand(query, this.dialect.getBindMarkersFactory(), new MapBindParameterSource(bindings));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getTableName(java.lang.Class)
	 */
	@Override
	public String getTableName(Class<?> type) {
		return getRequiredPersistentEntity(type).getTableName();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getStatementMapper()
	 */
	@Override
	public StatementMapper getStatementMapper() {
		return this.statementMapper;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getConverter()
	 */
	public R2dbcConverter getConverter() {
		return this.converter;
	}

	public MappingContext<RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> getMappingContext() {
		return this.mappingContext;
	}

	private RelationalPersistentEntity<?> getRequiredPersistentEntity(Class<?> typeToRead) {
		return this.mappingContext.getRequiredPersistentEntity(typeToRead);
	}

	@Nullable
	private RelationalPersistentEntity<?> getPersistentEntity(Class<?> typeToRead) {
		return this.mappingContext.getPersistentEntity(typeToRead);
	}
}
