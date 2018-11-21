/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.r2dbc.function;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.r2dbc.dialect.BindMarker;
import org.springframework.data.r2dbc.dialect.BindMarkers;
import org.springframework.data.r2dbc.dialect.Dialect;
import org.springframework.data.r2dbc.dialect.LimitClause;
import org.springframework.data.r2dbc.dialect.LimitClause.Position;
import org.springframework.data.r2dbc.function.convert.EntityRowMapper;
import org.springframework.data.r2dbc.function.convert.SettableValue;
import org.springframework.data.relational.core.conversion.BasicRelationalConverter;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Default {@link ReactiveDataAccessStrategy} implementation.
 *
 * @author Mark Paluch
 */
public class DefaultReactiveDataAccessStrategy implements ReactiveDataAccessStrategy {

	private final RelationalConverter relationalConverter;
	private final Dialect dialect;

	/**
	 * Creates a new {@link DefaultReactiveDataAccessStrategy} given {@link Dialect}.
	 *
	 * @param dialect the {@link Dialect} to use.
	 */
	public DefaultReactiveDataAccessStrategy(Dialect dialect) {
		this(dialect, new BasicRelationalConverter(new RelationalMappingContext()));
	}

	/**
	 * Creates a new {@link DefaultReactiveDataAccessStrategy} given {@link Dialect} and {@link RelationalConverter}.
	 *
	 * @param dialect the {@link Dialect} to use.
	 * @param converter must not be {@literal null}.
	 */
	public DefaultReactiveDataAccessStrategy(Dialect dialect, RelationalConverter converter) {

		Assert.notNull(dialect, "Dialect must not be null");
		Assert.notNull(converter, "RelationalConverter must not be null");

		this.relationalConverter = converter;
		this.dialect = dialect;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getAllFields(java.lang.Class)
	 */
	@Override
	public List<String> getAllColumns(Class<?> typeToRead) {

		RelationalPersistentEntity<?> persistentEntity = getPersistentEntity(typeToRead);

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
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getValuesToInsert(java.lang.Object)
	 */
	@Override
	public List<SettableValue> getValuesToInsert(Object object) {

		Class<?> userClass = ClassUtils.getUserClass(object);

		RelationalPersistentEntity<?> entity = getRequiredPersistentEntity(userClass);
		PersistentPropertyAccessor propertyAccessor = entity.getPropertyAccessor(object);

		List<SettableValue> values = new ArrayList<>();

		for (RelationalPersistentProperty property : entity) {

			Object value = propertyAccessor.getProperty(property);

			if (value == null) {
				continue;
			}

			values.add(new SettableValue(property.getColumnName(), value, property.getType()));
		}

		return values;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getMappedSort(java.lang.Class, org.springframework.data.domain.Sort)
	 */
	@Override
	public Sort getMappedSort(Class<?> typeToRead, Sort sort) {

		RelationalPersistentEntity<?> entity = getPersistentEntity(typeToRead);
		if (entity == null) {
			return sort;
		}

		List<Order> mappedOrder = new ArrayList<>();

		for (Order order : sort) {

			RelationalPersistentProperty persistentProperty = entity.getPersistentProperty(order.getProperty());
			if (persistentProperty == null) {
				mappedOrder.add(order);
			} else {
				mappedOrder
						.add(Order.by(persistentProperty.getColumnName()).with(order.getNullHandling()).with(order.getDirection()));
			}
		}

		return Sort.by(mappedOrder);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getRowMapper(java.lang.Class)
	 */
	@Override
	public <T> BiFunction<Row, RowMetadata, T> getRowMapper(Class<T> typeToRead) {
		return new EntityRowMapper<T>((RelationalPersistentEntity) getRequiredPersistentEntity(typeToRead),
				relationalConverter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getTableName(java.lang.Class)
	 */
	@Override
	public String getTableName(Class<?> type) {
		return getRequiredPersistentEntity(type).getTableName();
	}

	private RelationalPersistentEntity<?> getRequiredPersistentEntity(Class<?> typeToRead) {
		return relationalConverter.getMappingContext().getRequiredPersistentEntity(typeToRead);
	}

	@Nullable
	private RelationalPersistentEntity<?> getPersistentEntity(Class<?> typeToRead) {
		return relationalConverter.getMappingContext().getPersistentEntity(typeToRead);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#insertAndReturnGeneratedKeys(java.lang.String, java.util.Set)
	 */
	@Override
	public BindableOperation insertAndReturnGeneratedKeys(String table, Set<String> columns) {
		return new DefaultBindableInsert(dialect.getBindMarkersFactory().create(), table, columns,
				dialect.returnGeneratedKeys());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#select(java.lang.String, java.util.Set, org.springframework.data.domain.Sort, org.springframework.data.domain.Pageable)
	 */
	@Override
	public QueryOperation select(String table, Set<String> columns, Sort sort, Pageable page) {

		StringBuilder selectBuilder = new StringBuilder();

		selectBuilder.append("SELECT").append(' ') //
				.append(StringUtils.collectionToDelimitedString(columns, ", ")).append(' ') //
				.append("FROM").append(' ').append(table);

		if (sort.isSorted()) {
			selectBuilder.append(' ').append("ORDER BY").append(' ').append(getSortClause(sort));
		}

		if (page.isPaged()) {

			LimitClause limitClause = dialect.limit();

			if (limitClause.getClausePosition() == Position.END) {

				selectBuilder.append(' ').append(limitClause.getClause(page.getPageSize(), page.getOffset()));
			}
		}

		return selectBuilder::toString;
	}

	private StringBuilder getSortClause(Sort sort) {

		StringBuilder sortClause = new StringBuilder();

		for (Order order : sort) {

			if (sortClause.length() != 0) {
				sortClause.append(',').append(' ');
			}

			sortClause.append(order.getProperty()).append(' ').append(order.getDirection().isAscending() ? "ASC" : "DESC");
		}
		return sortClause;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#selectById(java.lang.String, java.util.Set, java.lang.String)
	 */
	@Override
	public BindIdOperation selectById(String table, Set<String> columns, String idColumn) {

		return new DefaultBindIdOperation(dialect.getBindMarkersFactory().create(), marker -> {

			String columnClause = StringUtils.collectionToDelimitedString(columns, ", ");

			return String.format("SELECT %s FROM %s WHERE %s = %s", columnClause, table, idColumn, marker.getPlaceholder());
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#selectById(java.lang.String, java.util.Set, java.lang.String, int)
	 */
	@Override
	public BindIdOperation selectById(String table, Set<String> columns, String idColumn, int limit) {

		LimitClause limitClause = dialect.limit();

		return new DefaultBindIdOperation(dialect.getBindMarkersFactory().create(), marker -> {

			String columnClause = StringUtils.collectionToDelimitedString(columns, ", ");

			if (limitClause.getClausePosition() == Position.END) {

				return String.format("SELECT %s FROM %s WHERE %s = %s ORDER BY %s %s", columnClause, table, idColumn,
						marker.getPlaceholder(), idColumn, limitClause.getClause(limit));
			}

			throw new UnsupportedOperationException(
					String.format("Limit clause position %s not supported!", limitClause.getClausePosition()));
		});
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#selectByIdIn(java.lang.String, java.util.Set, java.lang.String)
	 */
	@Override
	public BindIdOperation selectByIdIn(String table, Set<String> columns, String idColumn) {

		String query = String.format("SELECT %s FROM %s", StringUtils.collectionToDelimitedString(columns, ", "), table);
		return new DefaultBindIdIn(dialect.getBindMarkersFactory().create(), query, idColumn);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#updateById(java.lang.String, java.util.Set, java.lang.String)
	 */
	@Override
	public BindIdOperation updateById(String table, Set<String> columns, String idColumn) {
		return new DefaultBindableUpdate(dialect.getBindMarkersFactory().create(), table, columns, idColumn);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#deleteById(java.lang.String, java.lang.String)
	 */
	@Override
	public BindIdOperation deleteById(String table, String idColumn) {

		return new DefaultBindIdOperation(dialect.getBindMarkersFactory().create(),
				marker -> String.format("DELETE FROM %s WHERE %s = %s", table, idColumn, marker.getPlaceholder()));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#deleteByIdIn(java.lang.String, java.lang.String)
	 */
	@Override
	public BindIdOperation deleteByIdIn(String table, String idColumn) {

		String query = String.format("DELETE FROM %s", table);
		return new DefaultBindIdIn(dialect.getBindMarkersFactory().create(), query, idColumn);
	}

	/**
	 * Default {@link BindableOperation} implementation for a {@code INSERT} operation.
	 */
	static class DefaultBindableInsert implements BindableOperation {

		private final Map<String, BindMarker> markers = new LinkedHashMap<>();
		private final String query;

		DefaultBindableInsert(BindMarkers bindMarkers, String table, Collection<String> columns,
				String returningStatement) {

			StringBuilder builder = new StringBuilder();
			List<String> placeholders = new ArrayList<>(columns.size());

			for (String column : columns) {
				BindMarker marker = markers.computeIfAbsent(column, bindMarkers::next);
				placeholders.add(marker.getPlaceholder());
			}

			String columnsString = StringUtils.collectionToDelimitedString(columns, ", ");
			String placeholdersString = StringUtils.collectionToDelimitedString(placeholders, ", ");

			builder.append("INSERT INTO ").append(table).append(" (").append(columnsString).append(")").append(" VALUES(")
					.append(placeholdersString).append(")");

			if (StringUtils.hasText(returningStatement)) {
				builder.append(' ').append(returningStatement);
			}

			this.query = builder.toString();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.BindableOperation#bind(io.r2dbc.spi.Statement, java.lang.String, java.lang.Object)
		 */
		@Override
		public void bind(Statement<?> statement, String identifier, Object value) {
			markers.get(identifier).bind(statement, value);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.BindableOperation#bindNull(io.r2dbc.spi.Statement, java.lang.String, java.lang.Class)
		 */
		@Override
		public void bindNull(Statement<?> statement, String identifier, Class<?> valueType) {
			markers.get(identifier).bindNull(statement, valueType);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.QueryOperation#toQuery()
		 */
		@Override
		public String toQuery() {
			return this.query;
		}
	}

	/**
	 * Default {@link BindIdOperation} implementation for a {@code UPDATE} operation using a single key.
	 */
	static class DefaultBindableUpdate implements BindIdOperation {

		private final Map<String, BindMarker> markers = new LinkedHashMap<>();
		private final BindMarker idMarker;
		private final String query;

		DefaultBindableUpdate(BindMarkers bindMarkers, String tableName, Set<String> columns, String idColumnName) {

			this.idMarker = bindMarkers.next();

			StringBuilder setClause = new StringBuilder();

			for (String column : columns) {

				BindMarker marker = markers.computeIfAbsent(column, bindMarkers::next);

				if (setClause.length() != 0) {
					setClause.append(", ");
				}

				setClause.append(column).append(" = ").append(marker.getPlaceholder());
			}

			this.query = String.format("UPDATE %s SET %s WHERE %s = %s", tableName, setClause, idColumnName,
					idMarker.getPlaceholder());
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.BindableOperation#bind(io.r2dbc.spi.Statement, java.lang.String, java.lang.Object)
		 */
		@Override
		public void bind(Statement<?> statement, String identifier, Object value) {
			markers.get(identifier).bind(statement, value);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.BindableOperation#bindNull(io.r2dbc.spi.Statement, java.lang.String, java.lang.Class)
		 */
		@Override
		public void bindNull(Statement<?> statement, String identifier, Class<?> valueType) {
			markers.get(identifier).bindNull(statement, valueType);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.BindIdOperation#bindId(io.r2dbc.spi.Statement, java.lang.Object)
		 */
		@Override
		public void bindId(Statement<?> statement, Object value) {
			idMarker.bind(statement, value);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.BindIdOperation#bindIds(io.r2dbc.spi.Statement, java.lang.Iterable)
		 */
		@Override
		public void bindIds(Statement<?> statement, Iterable<? extends Object> values) {
			throw new UnsupportedOperationException();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.QueryOperation#toQuery()
		 */
		@Override
		public String toQuery() {
			return this.query;
		}
	}

	/**
	 * Default {@link BindIdOperation} implementation for a {@code SELECT} or {@code DELETE} operation using a single key
	 * in the {@code WHERE} predicate.
	 */
	static class DefaultBindIdOperation implements BindIdOperation {

		private final BindMarker idMarker;
		private final String query;

		DefaultBindIdOperation(BindMarkers bindMarkers, Function<BindMarker, String> queryFunction) {

			this.idMarker = bindMarkers.next();
			this.query = queryFunction.apply(this.idMarker);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.BindableOperation#bind(io.r2dbc.spi.Statement, java.lang.String, java.lang.Object)
		 */
		@Override
		public void bind(Statement<?> statement, String identifier, Object value) {
			throw new UnsupportedOperationException();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.BindableOperation#bindNull(io.r2dbc.spi.Statement, java.lang.String, java.lang.Class)
		 */
		@Override
		public void bindNull(Statement<?> statement, String identifier, Class<?> valueType) {
			throw new UnsupportedOperationException();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.BindIdOperation#bindId(io.r2dbc.spi.Statement, java.lang.Object)
		 */
		@Override
		public void bindId(Statement<?> statement, Object value) {
			idMarker.bind(statement, value);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.BindIdOperation#bindIds(io.r2dbc.spi.Statement, java.lang.Iterable)
		 */
		@Override
		public void bindIds(Statement<?> statement, Iterable<? extends Object> values) {
			throw new UnsupportedOperationException();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.QueryOperation#toQuery()
		 */
		@Override
		public String toQuery() {
			return this.query;
		}
	}

	/**
	 * Default {@link BindIdOperation} implementation for a {@code SELECT … WHERE id IN (…)} or
	 * {@code DELETE … WHERE id IN (…)}.
	 */
	static class DefaultBindIdIn implements BindIdOperation {

		private final List<String> markers = new ArrayList<>();
		private final BindMarkers bindMarkers;
		private final String baseQuery;
		private final String idColumnName;

		DefaultBindIdIn(BindMarkers bindMarkers, String baseQuery, String idColumnName) {

			this.bindMarkers = bindMarkers;
			this.baseQuery = baseQuery;
			this.idColumnName = idColumnName;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.BindableOperation#bind(io.r2dbc.spi.Statement, java.lang.String, java.lang.Object)
		 */
		@Override
		public void bind(Statement<?> statement, String identifier, Object value) {
			throw new UnsupportedOperationException();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.BindableOperation#bindNull(io.r2dbc.spi.Statement, java.lang.String, java.lang.Class)
		 */
		@Override
		public void bindNull(Statement<?> statement, String identifier, Class<?> valueType) {
			throw new UnsupportedOperationException();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.BindIdOperation#bindId(io.r2dbc.spi.Statement, java.lang.Object)
		 */
		@Override
		public void bindId(Statement<?> statement, Object value) {

			BindMarker bindMarker = bindMarkers.next();
			markers.add(bindMarker.getPlaceholder());
			bindMarker.bind(statement, value);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.BindIdOperation#bindIds(io.r2dbc.spi.Statement, java.lang.Iterable)
		 */
		@Override
		public void bindIds(Statement<?> statement, Iterable<? extends Object> values) {

			for (Object value : values) {
				bindId(statement, value);
			}
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.QueryOperation#toQuery()
		 */
		@Override
		public String toQuery() {

			if (this.markers.isEmpty()) {
				throw new UnsupportedOperationException();
			}

			String in = StringUtils.collectionToDelimitedString(this.markers, ", ");

			return String.format("%s WHERE %s IN (%s)", this.baseQuery, this.idColumnName, in);
		}
	}
}
