/*
 * Copyright 2023-2024 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.domain.RowDocument;
import org.springframework.lang.Nullable;

/**
 * Support class for {@code ResultSet}-driven extractor implementations extracting {@link RowDocument documents} from
 * flexible input streams.
 *
 * @author Mark Paluch
 * @since 3.2
 */
abstract class RowDocumentExtractorSupport {

	/**
	 * Result adapter to obtain values and column metadata.
	 *
	 * @param <RS>
	 */
	interface TabularResultAdapter<RS> {

		/**
		 * Read a value from the row input at {@code index}.
		 *
		 * @param row the row to read from.
		 * @param index the column index.
		 * @return the column value. Can be {@code null}.
		 */
		@Nullable
		Object getObject(RS row, int index);

		/**
		 * Retrieve a column name to column index map for access by column name.
		 *
		 * @param result the result set to read from.
		 * @return column name to column index map.
		 */
		Map<String, Integer> getColumnMap(RS result);
	}

	/**
	 * Reading context encapsulating value reading and column handling.
	 *
	 * @param <RS>
	 */
	protected static class AggregateContext<RS> {

		private final TabularResultAdapter<RS> adapter;
		private final RelationalMappingContext context;
		private final PathToColumnMapping propertyToColumn;
		private final Map<String, Integer> columnMap;

		protected AggregateContext(TabularResultAdapter<RS> adapter, RelationalMappingContext context,
				PathToColumnMapping propertyToColumn, Map<String, Integer> columnMap) {

			this.adapter = adapter;
			this.context = context;
			this.propertyToColumn = propertyToColumn;
			this.columnMap = columnMap;
		}

		public RelationalPersistentEntity<?> getRequiredPersistentEntity(RelationalPersistentProperty property) {
			return context.getRequiredPersistentEntity(property);
		}

		public String getColumnName(AggregatePath path) {
			return propertyToColumn.column(path);
		}

		public String getKeyColumnName(AggregatePath path) {
			return propertyToColumn.keyColumn(path);
		}

		public boolean containsColumn(String columnName) {
			return columnMap.containsKey(columnName);
		}

		@Nullable
		public Object getObject(RS row, String columnName) {
			return adapter.getObject(row, columnMap.get(columnName));
		}

		/**
		 * Collect the value for {@link AggregatePath} from the {@code row} and add it under {@link SqlIdentifier} to the
		 * {@link RowDocument}.
		 */
		void collectValue(RS row, AggregatePath source, RowDocument document, SqlIdentifier targetName) {

			String columnLabel = propertyToColumn.column(source);
			Integer index = columnMap.get(columnLabel);

			if (index == null) {
				return;
			}

			Object resultSetValue = adapter.getObject(row, index);
			if (resultSetValue == null) {
				return;
			}

			document.put(targetName.getReference(), resultSetValue);
		}

	}

	/**
	 * Sink abstraction for tabular result sets that represent an aggregate including all of its nested entities. Reading
	 * is driven by the results and readers receive a feed of rows to extract the data they are looking for.
	 * <p>
	 * Sinks aim to produce a {@link #getResult() result}. Based on the inputs, results may be {@link #hasResult()
	 * present} or absent.
	 */
	protected abstract static class TabularSink<RS> {

		/**
		 * Accept a row of data and process their results to form potentially a {@link #getResult() result}.
		 *
		 * @param row the row to read from.
		 */
		abstract void accept(RS row);

		/**
		 * @return {@code true} if the sink has produced a result.
		 */
		abstract boolean hasResult();

		/**
		 * Retrieve the sink result if present.
		 *
		 * @return the sink result.
		 */
		@Nullable
		abstract Object getResult();

		/**
		 * Reset the sink to prepare for the next result.
		 */
		abstract void reset();
	}

	/**
	 * Entity-driven sink to form a {@link RowDocument document} representing the underlying entities properties.
	 *
	 * @param <RS>
	 */
	protected static class RowDocumentSink<RS> extends TabularSink<RS> {

		private final AggregateContext<RS> aggregateContext;
		private final RelationalPersistentEntity<?> entity;
		private final AggregatePath basePath;
		private RowDocument result;

		private String keyColumnName;

		private @Nullable Object key;
		private final Map<RelationalPersistentProperty, TabularSink<RS>> readerState = new LinkedHashMap<>();

		public RowDocumentSink(AggregateContext<RS> aggregateContext, RelationalPersistentEntity<?> entity,
				AggregatePath basePath) {

			this.aggregateContext = aggregateContext;
			this.entity = entity;
			this.basePath = basePath;

			String keyColumnName;
			if (entity.hasIdProperty()) {
				keyColumnName = aggregateContext.getColumnName(basePath.append(entity.getRequiredIdProperty()));
			} else {
				keyColumnName = aggregateContext.getColumnName(basePath);
			}

			this.keyColumnName = keyColumnName;
		}

		@Override
		void accept(RS row) {

			boolean first = result == null;

			if (first) {
				RowDocument document = new RowDocument();
				readFirstRow(row, document);
				this.result = document;
			}

			for (TabularSink<RS> reader : readerState.values()) {
				reader.accept(row);
			}
		}

		/**
		 * First row contains the root aggregate and all headers for nested collections/maps/entities.
		 */
		private void readFirstRow(RS row, RowDocument document) {

			// key marker
			if (aggregateContext.containsColumn(keyColumnName)) {
				key = aggregateContext.getObject(row, keyColumnName);
			}

			readEntity(row, document, basePath, entity);
		}

		private void readEntity(RS row, RowDocument document, AggregatePath basePath,
				RelationalPersistentEntity<?> entity) {

			for (RelationalPersistentProperty property : entity) {

				AggregatePath path = basePath.append(property);

				if (property.isEntity() && !property.isEmbedded() && (property.isCollectionLike() || property.isQualified())) {

					readerState.put(property, new ContainerSink<>(aggregateContext, property, path));
					continue;
				}

				if (property.isEmbedded()) {

					RelationalPersistentEntity<?> embeddedEntity = aggregateContext.getRequiredPersistentEntity(property);
					readEntity(row, document, path, embeddedEntity);
					continue;
				}

				if (property.isEntity()) {
					readerState.put(property,
							new RowDocumentSink<>(aggregateContext, aggregateContext.getRequiredPersistentEntity(property), path));
					continue;
				}

				aggregateContext.collectValue(row, path, document, property.getColumnName());
			}
		}

		/**
		 * Read properties of embedded from the result set and store them under their column names
		 */
		private void collectEmbeddedValues(RS row, RowDocument document, RelationalPersistentProperty property,
				AggregatePath path) {

			RelationalPersistentEntity<?> embeddedHolder = aggregateContext.getRequiredPersistentEntity(property);
			for (RelationalPersistentProperty embeddedProperty : embeddedHolder) {

				if (embeddedProperty.isQualified() || embeddedProperty.isCollectionLike() || embeddedProperty.isEntity()) {
					// hell, no!
					throw new UnsupportedOperationException("Reading maps and collections into embeddable isn't supported yet");
				}

				AggregatePath nested = path.append(embeddedProperty);
				aggregateContext.collectValue(row, nested, document, nested.getColumnInfo().name());
			}
		}

		@Override
		boolean hasResult() {

			if (result == null) {
				return false;
			}

			for (TabularSink<?> value : readerState.values()) {
				if (value.hasResult()) {
					return true;
				}
			}

			return !(result.isEmpty() && key == null);
		}

		@Override
		RowDocument getResult() {

			readerState.forEach((property, reader) -> {

				if (reader.hasResult()) {
					result.put(property.getColumnName().getReference(), reader.getResult());
				}
			});

			return result;
		}

		@Override
		void reset() {

			result = null;
			readerState.clear();
		}
	}

	/**
	 * Sink using a single column to retrieve values from.
	 *
	 * @param <RS>
	 */
	private static class SingleColumnSink<RS> extends TabularSink<RS> {

		private final AggregateContext<RS> aggregateContext;
		private final String columnName;

		private @Nullable Object value;

		public SingleColumnSink(AggregateContext<RS> aggregateContext, AggregatePath path) {

			this.aggregateContext = aggregateContext;
			this.columnName = path.getColumnInfo().name().getReference();
		}

		@Override
		void accept(RS row) {

			if (aggregateContext.containsColumn(columnName)) {
				value = aggregateContext.getObject(row, columnName);
			} else {
				value = null;
			}
		}

		@Override
		boolean hasResult() {
			return value != null;
		}

		@Override
		Object getResult() {
			return getValue();
		}

		@Nullable
		public Object getValue() {
			return value;
		}

		@Override
		void reset() {
			value = null;
		}
	}

	/**
	 * A sink that aggregates multiple values in a {@link CollectionContainer container} such as List or Map. Inner values
	 * are determined by the value type while the key type is expected to be a simple type such a string or a number.
	 *
	 * @param <RS>
	 */
	private static class ContainerSink<RS> extends TabularSink<RS> {

		private final String keyColumn;
		private final AggregateContext<RS> aggregateContext;

		private Object key;
		private boolean hasResult = false;

		private final TabularSink<RS> componentReader;
		private final CollectionContainer container;

		public ContainerSink(AggregateContext<RS> aggregateContext, RelationalPersistentProperty property,
				AggregatePath path) {

			this.aggregateContext = aggregateContext;
			this.keyColumn = aggregateContext.getKeyColumnName(path);
			this.componentReader = property.isEntity()
					? new RowDocumentSink<>(aggregateContext, aggregateContext.getRequiredPersistentEntity(property), path)
					: new SingleColumnSink<>(aggregateContext, path);

			this.container = property.isMap() ? new MapContainer() : new ListContainer();
		}

		@Override
		void accept(RS row) {

			if (!aggregateContext.containsColumn(keyColumn)) {
				return;
			}

			Object key = aggregateContext.getObject(row, keyColumn);
			if (key == null && !hasResult) {
				return;
			}

			boolean keyChange = key != null && !key.equals(this.key);

			if (!hasResult) {
				hasResult = true;
			}

			if (keyChange) {
				if (componentReader.hasResult()) {
					container.add(this.key, componentReader.getResult());
					componentReader.reset();
				}
			}

			if (key != null) {
				this.key = key;
			}

			this.componentReader.accept(row);
		}

		@Override
		public boolean hasResult() {
			return hasResult;
		}

		@Override
		public Object getResult() {

			if (componentReader.hasResult()) {

				container.add(this.key, componentReader.getResult());
				componentReader.reset();
			}

			return container.get();
		}

		@Override
		void reset() {
			hasResult = false;
		}
	}

	/**
	 * Base class defining method signatures to add values to a container that can hold multiple values, such as a List or
	 * Map.
	 */
	private abstract static class CollectionContainer {

		/**
		 * Append the value.
		 *
		 * @param key the entry key/index.
		 * @param value the entry value, can be {@literal null}.
		 */
		abstract void add(Object key, @Nullable Object value);

		/**
		 * Return the container holding the values that were previously added.
		 *
		 * @return the container holding the values that were previously added.
		 */
		abstract Object get();
	}

	// TODO: Are we 0 or 1 based?
	private static class ListContainer extends CollectionContainer {

		private final Map<Number, Object> list = new TreeMap<>(Comparator.comparing(Number::longValue));

		@Override
		public void add(Object key, @Nullable Object value) {
			list.put(((Number) key).intValue() - 1, value);
		}

		@Override
		public List<Object> get() {

			List<Object> result = new ArrayList<>(list.size());

			// TODO: How do we go about padding? Should we insert null values?
			list.forEach((index, o) -> {

				while (result.size() < index.intValue()) {
					result.add(null);
				}

				result.add(o);
			});

			return result;
		}
	}

	private static class MapContainer extends CollectionContainer {

		private final Map<Object, Object> map = new LinkedHashMap<>();

		@Override
		public void add(Object key, @Nullable Object value) {
			map.put(key, value);
		}

		@Override
		public Map<Object, Object> get() {
			return new LinkedHashMap<>(map);
		}
	}

}
