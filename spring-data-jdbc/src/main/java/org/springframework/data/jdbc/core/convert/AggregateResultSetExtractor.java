/*
 * Copyright 2023 the original author or authors.
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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Supplier;

import org.springframework.dao.DataAccessException;
import org.springframework.data.mapping.Parameter;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.EntityInstantiator;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.domain.RowDocument;
import org.springframework.data.util.TypeInformation;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.LinkedCaseInsensitiveMap;

/**
 * Extracts complete aggregates from a {@link ResultSet}. The {@literal ResultSet} must have a very special structure
 * which looks somewhat how one would represent an aggregate in a single excel table. The first row contains data of the
 * aggregate root, any single valued reference and the first element of any collection. Following rows do NOT repeat the
 * aggregate root data but contain data of second elements of any collections. For details see accompanying unit tests.
 *
 * @param <T> the type of aggregates to extract
 * @author Jens Schauder
 * @since 3.2
 */
class AggregateResultSetExtractor<T> implements ResultSetExtractor<Iterable<T>> {

	private final RelationalMappingContext context;
	private final RelationalPersistentEntity<T> rootEntity;
	private final JdbcConverter converter;
	private final PathToColumnMapping propertyToColumn;

	/**
	 * @param rootEntity the aggregate root. Must not be {@literal null}.
	 * @param converter Used for converting objects from the database to whatever is required by the aggregate. Must not
	 *          be {@literal null}.
	 * @param pathToColumn a mapping from {@link org.springframework.data.relational.core.mapping.AggregatePath} to the
	 *          column of the {@link ResultSet} that holds the data for that
	 *          {@link org.springframework.data.relational.core.mapping.AggregatePath}.
	 */
	AggregateResultSetExtractor(RelationalPersistentEntity<T> rootEntity, JdbcConverter converter,
			PathToColumnMapping pathToColumn) {

		Assert.notNull(rootEntity, "rootEntity must not be null");
		Assert.notNull(converter, "converter must not be null");
		Assert.notNull(pathToColumn, "propertyToColumn must not be null");

		this.rootEntity = rootEntity;
		this.converter = converter;
		this.context = converter.getMappingContext();
		this.propertyToColumn = pathToColumn;
	}

	@Override
	public Iterable<T> extractData(ResultSet resultSet) throws DataAccessException {

		CachingResultSet crs = new CachingResultSet(resultSet);

		CollectionReader reader = new CollectionReader(crs);

		while (crs.next()) {
			reader.read();
		}

		return (Iterable<T>) reader.getResultAndReset();
	}

	/**
	 * create an instance and populate all its properties
	 */
	@Nullable
	private Object hydrateInstance(EntityInstantiator instantiator, ResultSetParameterValueProvider valueProvider,
			RelationalPersistentEntity<?> entity) {

		if (!valueProvider.basePath.isRoot() && // this is a nested ValueProvider
				valueProvider.basePath.getRequiredLeafProperty().isEmbedded() && // it's an embedded
				!valueProvider.basePath.getRequiredLeafProperty().shouldCreateEmptyEmbedded() && // it's embedded
				!valueProvider.hasValue()) { // all values have been null
			return null;
		}

		Object instance = instantiator.createInstance(entity, valueProvider);

		PersistentPropertyAccessor<?> accessor = new ConvertingPropertyAccessor<>(entity.getPropertyAccessor(instance),
				converter.getConversionService());

		if (entity.requiresPropertyPopulation()) {

			entity.doWithProperties((PropertyHandler<RelationalPersistentProperty>) p -> {

				if (!entity.isCreatorArgument(p)) {
					accessor.setProperty(p, valueProvider.getValue(p));
				}
			});
		}
		return instance;
	}

	/**
	 * Reads the next {@link RowDocument} from the {@link ResultSet}. The result set can be pristine (i.e.
	 * {@link ResultSet#isBeforeFirst()}) or pointing already at a row.
	 *
	 * @param resultSet the result set to consume.
	 * @return a {@link RowDocument}.
	 * @throws SQLException
	 * @throws IllegalStateException if the {@link ResultSet#isAfterLast() fully consumed}.
	 */
	public RowDocument extractNextDocument(ResultSet resultSet) throws SQLException {

		AggregatePath root = context.getAggregatePath(rootEntity);

		if (resultSet.isBeforeFirst()) {
			resultSet.next();
		}

		if (resultSet.isAfterLast()) {
			throw new IllegalStateException("ResultSet is fully consumed");
		}

		String idColumn = propertyToColumn.column(root.append(rootEntity.getRequiredIdProperty()));
		Map<String, Integer> columns = getColumnsInResultSet(resultSet);
		Integer identifierIndex = columns.get(idColumn);

		if (identifierIndex == null) {
			throw new IllegalStateException(
					String.format("Cannot find identifier column %s in columns %s", idColumn, columns));
		}

		RowDocumentReader reader = new RowDocumentReader(columns, rootEntity, root);
		Object key = resultSet.getObject(identifierIndex);

		do {
			Object nextKey = resultSet.getObject(identifierIndex);

			if (nextKey != null && !nextKey.equals(key)) {
				break;
			}

			reader.read(resultSet);
		} while (resultSet.next());

		return reader.getResult();
	}

	private static Map<String, Integer> getColumnsInResultSet(ResultSet resultSet) throws SQLException {

		ResultSetMetaData metaData = resultSet.getMetaData();
		Map<String, Integer> columns = new LinkedCaseInsensitiveMap<>(metaData.getColumnCount());

		for (int i = 0; i < metaData.getColumnCount(); i++) {
			columns.put(metaData.getColumnLabel(i + 1), i + 1);
		}
		return columns;
	}

	private void collectValue(Map<String, Integer> columnMap, ResultSet resultSet, AggregatePath source,
			RowDocument document, SqlIdentifier targetName) throws SQLException {

		String columnLabel = propertyToColumn.column(source);
		Integer index = columnMap.get(columnLabel);

		if (index == null) {
			return;
		}

		Object resultSetValue = JdbcUtils.getResultSetValue(resultSet, index);
		if (resultSetValue == null) {
			return;
		}

		document.put(targetName.getReference(), resultSetValue);
	}

	abstract class TabularReader {

		abstract void read(ResultSet row) throws SQLException;

		abstract boolean hasResult();

		abstract Object getResult();

		abstract void reset();
	}

	interface CollectionContainer {

		void add(Object key, Object value);

		Object get();
	}

	static class ListContainer implements CollectionContainer {

		private final Map<Number, Object> list = new TreeMap<>(Comparator.comparing(Number::longValue));

		@Override
		public void add(Object key, Object value) {
			list.put(((Number) key).intValue() - 1, value);
		}

		@Override
		public List<Object> get() {

			List<Object> result = new ArrayList<>(list.size());

			list.forEach((index, o) -> {

				while (result.size() < index.intValue()) {
					result.add(null);
				}

				result.add(o);
			});

			return result;
		}
	}

	class RowDocumentReader extends TabularReader {

		private final Map<String, Integer> columnMap;

		private final RelationalPersistentEntity<?> entity;
		private final AggregatePath basePath;
		private RowDocument result;

		private final Map<RelationalPersistentProperty, TabularReader> readerState = new LinkedHashMap<>();

		public RowDocumentReader(Map<String, Integer> columnMap, RelationalPersistentEntity<?> entity,
				AggregatePath basePath) {
			this.columnMap = columnMap;
			this.entity = entity;
			this.basePath = basePath;
		}

		@Override
		void read(ResultSet row) throws SQLException {

			boolean first = result == null;

			if (first) {
				result = new RowDocument();
			}

			for (RelationalPersistentProperty property : entity) {

				if (first) {
					AggregatePath path = basePath.append(property);

					if (property.isQualified()) {
						readerState.put(property, new TabularContainerReader(columnMap, property, path));
						continue;
					} else if (property.isEntity() && !property.isEmbedded()) {
						readerState.put(property,
								new RowDocumentReader(columnMap, context.getRequiredPersistentEntity(property), path));
						continue;
					}
				}

				// first row contains the root aggregate
				if (first) {

					AggregatePath path = this.basePath.append(property);
					if (property.isEmbedded()) {
						// read properties of embedded from the result set and store them under their column names

						RelationalPersistentEntity<?> embeddedHolder = context.getRequiredPersistentEntity(property);
						for (RelationalPersistentProperty embeddedProperty : embeddedHolder) {

							if (embeddedProperty.isQualified() || embeddedProperty.isCollectionLike()
									|| embeddedProperty.isEntity()) {
								// hell, no!
								continue;
							}

							AggregatePath nested = path.append(embeddedProperty);
							collectValue(columnMap, row, nested, result, nested.getColumnInfo().name());
						}
					}

					collectValue(columnMap, row, path, result, property.getColumnName());
				}
			}

			for (TabularReader reader : readerState.values()) {
				reader.read(row);
			}
		}

		@Override
		boolean hasResult() {

			if (result == null) {
				return false;
			}

			for (TabularReader value : readerState.values()) {
				if (value.hasResult()) {
					return true;
				}
			}

			return !result.isEmpty();
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

	static class MapContainer implements CollectionContainer {

		private final Map<Object, Object> map = new LinkedHashMap<>();

		@Override
		public void add(Object key, Object value) {
			map.put(key, value);
		}

		@Override
		public Map<Object, Object> get() {
			return new LinkedHashMap<>(map);
		}
	}

	private class TabularContainerReader extends TabularReader {

		private final Map<String, Integer> columnMap;
		private final RelationalPersistentProperty property;
		private final AggregatePath path;

		private final String keyColumn;

		private Object key;
		private boolean hasResult = false;

		private final CollectionContainer container;

		private final TabularReader componentReader;

		public TabularContainerReader(Map<String, Integer> columnMap, RelationalPersistentProperty property,
				AggregatePath path) {

			this.columnMap = columnMap;
			this.property = property;
			this.path = path;
			this.keyColumn = propertyToColumn.keyColumn(path);
			this.componentReader = property.isEntity()
					? new RowDocumentReader(columnMap, context.getRequiredPersistentEntity(property), path)
					: new SingleColumnReader(columnMap, propertyToColumn.column(path));

			this.container = property.isMap() ? new MapContainer() : new ListContainer();
		}

		@Override
		void read(ResultSet row) throws SQLException {

			if (!columnMap.containsKey(keyColumn)) {
				return;
			}

			boolean initial = false;
			Object key = JdbcUtils.getResultSetValue(row, columnMap.get(keyColumn));
			if (key == null && !hasResult) {
				return;
			}

			boolean keyChange = false;

			if (key != null && !key.equals(this.key)) {
				keyChange = true;
			}

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

			this.componentReader.read(row);
		}

		public boolean hasResult() {
			return hasResult;
		}

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

	class SingleColumnReader extends TabularReader {

		private final Map<String, Integer> columnMap;
		private final String columnName;

		private @Nullable Object value;

		public SingleColumnReader(Map<String, Integer> columnMap, String columnName) {
			this.columnMap = columnMap;
			this.columnName = columnName;
		}

		@Override
		void read(ResultSet row) throws SQLException {

			if (!columnMap.containsKey(columnName)) {
				return;
			}

			value = JdbcUtils.getResultSetValue(row, columnMap.get(columnName));
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
	 * A {@link Reader} is responsible for reading a single entity or collection of entities from a set of columns
	 *
	 * @since 3.2
	 * @author Jens Schauder
	 */
	private interface Reader {

		/**
		 * read the data needed for creating the result of this {@literal Reader}
		 */
		void read();

		/**
		 * Checks if this {@literal Reader} has all the data needed for a complete result, or if it needs to read further
		 * rows.
		 *
		 * @return the result of the check.
		 */
		boolean hasResult();

		/**
		 * Constructs the result, returns it and resets the state of the reader to read the next instance.
		 *
		 * @return an instance of whatever this {@literal Reader} is supposed to read.
		 */
		@Nullable
		Object getResultAndReset();
	}

	/**
	 * Adapts a {@link Map} to the interface of a {@literal Collection<Map.Entry<Object, Object>>}.
	 *
	 * @since 3.2
	 * @author Jens Schauder
	 */
	private static class MapAdapter extends AbstractCollection<Map.Entry<Object, Object>> {

		private final Map<Object, Object> map = new HashMap<>();

		@Override
		public Iterator<Map.Entry<Object, Object>> iterator() {
			return map.entrySet().iterator();
		}

		@Override
		public int size() {
			return map.size();
		}

		@Override
		public boolean add(Map.Entry<Object, Object> entry) {

			map.put(entry.getKey(), entry.getValue());
			return true;
		}
	}

	/**
	 * Adapts a {@link List} to the interface of a {@literal Collection<Map.Entry<Object, Object>>}.
	 *
	 * @since 3.2
	 * @author Jens Schauder
	 */
	private static class ListAdapter extends AbstractCollection<Map.Entry<Object, Object>> {

		private final List<Object> list = new ArrayList<>();

		@Override
		public Iterator<Map.Entry<Object, Object>> iterator() {
			throw new UnsupportedOperationException("Do we need this?");
		}

		@Override
		public int size() {
			return list.size();
		}

		@Override
		public boolean add(Map.Entry<Object, Object> entry) {

			Integer index = (Integer) entry.getKey();
			while (index >= list.size()) {
				list.add(null);
			}
			list.set(index, entry.getValue());
			return true;
		}
	}

	/**
	 * A {@link Reader} for reading entities.
	 *
	 * @since 3.2
	 * @author Jens Schauder
	 */
	private class EntityReader implements Reader {

		/**
		 * Debugging the recursive structure of {@link Reader} instances can become a little mind bending. Giving each
		 * {@literal Reader} a descriptive name helps with that.
		 */
		private final String name;

		private final AggregatePath basePath;
		private final CachingResultSet crs;

		private final EntityInstantiator instantiator;
		@Nullable private final String idColumn;

		private ResultSetParameterValueProvider valueProvider;
		private boolean result;

		Object oldId = null;

		private EntityReader(AggregatePath basePath, CachingResultSet crs) {
			this(basePath, crs, null);
		}

		private EntityReader(AggregatePath basePath, CachingResultSet crs, @Nullable String keyColumn) {

			this.basePath = basePath;
			this.crs = crs;

			RelationalPersistentEntity<?> entity = basePath.isRoot() ? rootEntity : basePath.getRequiredLeafEntity();
			instantiator = converter.getEntityInstantiators().getInstantiatorFor(entity);

			idColumn = entity.hasIdProperty() ? propertyToColumn.column(basePath.append(entity.getRequiredIdProperty()))
					: keyColumn;

			reset();

			name = "EntityReader for " + (basePath.isRoot() ? "<root>" : basePath.toDotPath());
		}

		@Override
		public void read() {

			if (idColumn != null && oldId == null) {
				oldId = crs.getObject(idColumn);
			}

			valueProvider.readValues();
			if (idColumn == null) {
				result = true;
			} else {
				Object peekedId = crs.peek(idColumn);
				if (peekedId == null || !peekedId.equals(oldId)) {

					result = true;
					oldId = peekedId;
				}
			}
		}

		@Override
		public boolean hasResult() {
			return result;
		}

		@Override
		@Nullable
		public Object getResultAndReset() {

			try {
				return hydrateInstance(instantiator, valueProvider, valueProvider.baseEntity);
			} finally {

				reset();
			}
		}

		private void reset() {

			valueProvider = new ResultSetParameterValueProvider(crs, basePath);
			result = false;
		}

		@Override
		public String toString() {
			return name;
		}
	}

	/**
	 * A {@link Reader} for reading collections of entities.
	 *
	 * @since 3.2
	 * @author Jens Schauder
	 */
	class CollectionReader implements Reader {

		// debugging only
		private final String name;

		private final Supplier<Collection> collectionInitializer;
		private final Reader entityReader;

		private Collection result;

		private static Supplier<Collection> collectionInitializerFor(AggregatePath path) {

			RelationalPersistentProperty property = path.getRequiredLeafProperty();
			if (List.class.isAssignableFrom(property.getType())) {
				return ListAdapter::new;
			} else if (property.isMap()) {
				return MapAdapter::new;
			} else {
				return HashSet::new;
			}
		}

		private CollectionReader(AggregatePath basePath, CachingResultSet crs) {

			this.collectionInitializer = collectionInitializerFor(basePath);

			String keyColumn = null;
			final RelationalPersistentProperty property = basePath.getRequiredLeafProperty();
			if (property.isMap() || List.class.isAssignableFrom(basePath.getRequiredLeafProperty().getType())) {
				keyColumn = propertyToColumn.keyColumn(basePath);
			}

			if (property.isQualified()) {
				this.entityReader = new EntryReader(basePath, crs, keyColumn, property.getQualifierColumnType());
			} else {
				this.entityReader = new EntityReader(basePath, crs, keyColumn);
			}
			reset();
			name = "Reader for " + basePath.toDotPath();
		}

		private CollectionReader(CachingResultSet crs) {

			this.collectionInitializer = ArrayList::new;
			this.entityReader = new EntityReader(context.getAggregatePath(rootEntity), crs);
			reset();

			name = "Collectionreader for <root>";

		}

		@Override
		public void read() {

			entityReader.read();
			if (entityReader.hasResult()) {
				result.add(entityReader.getResultAndReset());
			}
		}

		@Override
		public boolean hasResult() {
			return false;
		}

		@Override
		public Object getResultAndReset() {

			try {
				if (result instanceof MapAdapter) {
					return ((MapAdapter) result).map;
				}
				if (result instanceof ListAdapter) {
					return ((ListAdapter) result).list;
				}
				return result;
			} finally {
				reset();
			}
		}

		private void reset() {
			result = collectionInitializer.get();
		}

		@Override
		public String toString() {
			return name;
		}
	}

	/**
	 * A {@link Reader} for reading collection entries. Most of the work is done by an {@link EntityReader}, but a
	 * additional key column might get read. The result is
	 *
	 * @since 3.2
	 * @author Jens Schauder
	 */
	private class EntryReader implements Reader {

		final EntityReader delegate;
		final String keyColumn;
		private final TypeInformation<?> keyColumnType;

		Object key;

		EntryReader(AggregatePath basePath, CachingResultSet crs, String keyColumn, Class<?> keyColumnType) {

			this.keyColumnType = TypeInformation.of(keyColumnType);
			this.delegate = new EntityReader(basePath, crs, keyColumn);
			this.keyColumn = keyColumn;
		}

		@Override
		public void read() {

			if (key == null) {
				Object unconvertedKeyObject = delegate.crs.getObject(keyColumn);
				key = converter.readValue(unconvertedKeyObject, keyColumnType);
			}
			delegate.read();
		}

		@Override
		public boolean hasResult() {
			return delegate.hasResult();
		}

		@Override
		public Object getResultAndReset() {

			try {
				return new AbstractMap.SimpleEntry<>(key, delegate.getResultAndReset());
			} finally {
				key = null;
			}
		}
	}

	/**
	 * A {@link ParameterValueProvider} that provided the values for an entity from a continues set of rows in a
	 * {@link ResultSet}. These might be referenced entities or collections of such entities. {@link ResultSet}.
	 *
	 * @since 3.2
	 * @author Jens Schauder
	 */
	private class ResultSetParameterValueProvider implements ParameterValueProvider<RelationalPersistentProperty> {

		private final CachingResultSet rs;
		/**
		 * The path which is used to determine columnNames
		 */
		private final AggregatePath basePath;
		private final RelationalPersistentEntity<?> baseEntity;

		/**
		 * Holds all the values for the entity, either directly or in the form of an appropriate {@link Reader}.
		 */
		private final Map<RelationalPersistentProperty, Object> aggregatedValues = new HashMap<>();

		ResultSetParameterValueProvider(CachingResultSet rs, AggregatePath basePath) {

			this.rs = rs;
			this.basePath = basePath;
			this.baseEntity = basePath.isRoot() ? rootEntity
					: context.getRequiredPersistentEntity(basePath.getRequiredLeafProperty().getActualType());
		}

		@SuppressWarnings("unchecked")
		@Override
		@Nullable
		public <S> S getParameterValue(Parameter<S, RelationalPersistentProperty> parameter) {

			return (S) getValue(baseEntity.getRequiredPersistentProperty(parameter.getName()));
		}

		@Nullable
		private Object getValue(RelationalPersistentProperty property) {

			Object value = aggregatedValues.get(property);

			if (value instanceof Reader) {
				return ((Reader) value).getResultAndReset();
			}

			value = converter.readValue(value, property.getTypeInformation());

			return value;
		}

		/**
		 * read values for all collection like properties and aggregate them in a collection.
		 */
		void readValues() {
			baseEntity.forEach(this::readValue);
		}

		private void readValue(RelationalPersistentProperty p) {

			if (p.isEntity()) {

				Reader reader = null;

				if (p.isCollectionLike() || p.isMap()) { // even when there are no values we still want a (empty) collection.

					reader = (Reader) aggregatedValues.computeIfAbsent(p, pp -> new CollectionReader(basePath.append(pp), rs));
				}
				if (getIndicatorOf(p) != null) {

					if (!(p.isCollectionLike() || p.isMap())) { // for single entities we want a null entity instead of on filled
																											// with null values.

						reader = (Reader) aggregatedValues.computeIfAbsent(p, pp -> new EntityReader(basePath.append(pp), rs));
					}

					Assert.state(reader != null, "reader must not be null");

					reader.read();
				}
			} else {
				aggregatedValues.computeIfAbsent(p, this::getObject);
			}
		}

		@Nullable
		private Object getIndicatorOf(RelationalPersistentProperty p) {
			if (p.isMap() || List.class.isAssignableFrom(p.getType())) {
				return rs.getObject(getKeyName(p));
			}

			if (p.isEmbedded()) {
				return true;
			}

			return rs.getObject(getColumnName(p));
		}

		/**
		 * Obtain a single columnValue from the resultset without throwing an exception. If the column does not exist a null
		 * value is returned. Does not instantiate complex objects.
		 *
		 * @param property
		 * @return
		 */
		@Nullable
		private Object getObject(RelationalPersistentProperty property) {
			return rs.getObject(getColumnName(property));
		}

		/**
		 * converts a property into a column name representing that property.
		 *
		 * @param property
		 * @return
		 */
		private String getColumnName(RelationalPersistentProperty property) {

			return propertyToColumn.column(basePath.append(property));
		}

		private String getKeyName(RelationalPersistentProperty property) {

			return propertyToColumn.keyColumn(basePath.append(property));
		}

		private boolean hasValue() {

			for (Object value : aggregatedValues.values()) {
				if (value != null) {
					return true;
				}
			}
			return false;
		}
	}
}
