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

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.jdbc.core.convert.RowDocumentExtractorSupport.AggregateContext;
import org.springframework.data.jdbc.core.convert.RowDocumentExtractorSupport.RowDocumentSink;
import org.springframework.data.jdbc.core.convert.RowDocumentExtractorSupport.TabularResultAdapter;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.domain.RowDocument;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.LinkedCaseInsensitiveMap;

/**
 * {@link ResultSet}-driven extractor to extract {@link RowDocument documents}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 3.2
 */
class RowDocumentResultSetExtractor {

	private final RelationalMappingContext context;
	private final PathToColumnMapping propertyToColumn;

	RowDocumentResultSetExtractor(RelationalMappingContext context, PathToColumnMapping propertyToColumn) {

		this.context = context;
		this.propertyToColumn = propertyToColumn;
	}

	/**
	 * Create a {@link RowDocument} from the current {@link ResultSet} row.
	 *
	 * @param resultSet must not be {@literal null}.
	 * @return
	 * @throws SQLException
	 */
	static RowDocument toRowDocument(ResultSet resultSet) throws SQLException {

		ResultSetMetaData md = resultSet.getMetaData();
		int columnCount = md.getColumnCount();
		RowDocument document = new RowDocument(columnCount);

		for (int i = 0; i < columnCount; i++) {
			Object rsv = JdbcUtils.getResultSetValue(resultSet, i + 1);
			String columnName = md.getColumnLabel(i + 1);
			document.put(columnName, rsv instanceof Array a ? a.getArray() : rsv);
		}

		return document;
	}

	/**
	 * Adapter to extract values and column metadata from a {@link ResultSet}.
	 */
	enum ResultSetAdapter implements TabularResultAdapter<ResultSet> {

		INSTANCE;

		@Override
		public Object getObject(ResultSet row, int index) {

			try {

				Object resultSetValue = JdbcUtils.getResultSetValue(row, index);

				if (resultSetValue instanceof Array a) {
					return a.getArray();
				}

				return resultSetValue;
			} catch (SQLException e) {
				throw new DataRetrievalFailureException("Cannot retrieve column " + index + " from ResultSet", e);
			}
		}

		@Override
		public Map<String, Integer> getColumnMap(ResultSet result) {

			try {

				ResultSetMetaData metaData = result.getMetaData();
				Map<String, Integer> columns = new LinkedCaseInsensitiveMap<>(metaData.getColumnCount());

				for (int i = 0; i < metaData.getColumnCount(); i++) {
					columns.put(metaData.getColumnLabel(i + 1), i + 1);
				}
				return columns;
			} catch (SQLException e) {
				throw new DataRetrievalFailureException("Cannot retrieve ColumnMap from ResultSet", e);
			}
		}
	}

	/**
	 * Reads the next {@link RowDocument} from the {@link ResultSet}. The result set can be pristine (i.e.
	 * {@link ResultSet#isBeforeFirst()}) or pointing already at a row.
	 *
	 * @param entity entity defining the document structure.
	 * @param resultSet the result set to consume.
	 * @return a {@link RowDocument}.
	 * @throws SQLException if thrown by the JDBC API.
	 * @throws IllegalStateException if the {@link ResultSet#isAfterLast() fully consumed}.
	 */
	public RowDocument extractNextDocument(Class<?> entity, ResultSet resultSet) throws SQLException {
		return extractNextDocument(context.getRequiredPersistentEntity(entity), resultSet);
	}

	/**
	 * Reads the next {@link RowDocument} from the {@link ResultSet}. The result set can be pristine (i.e.
	 * {@link ResultSet#isBeforeFirst()}) or pointing already at a row.
	 *
	 * @param entity entity defining the document structure.
	 * @param resultSet the result set to consume.
	 * @return a {@link RowDocument}.
	 * @throws SQLException if thrown by the JDBC API.
	 * @throws IllegalStateException if the {@link ResultSet#isAfterLast() fully consumed}.
	 */
	public RowDocument extractNextDocument(RelationalPersistentEntity<?> entity, ResultSet resultSet)
			throws SQLException {

		Iterator<RowDocument> iterator = iterate(entity, resultSet);

		if (!iterator.hasNext()) {
			throw new IllegalStateException("ResultSet is fully consumed");
		}

		return iterator.next();
	}

	/**
	 * Obtain a {@link Iterator} to retrieve {@link RowDocument documents} from a {@link ResultSet}.
	 *
	 * @param entity the entity to determine the document structure.
	 * @param rs the input result set.
	 * @return an iterator to consume the {@link ResultSet} as RowDocuments.
	 * @throws SQLException if thrown by the JDBC API.
	 */
	public Iterator<RowDocument> iterate(RelationalPersistentEntity<?> entity, ResultSet rs) throws SQLException {
		return new RowDocumentIterator(entity, rs);
	}

	/**
	 * Iterator implementation that advances through the {@link ResultSet} and feeds its input into a
	 * {@link org.springframework.data.jdbc.core.convert.RowDocumentExtractorSupport.RowDocumentSink}.
	 */
	private class RowDocumentIterator implements Iterator<RowDocument> {

		private final ResultSet resultSet;
		private final AggregatePath rootPath;
		private final RelationalPersistentEntity<?> rootEntity;
		private final Integer identifierIndex;
		private final AggregateContext<ResultSet> aggregateContext;

		/**
		 * Answers the question if the internal {@link ResultSet} points at an actual row.
		 */
		private boolean hasNext;

		RowDocumentIterator(RelationalPersistentEntity<?> entity, ResultSet resultSet) {

			ResultSetAdapter adapter = ResultSetAdapter.INSTANCE;

			this.rootPath = context.getAggregatePath(entity);
			this.rootEntity = entity;

			String idColumn = propertyToColumn.column(rootPath.append(entity.getRequiredIdProperty()));
			Map<String, Integer> columns = adapter.getColumnMap(resultSet);
			this.aggregateContext = new AggregateContext<>(adapter, context, propertyToColumn, columns);

			this.resultSet = resultSet;
			this.identifierIndex = columns.get(idColumn);
			this.hasNext = hasRow(resultSet);
		}

		private static boolean hasRow(ResultSet resultSet) {

			// If we are before the first row we need to advance to the first row.
			try {
				if (resultSet.isBeforeFirst()) {
					return resultSet.next();
				}
			} catch (SQLException e) {
				// seems that isBeforeFirst is not implemented
			}

			// if we are after the last row we are done and not pointing a valid row and also can't advance to one.
			try {
				if (resultSet.isAfterLast()) {
					return false;
				}
			} catch (SQLException e) {
				// seems that isAfterLast is not implemented
			}

			// if we arrived here we know almost nothing.
			// maybe isBeforeFirst or isBeforeLast aren't implemented
			// or the ResultSet is empty.

			try {
				resultSet.getObject(1);
				// we can see actual data, so we are looking at a current row.
				return true;
			} catch (SQLException ignored) {}

			try {
				return resultSet.next();
			} catch (SQLException e) {
				// we aren't looking at a row, but we can't advance either.
				// so it seems we are facing an empty ResultSet
				return false;
			}
		}

		@Override
		public boolean hasNext() {
			return hasNext;
		}

		@Override
		@Nullable
		public RowDocument next() {

			RowDocumentSink<ResultSet> reader = new RowDocumentSink<>(aggregateContext, rootEntity, rootPath);
			Object key = ResultSetAdapter.INSTANCE.getObject(resultSet, identifierIndex);

			try {

				do {
					Object nextKey = ResultSetAdapter.INSTANCE.getObject(resultSet, identifierIndex);

					if (nextKey != null && !nextKey.equals(key)) {
						break;
					}

					reader.accept(resultSet);
					hasNext = resultSet.next();
				} while (hasNext);
			} catch (SQLException e) {
				throw new DataRetrievalFailureException("Cannot advance ResultSet", e);
			}

			return reader.getResult();
		}
	}
}
