/*
 * Copyright 2002-2019 the original author or authors.
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

import lombok.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.r2dbc.dialect.BindMarker;
import org.springframework.data.r2dbc.dialect.BindMarkers;
import org.springframework.data.r2dbc.dialect.BindMarkersFactory;
import org.springframework.data.r2dbc.dialect.BindTarget;
import org.springframework.util.Assert;

/**
 * Helper methods for named parameter parsing.
 * <p>
 * Only intended for internal use within Spring's Data's R2DBC framework. Partially extracted from Spring's JDBC named
 * parameter support.
 * <p>
 * This is a subset of Spring Frameworks's {@code org.springframework.r2dbc.namedparam.NamedParameterUtils}.
 *
 * @author Thomas Risberg
 * @author Juergen Hoeller
 * @author Mark Paluch
 */
abstract class NamedParameterUtils {

	/**
	 * Set of characters that qualify as comment or quotes starting characters.
	 */
	private static final String[] START_SKIP = new String[] { "'", "\"", "--", "/*" };

	/**
	 * Set of characters that at are the corresponding comment or quotes ending characters.
	 */
	private static final String[] STOP_SKIP = new String[] { "'", "\"", "\n", "*/" };

	/**
	 * Set of characters that qualify as parameter separators, indicating that a parameter name in a SQL String has ended.
	 */
	private static final String PARAMETER_SEPARATORS = "\"':&,;()|=+-*%/\\<>^";

	/**
	 * An index with separator flags per character code. Technically only needed between 34 and 124 at this point.
	 */
	private static final boolean[] separatorIndex = new boolean[128];

	static {
		for (char c : PARAMETER_SEPARATORS.toCharArray()) {
			separatorIndex[c] = true;
		}
	}

	// -------------------------------------------------------------------------
	// Core methods used by NamedParameterSupport.
	// -------------------------------------------------------------------------

	/**
	 * Parse the SQL statement and locate any placeholders or named parameters. Named parameters are substituted for a
	 * placeholder.
	 *
	 * @param sql the SQL statement
	 * @return the parsed statement, represented as {@link ParsedSql} instance.
	 */
	public static ParsedSql parseSqlStatement(String sql) {

		Assert.notNull(sql, "SQL must not be null");

		Set<String> namedParameters = new HashSet<>();
		String sqlToUse = sql;
		List<ParameterHolder> parameterList = new ArrayList<>();

		char[] statement = sql.toCharArray();
		int namedParameterCount = 0;
		int unnamedParameterCount = 0;
		int totalParameterCount = 0;

		int escapes = 0;
		int i = 0;
		while (i < statement.length) {
			int skipToPosition = i;
			while (i < statement.length) {
				skipToPosition = skipCommentsAndQuotes(statement, i);
				if (i == skipToPosition) {
					break;
				} else {
					i = skipToPosition;
				}
			}
			if (i >= statement.length) {
				break;
			}
			char c = statement[i];
			if (c == ':' || c == '&') {
				int j = i + 1;
				if (c == ':' && j < statement.length && statement[j] == ':') {
					// Postgres-style "::" casting operator should be skipped
					i = i + 2;
					continue;
				}
				String parameter = null;
				if (c == ':' && j < statement.length && statement[j] == '{') {
					// :{x} style parameter
					while (statement[j] != '}') {
						j++;
						if (j >= statement.length) {
							throw new InvalidDataAccessApiUsageException(
									"Non-terminated named parameter declaration " + "at position " + i + " in statement: " + sql);
						}
						if (statement[j] == ':' || statement[j] == '{') {
							throw new InvalidDataAccessApiUsageException("Parameter name contains invalid character '" + statement[j]
									+ "' at position " + i + " in statement: " + sql);
						}
					}
					if (j - i > 2) {
						parameter = sql.substring(i + 2, j);
						namedParameterCount = addNewNamedParameter(namedParameters, namedParameterCount, parameter);
						totalParameterCount = addNamedParameter(parameterList, totalParameterCount, escapes, i, j + 1, parameter);
					}
					j++;
				} else {
					while (j < statement.length && !isParameterSeparator(statement[j])) {
						j++;
					}
					if (j - i > 1) {
						parameter = sql.substring(i + 1, j);
						namedParameterCount = addNewNamedParameter(namedParameters, namedParameterCount, parameter);
						totalParameterCount = addNamedParameter(parameterList, totalParameterCount, escapes, i, j, parameter);
					}
				}
				i = j - 1;
			} else {
				if (c == '\\') {
					int j = i + 1;
					if (j < statement.length && statement[j] == ':') {
						// escaped ":" should be skipped
						sqlToUse = sqlToUse.substring(0, i - escapes) + sqlToUse.substring(i - escapes + 1);
						escapes++;
						i = i + 2;
						continue;
					}
				}
			}
			i++;
		}
		ParsedSql parsedSql = new ParsedSql(sqlToUse);
		for (ParameterHolder ph : parameterList) {
			parsedSql.addNamedParameter(ph.getParameterName(), ph.getStartIndex(), ph.getEndIndex());
		}
		parsedSql.setNamedParameterCount(namedParameterCount);
		parsedSql.setUnnamedParameterCount(unnamedParameterCount);
		parsedSql.setTotalParameterCount(totalParameterCount);
		return parsedSql;
	}

	private static int addNamedParameter(List<ParameterHolder> parameterList, int totalParameterCount, int escapes, int i,
			int j, String parameter) {

		parameterList.add(new ParameterHolder(parameter, i - escapes, j - escapes));
		totalParameterCount++;
		return totalParameterCount;
	}

	private static int addNewNamedParameter(Set<String> namedParameters, int namedParameterCount, String parameter) {
		if (!namedParameters.contains(parameter)) {
			namedParameters.add(parameter);
			namedParameterCount++;
		}
		return namedParameterCount;
	}

	/**
	 * Skip over comments and quoted names present in an SQL statement.
	 *
	 * @param statement character array containing SQL statement.
	 * @param position current position of statement.
	 * @return next position to process after any comments or quotes are skipped.
	 */
	private static int skipCommentsAndQuotes(char[] statement, int position) {

		for (int i = 0; i < START_SKIP.length; i++) {
			if (statement[position] == START_SKIP[i].charAt(0)) {
				boolean match = true;
				for (int j = 1; j < START_SKIP[i].length(); j++) {
					if (statement[position + j] != START_SKIP[i].charAt(j)) {
						match = false;
						break;
					}
				}
				if (match) {
					int offset = START_SKIP[i].length();
					for (int m = position + offset; m < statement.length; m++) {
						if (statement[m] == STOP_SKIP[i].charAt(0)) {
							boolean endMatch = true;
							int endPos = m;
							for (int n = 1; n < STOP_SKIP[i].length(); n++) {
								if (m + n >= statement.length) {
									// last comment not closed properly
									return statement.length;
								}
								if (statement[m + n] != STOP_SKIP[i].charAt(n)) {
									endMatch = false;
									break;
								}
								endPos = m + n;
							}
							if (endMatch) {
								// found character sequence ending comment or quote
								return endPos + 1;
							}
						}
					}
					// character sequence ending comment or quote not found
					return statement.length;
				}
			}
		}
		return position;
	}

	/**
	 * Parse the SQL statement and locate any placeholders or named parameters. Named parameters are substituted for a
	 * native placeholder, and any select list is expanded to the required number of placeholders. Select lists may
	 * contain an array of objects, and in that case the placeholders will be grouped and enclosed with parentheses. This
	 * allows for the use of "expression lists" in the SQL statement like: <br />
	 * <br />
	 * {@code select id, name, state from table where (name, age) in (('John', 35), ('Ann', 50))}
	 * <p>
	 * The parameter values passed in are used to determine the number of placeholders to be used for a select list.
	 * Select lists should be limited to 100 or fewer elements. A larger number of elements is not guaranteed to be
	 * supported by the database and is strictly vendor-dependent.
	 *
	 * @param parsedSql the parsed representation of the SQL statement.
	 * @param bindMarkersFactory the bind marker factory.
	 * @param paramSource the source for named parameters.
	 * @return the expanded query that accepts bind parameters and allows for execution without further translation.
	 * @see #parseSqlStatement
	 */
	public static BindableOperation substituteNamedParameters(ParsedSql parsedSql, BindMarkersFactory bindMarkersFactory,
			BindParameterSource paramSource) {

		BindMarkerHolder markerHolder = new BindMarkerHolder(bindMarkersFactory.create());

		String originalSql = parsedSql.getOriginalSql();
		List<String> paramNames = parsedSql.getParameterNames();
		if (paramNames.isEmpty()) {
			return new ExpandedQuery(originalSql, markerHolder);
		}

		StringBuilder actualSql = new StringBuilder(originalSql.length());
		int lastIndex = 0;
		for (int i = 0; i < paramNames.size(); i++) {
			String paramName = paramNames.get(i);
			int[] indexes = parsedSql.getParameterIndexes(i);
			int startIndex = indexes[0];
			int endIndex = indexes[1];
			actualSql.append(originalSql, lastIndex, startIndex);
			if (paramSource.hasValue(paramName)) {
				Object value = paramSource.getValue(paramName);
				if (value instanceof Collection) {
					Iterator<?> entryIter = ((Collection<?>) value).iterator();
					int k = 0;
					while (entryIter.hasNext()) {
						if (k > 0) {
							actualSql.append(", ");
						}
						k++;
						Object entryItem = entryIter.next();
						if (entryItem instanceof Object[]) {
							Object[] expressionList = (Object[]) entryItem;
							actualSql.append('(');
							for (int m = 0; m < expressionList.length; m++) {
								if (m > 0) {
									actualSql.append(", ");
								}
								actualSql.append(markerHolder.addMarker(paramName));
							}
							actualSql.append(')');
						} else {
							actualSql.append(markerHolder.addMarker(paramName));
						}

					}
				} else {
					actualSql.append(markerHolder.addMarker(paramName));
				}
			} else {
				actualSql.append(markerHolder.addMarker(paramName));
			}
			lastIndex = endIndex;
		}
		actualSql.append(originalSql, lastIndex, originalSql.length());

		return new ExpandedQuery(actualSql.toString(), markerHolder);
	}

	/**
	 * Determine whether a parameter name ends at the current position, that is, whether the given character qualifies as
	 * a separator.
	 */
	private static boolean isParameterSeparator(char c) {
		return (c < 128 && separatorIndex[c]) || Character.isWhitespace(c);
	}

	// -------------------------------------------------------------------------
	// Convenience methods operating on a plain SQL String
	// -------------------------------------------------------------------------

	/**
	 * Parse the SQL statement and locate any placeholders or named parameters. Named parameters are substituted for a
	 * native placeholder and any select list is expanded to the required number of placeholders.
	 * <p>
	 *
	 * @param sql the SQL statement.
	 * @param bindMarkersFactory the bind marker factory.
	 * @param paramSource the source for named parameters.
	 * @return the expanded query that accepts bind parameters and allows for execution without further translation.
	 */
	public static BindableOperation substituteNamedParameters(String sql, BindMarkersFactory bindMarkersFactory,
			BindParameterSource paramSource) {
		ParsedSql parsedSql = parseSqlStatement(sql);
		return substituteNamedParameters(parsedSql, bindMarkersFactory, paramSource);
	}

	@Value
	private static class ParameterHolder {

		String parameterName;

		int startIndex;

		int endIndex;
	}

	/**
	 * Holder for bind marker progress.
	 */
	private static class BindMarkerHolder {

		private final BindMarkers bindMarkers;
		private final Map<String, List<BindMarker>> markers = new TreeMap<>();

		BindMarkerHolder(BindMarkers bindMarkers) {
			this.bindMarkers = bindMarkers;
		}

		String addMarker(String name) {

			BindMarker bindMarker = bindMarkers.next(name);
			markers.computeIfAbsent(name, ignore -> new ArrayList<>()).add(bindMarker);
			return bindMarker.getPlaceholder();
		}
	}

	/**
	 * Expanded query that allows binding of parameters using parameter names that were used to expand the query. Binding
	 * unrolls {@link Collection}s and nested arrays.
	 */
	private static class ExpandedQuery implements BindableOperation {

		private final String expandedSql;

		private final Map<String, List<BindMarker>> markers;

		ExpandedQuery(String expandedSql, BindMarkerHolder bindMarkerHolder) {
			this.expandedSql = expandedSql;
			this.markers = bindMarkerHolder.markers;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.BindableOperation#bind(BindTarget, java.lang.String, java.lang.Object)
		 */
		@Override
		@SuppressWarnings("unchecked")
		public void bind(BindTarget target, String identifier, Object value) {

			List<BindMarker> bindMarkers = getBindMarkers(identifier);

			if (bindMarkers == null) {

				target.bind(identifier, value);
				return;
			}

			if (bindMarkers.size() == 1) {
				bindMarkers.get(0).bind(target, value);
			} else {

				Assert.isInstanceOf(Collection.class, value,
						() -> String.format("Value [%s] must be an Collection with a size of [%d]", value, bindMarkers.size()));

				Collection<Object> collection = (Collection<Object>) value;

				Iterator<Object> iterator = collection.iterator();
				Iterator<BindMarker> markers = bindMarkers.iterator();

				while (iterator.hasNext()) {

					Object valueToBind = iterator.next();

					if (valueToBind instanceof Object[]) {
						Object[] objects = (Object[]) valueToBind;
						for (Object object : objects) {
							bind(target, markers, object);
						}
					} else {
						bind(target, markers, valueToBind);
					}
				}
			}
		}

		private void bind(BindTarget target, Iterator<BindMarker> markers, Object valueToBind) {

			Assert.isTrue(markers.hasNext(),
					() -> String.format(
							"No bind marker for value [%s] in SQL [%s]. Check that the query was expanded using the same arguments.",
							valueToBind, toQuery()));

			markers.next().bind(target, valueToBind);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.BindableOperation#bindNull(BindTarget, java.lang.String, java.lang.Class)
		 */
		@Override
		public void bindNull(BindTarget target, String identifier, Class<?> valueType) {

			List<BindMarker> bindMarkers = getBindMarkers(identifier);

			if (bindMarkers == null) {

				target.bindNull(identifier, valueType);
				return;
			}

			if (bindMarkers.size() == 1) {
				bindMarkers.get(0).bindNull(target, valueType);
				return;
			}

			throw new UnsupportedOperationException("bindNull(…) can bind only singular values");
		}

		private List<BindMarker> getBindMarkers(String identifier) {
			return markers.get(identifier);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.QueryOperation#toQuery()
		 */
		@Override
		public String toQuery() {
			return expandedSql;
		}
	}
}
