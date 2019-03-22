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
package org.springframework.data.r2dbc.function;

import io.r2dbc.spi.Statement;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.r2dbc.dialect.BindMarkersFactory;

/**
 * SQL translation support allowing the use of named parameters rather than native placeholders.
 * <p>
 * This class expands SQL from named parameters to native style placeholders at execution time. It also allows for
 * expanding a {@link java.util.List} of values to the appropriate number of placeholders.
 * <p>
 * <b>NOTE: An instance of this class is thread-safe once configured.</b>
 *
 * @author Mark Paluch
 */
public class NamedParameterExpander {

	/**
	 * Default maximum number of entries for the SQL cache: 256.
	 */
	public static final int DEFAULT_CACHE_LIMIT = 256;

	private volatile int cacheLimit = DEFAULT_CACHE_LIMIT;

	private final Log logger = LogFactory.getLog(getClass());

	/**
	 * Cache of original SQL String to ParsedSql representation.
	 */
	@SuppressWarnings("serial") private final Map<String, ParsedSql> parsedSqlCache = new LinkedHashMap<String, ParsedSql>(
			DEFAULT_CACHE_LIMIT, 0.75f, true) {
		@Override
		protected boolean removeEldestEntry(Map.Entry<String, ParsedSql> eldest) {
			return size() > getCacheLimit();
		}
	};

	private NamedParameterExpander() {}

	/**
	 * Creates a disabled instance of {@link NamedParameterExpander}.
	 *
	 * @return a disabled instance of {@link NamedParameterExpander}.
	 */
	public static NamedParameterExpander disabled() {
		return Disabled.INSTANCE;
	}

	/**
	 * Creates a new enabled instance of {@link NamedParameterExpander}.
	 *
	 * @return a new enabled instance of {@link NamedParameterExpander}.
	 */
	public static NamedParameterExpander enabled() {
		return new NamedParameterExpander();
	}

	/**
	 * Specify the maximum number of entries for the SQL cache. Default is 256.
	 */
	public void setCacheLimit(int cacheLimit) {
		this.cacheLimit = cacheLimit;
	}

	/**
	 * Return the maximum number of entries for the SQL cache.
	 */
	public int getCacheLimit() {
		return this.cacheLimit;
	}

	/**
	 * Obtain a parsed representation of the given SQL statement.
	 * <p>
	 * The default implementation uses an LRU cache with an upper limit of 256 entries.
	 *
	 * @param sql the original SQL statement
	 * @return a representation of the parsed SQL statement
	 */
	protected ParsedSql getParsedSql(String sql) {

		if (getCacheLimit() <= 0) {
			return NamedParameterUtils.parseSqlStatement(sql);
		}

		synchronized (this.parsedSqlCache) {

			ParsedSql parsedSql = this.parsedSqlCache.get(sql);
			if (parsedSql == null) {

				parsedSql = NamedParameterUtils.parseSqlStatement(sql);
				this.parsedSqlCache.put(sql, parsedSql);
			}
			return parsedSql;
		}
	}

	BindableOperation expand(String sql, BindMarkersFactory bindMarkersFactory, BindParameterSource paramSource) {

		ParsedSql parsedSql = getParsedSql(sql);

		BindableOperation expanded = NamedParameterUtils.substituteNamedParameters(parsedSql, bindMarkersFactory,
				paramSource);

		if (logger.isDebugEnabled()) {
			logger.debug(String.format("Expanding SQL statement [%s] to [%s]", sql, expanded.toQuery()));
		}

		return expanded;
	}

	/**
	 * Disabled named parameter support.
	 */
	static class Disabled extends NamedParameterExpander {

		private static final Disabled INSTANCE = new Disabled();

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.NamedParameterSupport#expand(java.lang.String, org.springframework.data.r2dbc.dialect.BindMarkersFactory, org.springframework.data.r2dbc.function.SqlParameterSource)
		 */
		@Override
		BindableOperation expand(String sql, BindMarkersFactory bindMarkersFactory, BindParameterSource paramSource) {

			return new BindableOperation() {

				@Override
				public void bind(Statement statement, String identifier, Object value) {
					statement.bind(identifier, value);
				}

				@Override
				public void bindNull(Statement statement, String identifier, Class<?> valueType) {
					statement.bindNull(identifier, valueType);
				}

				@Override
				public String toQuery() {
					return sql;
				}
			};
		}
	}
}
