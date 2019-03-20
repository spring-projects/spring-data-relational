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
package org.springframework.data.r2dbc.dialect;

import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * An SQL dialect for MySQL.
 *
 * @author Mark Paluch
 */
public class MySqlDialect implements Dialect {

	private static final Set<Class<?>> SIMPLE_TYPES = new HashSet<>(
			Arrays.asList(UUID.class, URL.class, URI.class, InetAddress.class));

	/**
	 * Singleton instance.
	 */
	public static final MySqlDialect INSTANCE = new MySqlDialect();

	private static final BindMarkersFactory ANONYMOUS = BindMarkersFactory.anonymous("?");

	private static final LimitClause LIMIT_CLAUSE = new LimitClause() {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.dialect.LimitClause#getClause(long, long)
		 */
		@Override
		public String getClause(long limit, long offset) {
			return String.format("LIMIT %d,%d", limit, offset);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.dialect.LimitClause#getClause(long)
		 */
		@Override
		public String getClause(long limit) {
			return "LIMIT " + limit;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.dialect.LimitClause#getClausePosition()
		 */
		@Override
		public Position getClausePosition() {
			return Position.END;
		}
	};

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.Dialect#getBindMarkersFactory()
	 */
	@Override
	public BindMarkersFactory getBindMarkersFactory() {
		return ANONYMOUS;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.Dialect#getSimpleTypesKeys()
	 */
	@Override
	public Collection<? extends Class<?>> getSimpleTypes() {
		return SIMPLE_TYPES;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.Dialect#limit()
	 */
	@Override
	public LimitClause limit() {
		return LIMIT_CLAUSE;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.dialect.Dialect#getArraySupport()
	 */
	@Override
	public ArrayColumns getArraySupport() {
		return ArrayColumns.Unsupported.INSTANCE;
	}
}
