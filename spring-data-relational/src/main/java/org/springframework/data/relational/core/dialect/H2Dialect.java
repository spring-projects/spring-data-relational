/*
 * Copyright 2019-2022 the original author or authors.
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
package org.springframework.data.relational.core.dialect;

import java.util.Collections;
import java.util.Set;

import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.IdentifierProcessing.LetterCasing;
import org.springframework.data.relational.core.sql.IdentifierProcessing.Quoting;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * An SQL dialect for H2.
 *
 * @author Mark Paluch
 * @author Myeonghyeon Lee
 * @author Christph Strobl
 * @since 2.0
 */
public class H2Dialect extends AbstractDialect {

	/**
	 * Singleton instance.
	 */
	public static final H2Dialect INSTANCE = new H2Dialect();

	protected H2Dialect() {}

	private static final LimitClause LIMIT_CLAUSE = new LimitClause() {

		@Override
		public String getLimit(long limit) {
			return "LIMIT " + limit;
		}

		@Override
		public String getOffset(long offset) {
			return "OFFSET " + offset;
		}

		@Override
		public String getLimitOffset(long limit, long offset) {
			return String.format("LIMIT %d OFFSET %d", limit, offset);
		}

		@Override
		public Position getClausePosition() {
			return Position.AFTER_ORDER_BY;
		}
	};

	private final H2ArrayColumns ARRAY_COLUMNS = new H2ArrayColumns();

	@Override
	public LimitClause limit() {
		return LIMIT_CLAUSE;
	}

	@Override
	public LockClause lock() {
		return AnsiDialect.LOCK_CLAUSE;
	}

	public ArrayColumns getArraySupport() {
		return ARRAY_COLUMNS;
	}

	static class H2ArrayColumns implements ArrayColumns {

		@Override
		public boolean isSupported() {
			return true;
		}

		@Override
		public Class<?> getArrayType(Class<?> userType) {

			Assert.notNull(userType, "Array component type must not be null");

			return ClassUtils.resolvePrimitiveIfNecessary(userType);
		}
	}

	@Override
	public IdentifierProcessing getIdentifierProcessing() {
		return IdentifierProcessing.create(Quoting.ANSI, LetterCasing.UPPER_CASE);
	}

	@Override
	public Set<Class<?>> simpleTypes() {

		if (!ClassUtils.isPresent("org.h2.api.TimestampWithTimeZone", getClass().getClassLoader())) {
			return Collections.emptySet();
		}
		try {
			return Collections.singleton(ClassUtils.forName("org.h2.api.TimestampWithTimeZone", getClass().getClassLoader()));
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException(e);
		}
	}
}
