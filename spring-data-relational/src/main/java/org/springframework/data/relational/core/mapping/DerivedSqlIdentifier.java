/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.relational.core.mapping;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.UnaryOperator;

import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link SqlIdentifier} that is derived from a property name or class name to infer the defaults provided by a
 * {@link NamingStrategy}.
 *
 * @author Mark Paluch
 * @author Kurt Niemi
 * @since 2.0
 */
class DerivedSqlIdentifier implements SqlIdentifier {

	private final String name;
	private final boolean quoted;
	private final String toString;
	private volatile @Nullable CachedSqlName sqlName;

	DerivedSqlIdentifier(String name, boolean quoted) {

		Assert.hasText(name, "A database object must have at least on name part.");
		this.name = name;
		this.quoted = quoted;
		this.toString = quoted ? toSql(IdentifierProcessing.ANSI) : this.name;
	}

	@Override
	public Iterator<SqlIdentifier> iterator() {
		return Collections.<SqlIdentifier> singleton(this).iterator();
	}

	@Override
	public SqlIdentifier transform(UnaryOperator<String> transformationFunction) {

		Assert.notNull(transformationFunction, "Transformation function must not be null");

		return new DerivedSqlIdentifier(transformationFunction.apply(name), quoted);
	}

	@Override
	public String toSql(IdentifierProcessing processing) {

		// using a local copy of volatile this.sqlName to ensure thread safety.
		CachedSqlName sqlName = this.sqlName;
		if (sqlName == null || sqlName.processing != processing) {

			String normalized = processing.standardizeLetterCase(name);
			this.sqlName = sqlName = new CachedSqlName(processing, quoted ? processing.quote(normalized) : normalized);
			return sqlName.sqlName();
		}

		return sqlName.sqlName();
	}

	@Override
	public boolean equals(@Nullable Object o) {

		if (this == o) {
			return true;
		}

		if (o instanceof SqlIdentifier) {
			return toString().equals(o.toString());
		}

		return false;
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public String toString() {
		return toString;
	}

	record CachedSqlName(IdentifierProcessing processing, String sqlName) {
	}
}
