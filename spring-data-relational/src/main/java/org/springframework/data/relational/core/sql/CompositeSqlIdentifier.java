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
package org.springframework.data.relational.core.sql;

import java.util.Arrays;
import java.util.Iterator;
import java.util.StringJoiner;
import java.util.function.UnaryOperator;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Composite {@link SqlIdentifier}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 2.0
 */
class CompositeSqlIdentifier implements SqlIdentifier {

	private final SqlIdentifier[] parts;

	CompositeSqlIdentifier(SqlIdentifier... parts) {

		Assert.notNull(parts, "SqlIdentifier parts must not be null");
		Assert.noNullElements(parts, "SqlIdentifier parts must not contain null elements");
		Assert.isTrue(parts.length > 0, "SqlIdentifier parts must not be empty");

		this.parts = parts;
	}

	@Override
	public Iterator<SqlIdentifier> iterator() {
		return Arrays.asList(parts).iterator();
	}

	@Override
	public SqlIdentifier transform(UnaryOperator<String> transformationFunction) {
		throw new UnsupportedOperationException("Composite SQL Identifiers cannot be transformed");
	}

	@Override
	public String toSql(IdentifierProcessing processing) {

		StringJoiner stringJoiner = new StringJoiner(".");

		for (SqlIdentifier namePart : parts) {
			stringJoiner.add(namePart.toSql(processing));
		}

		return stringJoiner.toString();
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
		return toSql(IdentifierProcessing.ANSI);
	}
}
