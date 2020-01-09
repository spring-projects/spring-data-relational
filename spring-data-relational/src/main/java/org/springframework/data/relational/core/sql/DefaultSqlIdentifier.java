/*
 * Copyright 2020 the original author or authors.
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

import java.util.function.UnaryOperator;

import org.springframework.util.Assert;

/**
 * Default {@link SqlIdentifier} implementation using a {@code name} and whether the identifier is quoted.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 2.0
 */
class DefaultSqlIdentifier implements SqlIdentifier {

	private final String name;
	private final boolean quoted;

	DefaultSqlIdentifier(String name, boolean quoted) {

		Assert.hasText(name, "A database object name must not be null or empty");

		this.name = name;
		this.quoted = quoted;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.domain.SqlIdentifier#transform(java.util.function.UnaryOperator)
	 */
	@Override
	public SqlIdentifier transform(UnaryOperator<String> transformationFunction) {

		Assert.notNull(transformationFunction, "Transformation function must not be null");

		return new DefaultSqlIdentifier(transformationFunction.apply(name), quoted);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.domain.SqlIdentifier#toSql(org.springframework.data.relational.domain.IdentifierProcessing)
	 */
	@Override
	public String toSql(IdentifierProcessing processing) {
		return quoted ? processing.quote(getReference(processing)) : getReference(processing);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.domain.SqlIdentifier#getReference(org.springframework.data.relational.domain.IdentifierProcessing)
	 */
	@Override
	public String getReference(IdentifierProcessing processing) {
		return name;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o) {

		if (this == o)
			return true;
		if (o instanceof SqlIdentifier) {
			return toString().equals(o.toString());
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {

		if (quoted) {
			return toSql(IdentifierProcessing.ANSI);
		}

		return this.name;
	}
}
