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
package org.springframework.data.relational.domain;

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

import org.springframework.util.Assert;

/**
 * Represents a named object that exists in the database like a table name or a column name
 * 
 * @author Jens Schauder
 * @since 2.0
 */
public interface SqlIdentifier {

	SimpleSqlIdentifier prefix(SqlIdentifier prefix, String separator);

	SqlIdentifier suffix(String suffix);

	SqlIdentifier concat(SqlIdentifier second);

	String toSql(IdentifierProcessing processing);

	String toColumnName(IdentifierProcessing processing);

	static SimpleSqlIdentifier quoted(String name) {
		return new SimpleSqlIdentifier(name, true, true);
	}

	static SimpleSqlIdentifier unquoted(String name) {
		return new SimpleSqlIdentifier(name, false, true);
	}

	SqlIdentifier EMPTY = new SqlIdentifier() {

		@Override
		public SimpleSqlIdentifier prefix(SqlIdentifier prefix, String separator) {
			throw new UnsupportedOperationException("We can't prefix an empty DatabaseObjectIdentifier");
		}

		@Override
		public SqlIdentifier suffix(String suffix) {
			throw new UnsupportedOperationException("We can't suffix an empty DatabaseObjectIdentifier");
		}

		@Override
		public SqlIdentifier concat(SqlIdentifier second) {
			return second;
		}

		@Override
		public String toSql(IdentifierProcessing processing) {
			throw new UnsupportedOperationException("An empty SqlIdentifier can't be used in to create SQL snippets");
		}

		@Override
		public String toColumnName(IdentifierProcessing processing) {
			throw new UnsupportedOperationException("An empty SqlIdentifier can't be used in to create column names");
		}

		public String toString() {
			return "<NULL-IDENTIFIER>";
		}
	};

	final class SimpleSqlIdentifier implements SqlIdentifier {

		private final String name;
		private final boolean quoted;
		private final boolean fixedLetterCasing;

		private SimpleSqlIdentifier(String name, boolean quoted, boolean fixedLetterCasing) {

			Assert.hasText(name, "A database object must have at least on name part.");

			this.name = name;
			this.quoted = quoted;
			this.fixedLetterCasing = fixedLetterCasing;
		}

		public SimpleSqlIdentifier withAdjustableLetterCasing() {
			return new SimpleSqlIdentifier(name, quoted, false);
		}

		public SimpleSqlIdentifier prefix(String prefix) {
			return new SimpleSqlIdentifier(prefix + name, quoted, fixedLetterCasing);
		}

		@Override
		public SimpleSqlIdentifier prefix(SqlIdentifier prefix, String separator) {

			Assert.isInstanceOf(SimpleSqlIdentifier.class, prefix, "Prefixing is only supported for simple SqlIdentifier");

			return new SimpleSqlIdentifier(((SimpleSqlIdentifier) prefix).name + separator + name, quoted, fixedLetterCasing);
		}

		public SimpleSqlIdentifier suffix(String suffix) {
			return new SimpleSqlIdentifier(name + suffix, quoted, fixedLetterCasing);
		}

		@Override
		public boolean equals(Object o) {

			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;
			SimpleSqlIdentifier that = (SimpleSqlIdentifier) o;
			return quoted == that.quoted && Objects.equals(name, that.name);
		}

		@Override
		public int hashCode() {

			int result = Objects.hash(quoted);
			result = 31 * result + name.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "DatabaseObjectIdentifier{" + "name=" + name + ", quoted=" + quoted + '}';
		}

		@Override
		public SqlIdentifier concat(SqlIdentifier second) {
			// TODO: this is completely broken and needs fixing.
			return new CombinedSqlIdentifier(this, (SimpleSqlIdentifier) second);
		}

		@Override
		public String toSql(IdentifierProcessing processing) {

			return quoted ? processing.quote(toColumnName(processing)) : toColumnName(processing);
		}

		@Override
		public String toColumnName(IdentifierProcessing processing) {
			return fixedLetterCasing ? name : processing.standardizeLetterCase(name);
		}
	}

	final class CombinedSqlIdentifier implements SqlIdentifier {

		private final SimpleSqlIdentifier[] parts;

		private CombinedSqlIdentifier(SimpleSqlIdentifier... parts) {
			this.parts = parts;
		}

		@Override
		public SqlIdentifier concat(SqlIdentifier second) {
			throw new UnsupportedOperationException();
		}

		@Override
		public SimpleSqlIdentifier prefix(SqlIdentifier prefix, String separator) {
			throw new UnsupportedOperationException();
		}

		@Override
		public SqlIdentifier suffix(String suffix) {
			throw new UnsupportedOperationException();
		}

		@Override
		public String toSql(IdentifierProcessing processing) {

			StringJoiner stringJoiner = new StringJoiner(".");

			for (SimpleSqlIdentifier namePart : parts) {
				stringJoiner.add(namePart.toSql(processing));
			}

			return stringJoiner.toString();
		}

		@Override
		public String toColumnName(IdentifierProcessing processing) {
			throw new UnsupportedOperationException("A CombinedSqlIdentifier can't be used as a column name");
		}

		@Override
		public String toString() {
			return "CombinedDatabaseObjectIdentifier{" + "parts=" + Arrays.toString(parts) + '}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;
			CombinedSqlIdentifier that = (CombinedSqlIdentifier) o;
			return Arrays.equals(parts, that.parts);
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(parts);
		}
	}
}
