/*
 * Copyright 2019-2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.IdentifierProcessing.LetterCasing;
import org.springframework.data.relational.core.sql.IdentifierProcessing.Quoting;
import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * Unit tests for {@link DerivedSqlIdentifier}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Kurt Niemi
 */
public class DerivedSqlIdentifierUnitTests {

	public static final IdentifierProcessing BRACKETS_LOWER_CASE = IdentifierProcessing.create(new Quoting("[", "]"),
			LetterCasing.LOWER_CASE);

	@Test // DATAJDBC-386
	public void quotedSimpleObjectIdentifierWithAdjustableLetterCasing() {

		SqlIdentifier identifier = new DerivedSqlIdentifier("someName", true);

		assertThat(identifier.toSql(BRACKETS_LOWER_CASE)).isEqualTo("[somename]");
		assertThat(identifier.getReference(BRACKETS_LOWER_CASE)).isEqualTo("someName");
		assertThat(identifier.getReference()).isEqualTo("someName");
	}

	@Test // DATAJDBC-386
	public void unquotedSimpleObjectIdentifierWithAdjustableLetterCasing() {

		SqlIdentifier identifier = new DerivedSqlIdentifier("someName", false);
		String sql = identifier.toSql(BRACKETS_LOWER_CASE);

		assertThat(sql).isEqualTo("somename");
		assertThat(identifier.getReference(BRACKETS_LOWER_CASE)).isEqualTo("someName");
		assertThat(identifier.getReference()).isEqualTo("someName");
	}

	@Test // DATAJDBC-386
	public void quotedMultipartObjectIdentifierWithAdjustableLetterCase() {

		SqlIdentifier identifier = SqlIdentifier.from(new DerivedSqlIdentifier("some", true),
				new DerivedSqlIdentifier("name", true));
		String sql = identifier.toSql(IdentifierProcessing.ANSI);

		assertThat(sql).isEqualTo("\"SOME\".\"NAME\"");
	}

	@Test // DATAJDBC-386
	public void equality() {

		SqlIdentifier basis = new DerivedSqlIdentifier("simple", false);
		SqlIdentifier equal = new DerivedSqlIdentifier("simple", false);
		SqlIdentifier quoted = new DerivedSqlIdentifier("simple", true);
		SqlIdentifier notSimple = SqlIdentifier.from(new DerivedSqlIdentifier("simple", false),
				new DerivedSqlIdentifier("not", false));

		assertThat(basis).isEqualTo(equal).isEqualTo(SqlIdentifier.unquoted("simple"))
				.hasSameHashCodeAs(SqlIdentifier.unquoted("simple"));
		assertThat(equal).isEqualTo(basis);
		assertThat(basis).isNotEqualTo(quoted);
		assertThat(basis).isNotEqualTo(notSimple);

		assertThat(quoted).isEqualTo(SqlIdentifier.quoted("SIMPLE")).hasSameHashCodeAs(SqlIdentifier.quoted("SIMPLE"));
	}
}
