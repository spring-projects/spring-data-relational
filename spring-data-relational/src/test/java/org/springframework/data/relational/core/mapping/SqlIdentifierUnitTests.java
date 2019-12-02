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
package org.springframework.data.relational.core.mapping;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.relational.domain.SqlIdentifier.*;

import org.assertj.core.api.SoftAssertions;
import org.junit.Test;
import org.springframework.data.relational.domain.IdentifierProcessing;
import org.springframework.data.relational.domain.IdentifierProcessing.DefaultIdentifierProcessing;
import org.springframework.data.relational.domain.IdentifierProcessing.LetterCasing;
import org.springframework.data.relational.domain.IdentifierProcessing.Quoting;
import org.springframework.data.relational.domain.SqlIdentifier;

/**
 * Unit tests for SqlIdentifier.
 * 
 * @author Jens Schauder
 */
public class SqlIdentifierUnitTests {

	public static final DefaultIdentifierProcessing BRACKETS_LOWER_CASE = new DefaultIdentifierProcessing(
			new Quoting("[", "]"), LetterCasing.LOWER_CASE);

	@Test // DATAJDBC-386
	public void quotedSimpleObjectIdentifier() {

		SimpleSqlIdentifier identifier = quoted("someName");

		assertThat(identifier.toSql(BRACKETS_LOWER_CASE)).isEqualTo("[someName]");
		assertThat(identifier.toColumnName(BRACKETS_LOWER_CASE)).isEqualTo("someName");

	}

	@Test // DATAJDBC-386
	public void unquotedSimpleObjectIdentifier() {

		SimpleSqlIdentifier identifier = unquoted("someName");
		String sql = identifier.toSql(BRACKETS_LOWER_CASE);

		assertThat(sql).isEqualTo("someName");
		assertThat(identifier.toColumnName(BRACKETS_LOWER_CASE)).isEqualTo("someName");
	}

	@Test // DATAJDBC-386
	public void quotedSimpleObjectIdentifierWithAdjustableLetterCasing() {

		SimpleSqlIdentifier identifier = quoted("someName").withAdjustableLetterCasing();

		assertThat(identifier.toSql(BRACKETS_LOWER_CASE)).isEqualTo("[somename]");
		assertThat(identifier.toColumnName(BRACKETS_LOWER_CASE)).isEqualTo("somename");

	}

	@Test // DATAJDBC-386
	public void unquotedSimpleObjectIdentifierWithAdjustableLetterCasing() {

		SimpleSqlIdentifier identifier = unquoted("someName").withAdjustableLetterCasing();
		String sql = identifier.toSql(BRACKETS_LOWER_CASE);

		assertThat(sql).isEqualTo("somename");
		assertThat(identifier.toColumnName(BRACKETS_LOWER_CASE)).isEqualTo("somename");
	}

	@Test // DATAJDBC-386
	public void quotedMultipartObjectIdentifierWithAdjustableLetterCase() {

		SqlIdentifier identifier = quoted("some").withAdjustableLetterCasing()
				.concat(quoted("name").withAdjustableLetterCasing());
		String sql = identifier.toSql(IdentifierProcessing.ANSI);

		assertThat(sql).isEqualTo("\"SOME\".\"NAME\"");
	}

	@Test // DATAJDBC-386
	public void quotedMultipartObjectIdentifier() {

		SqlIdentifier identifier = quoted("some").concat(quoted("name"));
		String sql = identifier.toSql(IdentifierProcessing.ANSI);

		assertThat(sql).isEqualTo("\"some\".\"name\"");
	}

	@Test // DATAJDBC-386
	public void unquotedMultipartObjectIdentifier() {

		SqlIdentifier identifier = unquoted("some").concat(unquoted("name"));
		String sql = identifier.toSql(IdentifierProcessing.ANSI);

		assertThat(sql).isEqualTo("some.name");
	}

	@Test // DATAJDBC-386
	public void equality() {

		SqlIdentifier basis = SqlIdentifier.unquoted("simple");
		SqlIdentifier equal = SqlIdentifier.unquoted("simple");
		SqlIdentifier quoted = quoted("simple");
		SqlIdentifier notSimple = SqlIdentifier.unquoted("simple").concat(unquoted("not"));

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(basis).isEqualTo(equal);
			softly.assertThat(equal).isEqualTo(basis);
			softly.assertThat(basis).isNotEqualTo(quoted);
			softly.assertThat(basis).isNotEqualTo(notSimple);
		});
	}
}
