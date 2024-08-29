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

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.SoftAssertions.*;
import static org.springframework.data.relational.core.sql.SqlIdentifier.*;

import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.sql.IdentifierProcessing.LetterCasing;
import org.springframework.data.relational.core.sql.IdentifierProcessing.Quoting;

/**
 * Unit tests for {@link SqlIdentifier}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Kurt Niemi
 */
public class SqlIdentifierUnitTests {

	public static final IdentifierProcessing BRACKETS_LOWER_CASE = IdentifierProcessing.create(new Quoting("[", "]"),
			LetterCasing.LOWER_CASE);

	@Test // DATAJDBC-386
	public void quotedSimpleObjectIdentifier() {

		SqlIdentifier identifier = quoted("someName");

		assertThat(identifier.toSql(BRACKETS_LOWER_CASE)).isEqualTo("[someName]");
		assertThat(identifier.getReference()).isEqualTo("someName");
	}

	@Test // DATAJDBC-386
	public void unquotedSimpleObjectIdentifier() {

		SqlIdentifier identifier = unquoted("someName");
		String sql = identifier.toSql(BRACKETS_LOWER_CASE);

		assertThat(sql).isEqualTo("someName");
		assertThat(identifier.getReference()).isEqualTo("someName");
	}

	@Test // DATAJDBC-386
	public void quotedMultipartObjectIdentifier() {

		SqlIdentifier identifier = SqlIdentifier.from(quoted("some"), quoted("name"));
		String sql = identifier.toSql(IdentifierProcessing.ANSI);

		assertThat(sql).isEqualTo("\"some\".\"name\"");
	}

	@Test // DATAJDBC-386
	public void unquotedMultipartObjectIdentifier() {

		SqlIdentifier identifier = SqlIdentifier.from(unquoted("some"), unquoted("name"));
		String sql = identifier.toSql(IdentifierProcessing.ANSI);

		assertThat(sql).isEqualTo("some.name");
	}

	@Test // DATAJDBC-386
	public void equality() {

		SqlIdentifier basis = SqlIdentifier.unquoted("simple");
		SqlIdentifier equal = SqlIdentifier.unquoted("simple");
		SqlIdentifier quoted = quoted("simple");
		SqlIdentifier notSimple = SqlIdentifier.from(unquoted("simple"), unquoted("not"));

		assertSoftly(softly -> {

			softly.assertThat(basis).isEqualTo(equal);
			softly.assertThat(equal).isEqualTo(basis);
			softly.assertThat(basis).isNotEqualTo(quoted);
			softly.assertThat(basis).isNotEqualTo(notSimple);
		});
	}
}
