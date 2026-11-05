/*
 * Copyright 2020-present the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Escaper}.
 *
 * @author Roman Chigvintsev
 * @author Mark Paluch
 * @author Alexander Tochin
 * @author Jens Schauder
 */
public class EscaperUnitTests {

	@Test // DATAJDBC-514
	public void ignoresNulls() {
		assertThat((Escaper.ANSI_LIKE_ESCAPER.escape(null))).isNull();
	}

	@Test // DATAJDBC-514
	public void ignoresEmptyString() {
		assertThat(Escaper.ANSI_LIKE_ESCAPER.escape("")).isEmpty();
	}

	@Test // DATAJDBC-514
	public void ignoresBlankString() {
		assertThat(Escaper.ANSI_LIKE_ESCAPER.escape(" ")).isEqualTo(" ");
	}

	@Test // DATAJDBC-514
	public void throwsExceptionWhenEscapeCharacterIsUnderscore() {
		assertThatIllegalArgumentException().isThrownBy(() -> Escaper.rewriteLikeWith('_'));
	}

	@Test // DATAJDBC-514
	public void throwsExceptionWhenEscapeCharacterIsPercent() {
		assertThatIllegalArgumentException().isThrownBy(() -> Escaper.rewriteLikeWith('%'));
	}

	@Test // DATAJDBC-514
	public void escapesUnderscoresUsingDefaultEscapeCharacter() {
		assertThat(Escaper.ANSI_LIKE_ESCAPER.escape("_test_")).isEqualTo("\\_test\\_");
	}

	@Test // DATAJDBC-514
	public void escapesPercentsUsingDefaultEscapeCharacter() {
		assertThat(Escaper.ANSI_LIKE_ESCAPER.escape("%test%")).isEqualTo("\\%test\\%");
	}

	@Test // DATAJDBC-514
	public void escapesSpecialCharactersUsingCustomEscapeCharacter() {
		assertThat(Escaper.rewriteLikeWith('$').escape("_%")).isEqualTo("$_$%");
	}

	@Test // DATAJDBC-514
	public void escapesAdditionalCharacters() {
		assertThat(Escaper.ANSI_LIKE_ESCAPER.withRewriteFor("[", "]").escape("Hello Wo[Rr]ld")).isEqualTo("Hello Wo\\[Rr\\]ld");
	}

	@Test // GH-2182
	public void escapesCharactersUsingDefaultEscapeCharacter() {
		assertThat(Escaper.ANSI_LIKE_ESCAPER.escape("%te\\st_")).isEqualTo("\\%te\\\\st\\_");
	}

	@Test // GH-2182
	public void escapesCharactersUsingCustomEscapeCharacter() {

		assertThat(Escaper.ANSI_LIKE_ESCAPER.escape("%te\\st_")).isEqualTo("\\%te\\\\st\\_");
		assertThat(Escaper.rewriteLikeWith('$').escape("%te$st_")).isEqualTo("$%te$$st$_");
	}

	@Test // GH-2325
	public void allowsEscapeCharacterToBePartOfCharactersToReplace() {
		assertThatNoException().isThrownBy(() -> Escaper.rewriting("'").with('\''));
	}

	@Test // GH-2325
	public void doublesSingleQuoteWhenEscapeCharacterIsSingleQuote() {

		Escaper escaper = Escaper.rewriting("'", "x").with('\'');

		assertThat(escaper.escape("O'Brien")).isEqualTo("O''Brien");
		assertThat(escaper.escape("''")).isEqualTo("''''");
	}

	@Test // GH-2325
	public void doesNotEscapeCharactersThatAreNotConfiguredToBeReplaced() {

		Escaper escaper = Escaper.rewriting("'").with('\'');

		assertThat(escaper.escape("63%_is h_p")).isEqualTo("63%_is h_p");
	}

	@Test // GH-2325
	public void escapeCharacterIsHandledExactlyOnceWhenPartOfCharactersToReplace() {

		// step 1 doubles the escape character; step 2 must skip it so it is not doubled again
		Escaper escaper = Escaper.rewriting("'", "%").with('\'');

		assertThat(escaper.escape("a'b%c")).isEqualTo("a''b'%c");
	}
}
