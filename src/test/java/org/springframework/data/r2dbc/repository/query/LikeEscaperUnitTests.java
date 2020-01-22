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
package org.springframework.data.r2dbc.repository.query;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.Test;

/**
 * @author Roman Chigvintsev
 */
public class LikeEscaperUnitTests {
	@Test
	public void ignoresNulls() {
		assertNull(LikeEscaper.DEFAULT.escape(null));
	}

	@Test
	public void ignoresEmptyString() {
		assertThat(LikeEscaper.DEFAULT.escape("")).isEqualTo("");
	}

	@Test
	public void ignoresBlankString() {
		assertThat(LikeEscaper.DEFAULT.escape(" ")).isEqualTo(" ");
	}

	@Test(expected = IllegalArgumentException.class)
	public void throwsExceptionWhenEscapeCharacterIsUnderscore() {
		LikeEscaper.of('_');
	}

	@Test(expected = IllegalArgumentException.class)
	public void throwsExceptionWhenEscapeCharacterIsPercent() {
		LikeEscaper.of('%');
	}

	@Test
	public void escapesUnderscoresUsingDefaultEscapeCharacter() {
		assertThat(LikeEscaper.DEFAULT.escape("_test_")).isEqualTo("\\_test\\_");
	}

	@Test
	public void escapesPercentsUsingDefaultEscapeCharacter() {
		assertThat(LikeEscaper.DEFAULT.escape("%test%")).isEqualTo("\\%test\\%");
	}

	@Test
	public void escapesSpecialCharactersUsingCustomEscapeCharacter() {
		assertThat(LikeEscaper.of('$').escape("_%")).isEqualTo("$_$%");
	}

	@Test
	public void doublesEscapeCharacter() {
		assertThat(LikeEscaper.DEFAULT.escape("\\")).isEqualTo("\\\\");
	}
}
