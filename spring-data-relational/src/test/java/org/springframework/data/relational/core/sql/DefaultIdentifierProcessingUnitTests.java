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

import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.sql.IdentifierProcessing.LetterCasing;
import org.springframework.data.relational.core.sql.IdentifierProcessing.Quoting;

/**
 * unit tests for {@link DefaultIdentifierProcessing}.
 *
 * @author Jens Schauder
 */
public class DefaultIdentifierProcessingUnitTests {

	@Test // DATAJDBC-386
	public void ansiConformProcessing() {

		DefaultIdentifierProcessing processing = IdentifierProcessing.create(Quoting.ANSI, LetterCasing.UPPER_CASE);

		assertThat(processing.quote("something")).isEqualTo("\"something\"");
		assertThat(processing.standardizeLetterCase("aBc")).isEqualTo("ABC");
	}

	@Test // DATAJDBC-386
	public void twoCharacterAsIs() {

		DefaultIdentifierProcessing processing = IdentifierProcessing.create(new Quoting("[", "]"), LetterCasing.AS_IS);

		assertThat(processing.quote("something")).isEqualTo("[something]");
		assertThat(processing.standardizeLetterCase("aBc")).isEqualTo("aBc");
	}
}
