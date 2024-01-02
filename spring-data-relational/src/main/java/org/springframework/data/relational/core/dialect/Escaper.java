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
package org.springframework.data.relational.core.dialect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.lang.Nullable;

/**
 * Helper class encapsulating an escape character for LIKE queries and the actually usage of it in escaping
 * {@link String}s.
 *
 * @author Roman Chigvintsev
 * @author Mark Paluch
 * @since 2.0
 */
public class Escaper {

	public static final Escaper DEFAULT = Escaper.of('\\');

	private final char escapeCharacter;
	private final List<String> toReplace;

	private Escaper(char escapeCharacter, List<String> toReplace) {

		if (toReplace.contains(Character.toString(escapeCharacter))) {
			throw new IllegalArgumentException(
					String.format("'%s' and cannot be used as escape character as it should be replaced", escapeCharacter));
		}

		this.escapeCharacter = escapeCharacter;
		this.toReplace = toReplace;
	}

	/**
	 * Creates new instance of this class with the given escape character.
	 *
	 * @param escapeCharacter escape character
	 * @return new instance of {@link Escaper}.
	 * @throws IllegalArgumentException if escape character is one of special characters ('_' and '%')
	 */
	public static Escaper of(char escapeCharacter) {
		return new Escaper(escapeCharacter, Arrays.asList("_", "%"));
	}

	/**
	 * Apply the {@link Escaper} to the given {@code chars}.
	 *
	 * @param chars characters/char sequences that should be escaped.
	 * @return
	 */
	public Escaper withRewriteFor(String... chars) {

		List<String> toReplace = new ArrayList<>(this.toReplace.size() + chars.length);
		toReplace.addAll(this.toReplace);
		toReplace.addAll(Arrays.asList(chars));

		return new Escaper(this.escapeCharacter, toReplace);
	}

	/**
	 * Returns the escape character.
	 *
	 * @return the escape character to use.
	 */
	public char getEscapeCharacter() {
		return escapeCharacter;
	}

	/**
	 * Escapes all special like characters ({@code _}, {@code %}) using the configured escape character.
	 *
	 * @param value value to be escaped
	 * @return escaped value
	 */
	@Nullable
	public String escape(@Nullable String value) {

		if (value == null) {
			return null;
		}

		return toReplace.stream().reduce(value, (it, character) -> it.replace(character, escapeCharacter + character));
	}
}
