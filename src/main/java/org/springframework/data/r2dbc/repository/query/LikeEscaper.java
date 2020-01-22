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

import java.util.Arrays;
import java.util.List;

import org.springframework.lang.Nullable;

/**
 * Helper class encapsulating an escape character for LIKE queries and the actually usage of it in escaping
 * {@link String}s.
 * <p>
 * This class is an adapted version of {@code org.springframework.data.jpa.repository.query.EscapeCharacter} from
 * Spring Data JPA project.
 *
 * @author Roman Chigvintsev
 */
public class LikeEscaper {
	public static final LikeEscaper DEFAULT = LikeEscaper.of('\\');

	private final char escapeCharacter;
	private final List<String> toReplace;

	private LikeEscaper(char escapeCharacter) {
		if (escapeCharacter == '_' || escapeCharacter == '%') {
			throw new IllegalArgumentException("'_' and '%' are special characters and cannot be used as "
					+ "escape character");
		}
		this.escapeCharacter = escapeCharacter;
		this.toReplace = Arrays.asList(String.valueOf(escapeCharacter), "_", "%");
	}

	/**
	 * Creates new instance of this class with the given escape character.
	 *
	 * @param escapeCharacter escape character
	 * @return new instance of {@link LikeEscaper}
	 * @throws IllegalArgumentException if escape character is one of special characters ('_' and '%')
	 */
	public static LikeEscaper of(char escapeCharacter) {
		return new LikeEscaper(escapeCharacter);
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
