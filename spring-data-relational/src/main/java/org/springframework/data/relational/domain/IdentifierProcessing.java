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

/**
 * An interface describing the processing steps for the conversion of {@link SqlIdentifier} to SQL snippets or column
 * names.
 * 
 * @author Jens Schauder
 * @since 2.0
 */
public interface IdentifierProcessing {

	/**
	 * An {@link IdentifierProcessing} that can be used for databases adhering to the SQL standard which uses double
	 * quotes ({@literal "}) for quoting and makes unquoted literals equivalent to upper case.
	 */
	IdentifierProcessing ANSI = new DefaultIdentifierProcessing(Quoting.ANSI, LetterCasing.UPPER_CASE);

	/**
	 * Converts a {@link String} representing a bare name of an identifier to a {@link String} with proper quoting
	 * applied.
	 *
	 * @param identifier the name of an identifier. Must not be {@literal null}.
	 * @return a quoted name of an identifier. Guaranteed to be not {@literal null}.
	 */
	String quote(String identifier);

	/**
	 * Standardizes the use of upper and lower case letters in an identifier in such a way that semantically the same
	 * identifier results from the quoted and the unquoted version. If this is not possible use of
	 * {@link LetterCasing#AS_IS} is recommended.
	 * 
	 * @param identifier an identifier with arbitrary upper and lower cases. must not be {@literal null}.
	 * @return an identifier with standardized use of upper and lower case letter. Guaranteed to be not {@literal null}.
	 */
	String standardizeLetterCase(String identifier);

	/**
	 * An {@link IdentifierProcessing} implementation based on two implementations for the quoting and for the letter case
	 * standardization.
	 */
	class DefaultIdentifierProcessing implements IdentifierProcessing {

		private final Quoting quoting;
		private final LetterCasing letterCasing;

		public DefaultIdentifierProcessing(Quoting quoting, LetterCasing letterCasing) {
			this.quoting = quoting;
			this.letterCasing = letterCasing;
		}

		@Override
		public String quote(String identifier) {
			return quoting.apply(identifier);
		}

		@Override
		public String standardizeLetterCase(String identifier) {
			return letterCasing.apply(identifier);
		}
	}

	/**
	 * A conversion from unquoted identifiers to quoted identifiers.
	 * 
	 * @author Jens Schauder
	 * @since 2.0
	 */
	class Quoting {

		public static final Quoting ANSI = new Quoting("\"");

		private final String prefix;
		private final String suffix;

		/**
		 * Constructs a {@literal Quoting} with potential different prefix and suffix used for quoting.
		 * 
		 * @param prefix a {@literal String} prefixed before the name for quoting it.
		 * @param suffix a {@literal String} suffixed at the end of the name for quoting it.
		 */
		public Quoting(String prefix, String suffix) {

			this.prefix = prefix;
			this.suffix = suffix;
		}

		/**
		 * Constructs a {@literal Quoting} with the same {@literal String} appended in front and end of an identifier.
		 * 
		 * @param quoteCharacter the value appended at the beginning and the end of a name in order to quote it.
		 */
		public Quoting(String quoteCharacter) {
			this(quoteCharacter, quoteCharacter);
		}

		public String apply(String identifier) {
			return prefix + identifier + suffix;
		}
	}

	/**
	 * Encapsulates the three kinds of letter casing supported.
	 *
	 * @author Jens Schauder
	 * @since 2.0
	 */
	enum LetterCasing {

		UPPER_CASE {

			@Override
			public String apply(String identifier) {
				return identifier.toUpperCase();
			}
		},

		LOWER_CASE {

			@Override
			public String apply(String identifier) {
				return identifier.toLowerCase();
			}
		},

		AS_IS {

			@Override
			public String apply(String identifier) {
				return identifier;
			}
		};

		abstract String apply(String identifier);
	}

}
