/*
 * Copyright 2021-2024 the original author or authors.
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
package org.springframework.data.r2dbc.dialect;

import org.springframework.r2dbc.core.binding.BindMarkersFactory;

/**
 * An SQL dialect for Oracle.
 *
 * @author Mark Paluch
 * @since 1.2.6
 */
public class OracleDialect extends org.springframework.data.relational.core.dialect.OracleDialect
		implements R2dbcDialect {

	/**
	 * Singleton instance.
	 */
	public static final OracleDialect INSTANCE = new OracleDialect();

	private static final BindMarkersFactory NAMED = BindMarkersFactory.named(":", "P", 32,
			OracleDialect::filterBindMarker);

	@Override
	public BindMarkersFactory getBindMarkersFactory() {
		return NAMED;
	}

	private static String filterBindMarker(CharSequence input) {

		StringBuilder builder = new StringBuilder();

		for (int i = 0; i < input.length(); i++) {

			char ch = input.charAt(i);

			// ascii letter or digit
			if (Character.isLetterOrDigit(ch) && ch < 127) {
				builder.append(ch);
			}
		}

		if (builder.isEmpty()) {
			return "";
		}

		return "_" + builder;
	}

}
