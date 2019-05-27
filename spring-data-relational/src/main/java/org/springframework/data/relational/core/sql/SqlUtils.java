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
package org.springframework.data.relational.core.sql;

import lombok.experimental.UtilityClass;

import java.util.regex.Pattern;

import org.springframework.util.StringUtils;

/**
 * Utility class for SQL related functions.
 * 
 * @author Jens Schauder
 * @since 1.1
 */
@UtilityClass
public class SqlUtils {

	private static final Pattern parameterPattern = Pattern.compile("\\W");

	/**
	 * Sanitizes a name so that the result maybe used for example as a bind parameter name or an alias. This is done by
	 * removing all special characters and if any where present appending and '_' in order to avoid resulting with a
	 * keyword.
	 * 
	 * @param name as used for a table or a column. It may contain special characters like quotes.
	 */
	public String sanitizeName(String name) {

		if (StringUtils.isEmpty(name)) {
			return name;
		}

		String sanitized = parameterPattern.matcher(name).replaceAll("");
		return sanitized.equals(name) ? sanitized : sanitized + "_";
	}
}
