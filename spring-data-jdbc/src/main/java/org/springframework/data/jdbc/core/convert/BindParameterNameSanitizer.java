/*
 * Copyright 2023-2024 the original author or authors.
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

package org.springframework.data.jdbc.core.convert;

import java.util.regex.Pattern;

/**
 * Sanitizes the name of bind parameters, so they don't contain any illegal characters.
 *
 * @author Jens Schauder
 * @since 3.0.2
 */
abstract class BindParameterNameSanitizer {

	private static final Pattern parameterPattern = Pattern.compile("\\W");

	static String sanitize(String rawName) {
		return parameterPattern.matcher(rawName).replaceAll("");
	}
}
