/*
 * Copyright 2018-2025 the original author or authors.
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
package org.springframework.data.relational.core.mapping;

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPaths;

/**
 * @author Jens Schauder
 */
public final class PersistentPropertyPathTestUtils {

	private PersistentPropertyPathTestUtils() {
		throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
	}

	public static PersistentPropertyPath<RelationalPersistentProperty> getPath(RelationalMappingContext context,
																			   String path, Class<?> baseType) {

		PersistentPropertyPaths<?, RelationalPersistentProperty> persistentPropertyPaths = context
				.findPersistentPropertyPaths(baseType, p -> true);

		return persistentPropertyPaths
				.filter(p -> p.toDotPath().equals(path))
				.stream()
				.findFirst()
				.orElse(null);

	}
}
