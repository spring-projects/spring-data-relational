/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.mapping.model;

import lombok.experimental.UtilityClass;

import org.springframework.data.mapping.PropertyPath;
import org.springframework.util.Assert;

/**
 * Utilities for working with {@link PropertyPath}s.
 *
 * @author Jens Schauder
 */
@UtilityClass
public class PropertyPaths {

	public static Class<?> getLeafType(PropertyPath path) {

		if (path.hasNext()) {
			return getLeafType(path.next());
		}
		return path.getType();
	}

	public static PropertyPath extendBy(PropertyPath path, String name) {

		Assert.notNull(path, "Path must not be null.");
		Assert.hasText(name, "Name must not be empty");

		return PropertyPath.from(path.toDotPath() + "." + name, path.getOwningType());
	}
}
