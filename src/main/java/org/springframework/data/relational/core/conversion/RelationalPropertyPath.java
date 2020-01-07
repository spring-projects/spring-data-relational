/*
 * Copyright 2017-2020 the original author or authors.
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
package org.springframework.data.relational.core.conversion;

import org.springframework.data.mapping.PropertyPath;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * A replacement for {@link org.springframework.data.mapping.PropertyPath} as long as it doesn't support objects with
 * empty path. See https://jira.spring.io/browse/DATACMNS-1204.
 *
 * @author Jens Schauder
 */
public class RelationalPropertyPath {

	private final PropertyPath path;
	private final Class<?> rootType;

	RelationalPropertyPath(PropertyPath path) {

		Assert.notNull(path, "path must not be null if rootType is not set");

		this.path = path;
		this.rootType = null;
	}

	private RelationalPropertyPath(Class<?> type) {

		Assert.notNull(type, "type must not be null if path is not set");

		this.path = null;
		this.rootType = type;
	}

	public static RelationalPropertyPath from(String source, Class<?> type) {

		if (StringUtils.isEmpty(source)) {
			return new RelationalPropertyPath(type);
		} else {
			return new RelationalPropertyPath(PropertyPath.from(source, type));
		}
	}

	public RelationalPropertyPath nested(String name) {

		return path == null ? //
				new RelationalPropertyPath(PropertyPath.from(name, rootType)) //
				: new RelationalPropertyPath(path.nested(name));
	}

	public PropertyPath getPath() {
		return path;
	}

	public String toDotPath() {
		return path == null ? "" : path.toDotPath();
	}
}
