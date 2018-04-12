/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.jdbc.core.conversion;

import org.springframework.data.mapping.PropertyPath;
import org.springframework.util.StringUtils;

/**
 * A replacement for {@link org.springframework.data.mapping.PropertyPath} as long as it doesn't support objects with
 * empty path.
 *
 * See https://jira.spring.io/browse/DATACMNS-1204.
 *
 * @author Jens Schauder
 * @since 1.0
 */
public class JdbcPropertyPath {

	private final PropertyPath path;
	private final Class<?> rootType;

	JdbcPropertyPath(PropertyPath path) {

		this.path = path;
		this.rootType = null;
	}

	private JdbcPropertyPath(Class<?> type) {

		this.path = null;
		this.rootType = type;
	}

	public static JdbcPropertyPath from(String source, Class<?> type) {

		if (StringUtils.isEmpty(source)) {
			return new JdbcPropertyPath(type);
		} else {
			return new JdbcPropertyPath(PropertyPath.from(source, type));
		}
	}

	public JdbcPropertyPath nested(String name) {
		return path == null ? new JdbcPropertyPath(PropertyPath.from(name, rootType)) : new JdbcPropertyPath(path.nested(name));
	}

	public PropertyPath getPath() {
		return path;
	}

	public String toDotPath() {
		return path == null ? "" : path.toDotPath();
	}
}
