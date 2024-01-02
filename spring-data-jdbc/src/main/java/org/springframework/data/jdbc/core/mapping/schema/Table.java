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
package org.springframework.data.jdbc.core.mapping.schema;

import java.util.ArrayList;
import java.util.List;

import java.util.stream.Collectors;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * Models a Table for generating SQL for Schema generation.
 *
 * @author Kurt Niemi
 * @author Evgenii Koba
 * @since 3.2
 */
record Table(@Nullable String schema, String name, List<Column> columns, List<ForeignKey> foreignKeys) {

	public Table(@Nullable String schema, String name) {
		this(schema, name, new ArrayList<>(), new ArrayList<>());
	}

	public Table(String name) {
		this(null, name);
	}

	public List<Column> getIdColumns() {
		return columns().stream().filter(Column::identity).collect(Collectors.toList());
	}

	@Override
	public boolean equals(Object o) {

		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		Table table = (Table) o;
		return ObjectUtils.nullSafeEquals(schema, table.schema) && ObjectUtils.nullSafeEquals(name, table.name);
	}

	@Override
	public int hashCode() {

		int result = 17;

		result += ObjectUtils.nullSafeHashCode(this.schema);
		result += ObjectUtils.nullSafeHashCode(this.name);

		return result;
	}
}
