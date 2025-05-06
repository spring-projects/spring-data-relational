/*
 * Copyright 2025 the original author or authors.
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

import java.util.Map;
import java.util.TreeMap;

import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * A builder for {@link AggregatePath.ColumnInfos} instances.
 *
 * @author Jens Schauder
 * @since 4.0
 */
class ColumInfosBuilder {

	private final AggregatePath basePath;
	private final Map<AggregatePath, AggregatePath.ColumnInfo> columnInfoMap = new TreeMap<>();

	/**
	 * Start construction with just the {@literal basePath} which all other paths are build upon.
	 *
	 * @param basePath must not be null.
	 */
	ColumInfosBuilder(AggregatePath basePath) {
		this.basePath = basePath;
	}

	/**
	 * Adds a {@link AggregatePath.ColumnInfo} to the {@link AggregatePath.ColumnInfos} under construction.
	 *
	 * @param path referencing the {@literal ColumnInfo}.
	 * @param name of the column.
	 * @param alias alias for the column.
	 */
	void add(AggregatePath path, SqlIdentifier name, SqlIdentifier alias) {
		add(path, new AggregatePath.ColumnInfo(name, alias));
	}

	/**
	 * Adds a {@link AggregatePath.ColumnInfo} to the {@link AggregatePath.ColumnInfos} under construction.
	 *
	 * @param property referencing the {@literal ColumnInfo}.
	 * @param name of the column.
	 * @param alias alias for the column.
	 */
	void add(RelationalPersistentProperty property, SqlIdentifier name, SqlIdentifier alias) {
		add(basePath.append(property), name, alias);
	}

	/**
	 * Adds a {@link AggregatePath.ColumnInfo} to the {@link AggregatePath.ColumnInfos} under construction.
	 *
	 * @param path the path referencing the {@literal ColumnInfo}
	 * @param columnInfo the {@literal ColumnInfo} added.
	 */
	void add(AggregatePath path, AggregatePath.ColumnInfo columnInfo) {
		columnInfoMap.put(path.subtract(basePath), columnInfo);
	}

	/**
	 * Build the final {@link AggregatePath.ColumnInfos} instance.
	 *
	 * @return a {@literal ColumnInfos} instance containing all the added {@link AggregatePath.ColumnInfo} instances.
	 */
	AggregatePath.ColumnInfos build() {
		return new AggregatePath.ColumnInfos(basePath, columnInfoMap);
	}

}
