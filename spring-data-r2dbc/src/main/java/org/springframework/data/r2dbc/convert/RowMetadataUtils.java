/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.r2dbc.convert;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.ReadableMetadata;
import io.r2dbc.spi.RowMetadata;
import org.springframework.data.util.ParsingUtils;
import org.springframework.lang.Nullable;

/**
 * Utility methods for {@link io.r2dbc.spi.RowMetadata}
 *
 * @author Mark Paluch
 * @author kfyty725
 * @since 1.3.7
 */
class RowMetadataUtils {
	/**
	 * Check whether the column {@code name} is contained in {@link RowMetadata}. The check happens case-insensitive.
	 *
	 * @param metadata the metadata object to inspect.
	 * @param name column name.
	 * @return {@code true} if the metadata contains the column {@code name}.
	 */
	public static boolean containsColumn(RowMetadata metadata, String name) {
		return containsColumn(getColumnMetadata(metadata), name);
	}

	/**
	 * Check whether the column {@code name} is contained in {@link RowMetadata}. The check happens case-insensitive.
	 *
	 * @param columns the metadata to inspect.
	 * @param name column name.
	 * @return {@code true} if the metadata contains the column {@code name}.
	 */
	public static boolean containsColumn(Iterable<? extends ReadableMetadata> columns, String name) {
		return findColumnMetadata(columns, name) != null;
	}

	/**
	 * Query matching {@link ColumnMetadata} from name
	 * <p>
	 * This method will check the column name of property and the name of property.
	 * Because when use alias in sql, the name of the property maybe equals to alias in sql, and the column name of property
     * are not equals to alias in sql.
	 *
	 * @param columns the metadata to inspect.
	 * @param name column name.
	 * @return the column metadata.
	 */
	@Nullable
	public static ReadableMetadata findColumnMetadata(Iterable<? extends ReadableMetadata> columns, String name) {
		for (ReadableMetadata columnMetadata : columns) {
			if (name.equalsIgnoreCase(columnMetadata.getName())) {
				return columnMetadata;
			}
			String columnName = ParsingUtils.reconcatenateCamelCase(columnMetadata.getName(), "_");
			if (name.equalsIgnoreCase(columnName)) {
				return columnMetadata;
			}
		}
		return null;
	}

	/**
	 * Return the {@link Iterable} of {@link ColumnMetadata} from {@link RowMetadata}.
	 *
	 * @param metadata the metadata object to inspect.
	 * @return
	 * @since 1.4.1
	 */
	public static Iterable<? extends ColumnMetadata> getColumnMetadata(RowMetadata metadata) {
		return metadata.getColumnMetadatas();
	}
}
