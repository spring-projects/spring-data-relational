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

/**
 * Utility methods for {@link io.r2dbc.spi.RowMetadata}
 *
 * @author Mark Paluch
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

		for (ReadableMetadata columnMetadata : columns) {
			if (name.equalsIgnoreCase(columnMetadata.getName())) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Return the {@link Iterable} of {@link ColumnMetadata} from {@link RowMetadata}.
	 *
	 * @param metadata the metadata object to inspect.
	 * @return
	 * @since 1.4.1
	 */
	@SuppressWarnings("unchecked")
	public static Iterable<? extends ColumnMetadata> getColumnMetadata(RowMetadata metadata) {
		return metadata.getColumnMetadatas();
	}
}
