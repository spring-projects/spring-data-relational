/*
 * Copyright 2023 the original author or authors.
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

import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.util.Assert;

/**
 * @author Mark Paluch
 */
public class SingleColumnAggregatePath {

	private final AggregatePath path;

	private SingleColumnAggregatePath(AggregatePath path) {
		this.path = path;
	}

	public static SingleColumnAggregatePath of(AggregatePath path) {

		Assert.notNull(path, "AggregatePath must not be null");

		if (path.isRoot()) {
			throw new IllegalStateException("Root path does not map to a single column");
		}

		if (path.isEmbedded()) {
			throw new IllegalStateException(String.format("Embedded property %s does not map to a single column", path));
		}

		return new SingleColumnAggregatePath(path);
	}

	/**
	 * The name of the column used to reference the id in the parent table.
	 *
	 * @throws IllegalStateException when called on an empty path.
	 */
	public SqlIdentifier getReverseColumnName() {
		return path.getRequiredLeafProperty().getReverseColumnName(path);
	}

}
