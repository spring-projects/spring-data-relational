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

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.AggregatePath;

/**
 * A mapping between {@link PersistentPropertyPath} and column names of a query. Column names are intentionally
 * represented by {@link String} values, since this is what a {@link java.sql.ResultSet} uses, and since all the query
 * columns should be aliases there is no need for quoting or similar as provided by
 * {@link org.springframework.data.relational.core.sql.SqlIdentifier}.
 *
 * @author Jens Schauder
 * @since 3.2
 */
interface PathToColumnMapping {

	String column(AggregatePath path);

	String keyColumn(AggregatePath path);
}
