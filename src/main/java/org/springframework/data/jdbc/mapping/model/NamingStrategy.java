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
package org.springframework.data.jdbc.mapping.model;

/**
 * @author Greg Turnquist
 */
public interface NamingStrategy {

	String getSchema();

	String getTableName(Class<?> type);

	String getColumnName(JdbcPersistentProperty property);

	default String getQualifiedTableName(Class<?> type) {
		return this.getSchema() + (this.getSchema().equals("") ? "" : ".") + this.getTableName(type);
	}

	/**
	 * For a reference A -> B this is the name in the table for B which references A.
	 *
	 * @return a column name.
	 */
	String getReverseColumnName(JdbcPersistentProperty property);

	/**
	 * For a map valued reference A -> Map&gt;X,B&lt; this is the name of the column in the tabel for B holding the key of the map.
	 * @return
	 */
	String getKeyColumn(JdbcPersistentProperty property);

}
