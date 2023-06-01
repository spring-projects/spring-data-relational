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
package org.springframework.data.relational.core.mapping.schemasqlgeneration;

import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * Interface for mapping a {@link RelationalPersistentProperty} to a Database type.
 *
 * @author Kurt Niemi
 * @since 3.2
 */
public interface SqlTypeMapping {

	/**
	 * Determine a column type for a persistent property.
	 *
	 * @param property the property for which the type should be determined.
	 * @return the SQL type to use, such as {@code VARCHAR} or {@code NUMERIC}.
	 */
	String getColumnType(RelationalPersistentProperty property);
}
