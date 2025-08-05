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

package org.springframework.data.jdbc.core.dialect;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.sql.SQLType;

/**
 * Serves as a hint to the {@link DefaultSqlTypeResolver}, that signals the {@link java.sql.SQLType} to be used.
 * The arguments of this annotation are identical to the methods on {@link java.sql.SQLType} interface, expect for
 * the {@link SQLType#getVendor()}, which is absent, because it typically does not matter as such for the underlying
 * JDBC drivers. The examples of usage, can be found in javadoc of {@link DefaultSqlTypeResolver}.
 *
 * @see DefaultSqlTypeResolver
 * @author Mikhail Polivakha
 */
@Documented
@Target({ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface SqlType {

	/**
	 * Returns the {@code SQLType} name that represents a SQL data type.
	 *
	 * @return The name of this {@code SQLType}.
	 */
	String name();

	/**
	 * Returns the vendor specific type number for the data type.
	 *
	 * @return An Integer representing the vendor specific data type
	 */
	int vendorTypeNumber();
}
