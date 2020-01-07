/*
 * Copyright 2019-2020 the original author or authors.
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

import lombok.Value;

import java.sql.JDBCType;

/**
 * Wraps a value with the JDBCType that should be used to pass it as a bind parameter to a
 * {@link java.sql.PreparedStatement}. Register a converter from any type to {@link JdbcValue} in order to control
 * the value and the {@link JDBCType} as which a value should get passed to the JDBC driver.
 *
 * @author Jens Schauder
 * @since 1.1
 */
@Value(staticConstructor = "of")
public class JdbcValue {

	Object value;
	JDBCType jdbcType;
}
