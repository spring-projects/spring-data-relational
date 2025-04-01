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

import java.sql.SQLType;

import org.springframework.data.relational.repository.query.RelationalParameters.RelationalParameter;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

/**
 * Common interface for all objects capable to resolve the {@link SQLType} to be used for a give method parameter.
 *
 * @author Mikhail Polivakha
 */
public interface SqlTypeResolver {

	/**
	 * Resolving the {@link SQLType} from the given {@link RelationalParameter}.
	 *
	 * @param relationalParameter the parameter of the query method
	 * @return {@code null} in case the given {@link SqlTypeResolver} cannot or do not want to determine the
	 * 				 {@link SQLType} of the given parameter
	 */
	@Nullable
	SQLType resolveSqlType(RelationalParameter relationalParameter);

	/**
	 * Resolving the {@link SQLType} from the given {@link RelationalParameter}. The definition of "actual"
	 * type can be looked up in the {@link TypeInformation#getActualType()}.
	 *
	 * @param relationalParameter the parameter of the query method
	 * @return {@code null} in case the given {@link SqlTypeResolver} cannot or do not want to determine the
	 * 	  		 actual {@link SQLType} of the given parameter
	 */
	@Nullable
	SQLType resolveActualSqlType(RelationalParameter relationalParameter);
}
