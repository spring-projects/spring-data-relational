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

import org.springframework.data.relational.repository.query.RelationalParameters;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link SqlTypeResolver}. Capable to resolve the {@link SqlType} annotation
 * on the {@link java.lang.annotation.ElementType#PARAMETER method parameters}, like this:
 * <p>
 * <pre class="code">
 * List<User> findByAge(&#64;SqlType(name = "TINYINT", vendorTypeNumber = Types.TINYINT) byte age);
 * </pre>
 *
 * Qualification of the actual {@link SQLType} (the sql type of the component), then the following needs to be done:
 * <pre class="code">
 * List<User> findByAgeIn(&#64;SqlType(name = "TINYINT", vendorTypeNumber = Types.TINYINT) Integer[] age);
 * </pre>
 *
 * @author Mikhail Polivakha
 */
public class DefaultSqlTypeResolver implements SqlTypeResolver {

	public static DefaultSqlTypeResolver INSTANCE = new DefaultSqlTypeResolver();

	@Override
	@Nullable
	public SQLType resolveSqlType(RelationalParameters.RelationalParameter relationalParameter) {
		return resolveInternally(relationalParameter);
	}

	@Override
	@Nullable
	public SQLType resolveActualSqlType(RelationalParameters.RelationalParameter relationalParameter) {
		return resolveInternally(relationalParameter);
	}

	private static AnnotationBasedSqlType resolveInternally(
			RelationalParameters.RelationalParameter relationalParameter) {
		SqlType parameterAnnotation = relationalParameter.getMethodParameter().getParameterAnnotation(SqlType.class);

		if (parameterAnnotation != null) {
			return new AnnotationBasedSqlType(parameterAnnotation);
		} else {
			return null;
		}
	}

	/**
	 * {@link SQLType} determined from the {@link SqlType} annotation.
	 *
	 * @author Mikhail Polivakha
	 */
	protected static class AnnotationBasedSqlType implements SQLType {

		private final SqlType sqlType;

		public AnnotationBasedSqlType(SqlType sqlType) {
			Assert.notNull(sqlType, "sqlType must not be null");

			this.sqlType = sqlType;
		}

		@Override
		public String getName() {
			return sqlType.name();
		}

		@Override
		public String getVendor() {
			return "Spring Data JDBC";
		}

		@Override
		public Integer getVendorTypeNumber() {
			return sqlType.vendorTypeNumber();
		}
	}
}
