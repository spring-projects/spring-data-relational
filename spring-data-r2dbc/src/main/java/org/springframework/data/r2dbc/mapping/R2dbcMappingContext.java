/*
 * Copyright 2019-2025 the original author or authors.
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
package org.springframework.data.r2dbc.mapping;

import org.springframework.core.KotlinDetector;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.util.KotlinReflectionUtils;

/**
 * R2DBC-specific extension to {@link RelationalMappingContext}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public class R2dbcMappingContext extends RelationalMappingContext {

	/**
	 * Create a new {@link R2dbcMappingContext}.
	 */
	public R2dbcMappingContext() {}

	/**
	 * Create a new {@link R2dbcMappingContext} using the given {@link NamingStrategy}.
	 *
	 * @param namingStrategy must not be {@literal null}.
	 */
	public R2dbcMappingContext(NamingStrategy namingStrategy) {
		super(namingStrategy);
	}

	/**
	 * Create a new {@code R2dbcMappingContext} using {@link #setForceQuote(boolean) plain identifiers}. Plain
	 * {@link org.springframework.data.relational.core.sql.SqlIdentifier identifiers} (i.e. table and column names) are
	 * typically not case-sensitive (case-sensitivity can be still enforced by specific database configurations).
	 *
	 * @return a new {@code R2dbcMappingContext} using plain identifiers.
	 * @since 4.0
	 */
	public static R2dbcMappingContext forPlainIdentifiers() {
		R2dbcMappingContext context = forQuotedIdentifiers();
		context.setForceQuote(false);
		return context;
	}

	/**
	 * Create a new {@code R2dbcMappingContext} using {@link #setForceQuote(boolean) plain identifiers} and the given
	 * {@link NamingStrategy}. Plain {@link org.springframework.data.relational.core.sql.SqlIdentifier identifiers} (i.e.
	 * table and column names) are typically not case-sensitive (case-sensitivity can be still enforced by specific
	 * database configurations).
	 *
	 * @param namingStrategy must not be {@literal null}.
	 * @return a new {@code R2dbcMappingContext} using plain identifiers.
	 * @since 4.0
	 */
	public static R2dbcMappingContext forPlainIdentifiers(NamingStrategy namingStrategy) {
		R2dbcMappingContext context = forQuotedIdentifiers(namingStrategy);
		context.setForceQuote(false);
		return context;
	}

	/**
	 * Create a new {@code R2dbcMappingContext} using {@link #setForceQuote(boolean) quoted identifiers} (default
	 * behavior). Quoted {@link org.springframework.data.relational.core.sql.SqlIdentifier identifiers} (i.e. table and
	 * column names) are typically case-sensitive.
	 *
	 * @return a new {@code R2dbcMappingContext} using quoted identifiers.
	 * @since 4.0
	 */
	public static R2dbcMappingContext forQuotedIdentifiers() {
		return new R2dbcMappingContext();
	}

	/**
	 * Create a new {@code R2dbcMappingContext} using {@link #setForceQuote(boolean) quoted identifiers} (default
	 * behavior) and the given {@link NamingStrategy}. Quoted
	 * {@link org.springframework.data.relational.core.sql.SqlIdentifier identifiers} (i.e. table and column names) are
	 * typically case-sensitive.
	 *
	 * @param namingStrategy must not be {@literal null}.
	 * @return a new {@code R2dbcMappingContext} using quoted identifiers.
	 * @since 4.0
	 */
	public static R2dbcMappingContext forQuotedIdentifiers(NamingStrategy namingStrategy) {
		return new R2dbcMappingContext(namingStrategy);
	}

	@Override
	protected boolean shouldCreatePersistentEntityFor(TypeInformation<?> type) {

		if (R2dbcSimpleTypeHolder.HOLDER.isSimpleType(type.getType())) {
			return false;
		}

		return !KotlinDetector.isKotlinType(type.getType()) || KotlinReflectionUtils.isSupportedKotlinClass(type.getType());
	}
}
