/*
 * Copyright 2019-present the original author or authors.
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
package org.springframework.data.relational.core.dialect;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.springframework.data.relational.core.sql.Functions;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SQL;
import org.springframework.data.relational.core.sql.SimpleFunction;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.render.SelectRenderContext;
import org.springframework.data.relational.core.sql.render.StandardSqlUpsertRenderContext;
import org.springframework.data.relational.core.sql.render.UpsertRenderContext;
import org.springframework.util.ClassUtils;

/**
 * Represents a dialect for a particular database.
 * <p>
 * Note that not all features are supported by all vendors. Dialect implementations provide feature flags and objects
 * that describe how a database runs certain commands or handles types such as array. Methods for unsupported
 * functionality may throw {@link UnsupportedOperationException}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Myeonghyeon Lee
 * @author Christoph Strobl
 * @author Mikhail Polivakha
 * @author Chirag Tailor
 * @since 1.1
 */
public interface Dialect {

	/**
	 * Returns the name of the dialect.
	 *
	 * @return the name of the dialect.
	 * @since 4.x
	 */
	default String getName() {
		return ClassUtils.getShortName(getClass());
	}

	/**
	 * Returns the {@link IdentifierProcessing handling of table- and column names (identifiers)} used for processing
	 * {@link SqlIdentifier} when converting them to SQL snippets or parameter names.
	 *
	 * @return the {@link IdentifierProcessing}. Guaranteed to be not {@literal null}.
	 * @since 2.0
	 */
	default IdentifierProcessing getIdentifierProcessing() {
		return IdentifierProcessing.ANSI;
	}

	/**
	 * Returns the {@link IdGeneration} used for generating identifiers.
	 *
	 * @return the {@link IdGeneration} used for generating identifiers.
	 */
	default IdGeneration getIdGeneration() {
		return IdGeneration.DEFAULT;
	}

	/**
	 * Return the {@link LockClause} used by this dialect.
	 *
	 * @return the {@link LockClause} used by this dialect.
	 */
	LockClause lock();

	/**
	 * Returns the array support object that describes how array-typed columns should be handled by this dialect.
	 *
	 * @return the array support object that describes how array-typed columns should be handled by this dialect.
	 */
	default ArrayColumns getArraySupport() {
		return ArrayColumns.unsupported();
	}

	/**
	 * Return a collection of converters for this dialect.
	 *
	 * @return a collection of converters for this dialect.
	 */
	default Collection<Object> getConverters() {
		return Collections.emptySet();
	}

	/**
	 * Return the {@link Set} of types considered store native types that can be handeled by the driver.
	 *
	 * @return never {@literal null}.
	 * @since 2.3
	 */
	default Set<Class<?>> simpleTypes() {
		return Collections.emptySet();
	}

	/**
	 * Provide a SQL function that is suitable for implementing an exists-query. The default is `COUNT(1)`, but for some
	 * databases a {@code LEAST(COUNT(1), 1)} might be required, which doesn't get accepted by other databases.
	 *
	 * @since 3.0
	 */
	default SimpleFunction getExistsFunction() {
		return Functions.count(SQL.literalOf(1));
	}

	/**
	 * Return whether the dialect supports single query loading.
	 *
	 * @return {@literal true} if the dialect supports single query loading; {@literal false} otherwise.
	 */
	default boolean supportsSingleQueryLoading() {
		return true;
	}

	/**
	 * Returns the {@link Escaper} used for {@code LIKE} value escaping.
	 *
	 * @return the {@link Escaper} used for {@code LIKE} value escaping.
	 * @since 2.0
	 */
	default Escaper getLikeEscaper() {
		return Escaper.DEFAULT;
	}

	/**
	 * Return the {@link LimitClause} used by this dialect.
	 *
	 * @return the {@link LimitClause} used by this dialect.
	 */
	LimitClause limit();

	/**
	 * Return the {@link OrderByNullPrecedence} used by this dialect.
	 *
	 * @return the {@link OrderByNullPrecedence} used by this dialect.
	 * @since 2.4
	 */
	default OrderByNullPrecedence orderByNullHandling() {
		return OrderByNullPrecedence.SQL_STANDARD;
	}

	/**
	 * Obtain the {@link SelectRenderContext}.
	 *
	 * @return the {@link SelectRenderContext}.
	 */
	SelectRenderContext getSelectContext();

	/**
	 * @return an appropriate {@link InsertRenderContext} for that specific dialect. for most of the Dialects the default
	 *         implementation will be valid, but, for example, in case of {@link SqlServerDialect} it is not.
	 * @since 2.4
	 */
	default InsertRenderContext getInsertRenderContext() {
		return InsertRenderContexts.DEFAULT;
	}

	/**
	 * Returns an {@link UpsertRenderContext} for single-statement upsert.
	 *
	 * @return the upsert render context. {@link StandardSqlUpsertRenderContext} by default.
	 * @throws UnsupportedOperationException if the dialect does not support single-statement upsert.
	 * @since 4.x
	 */
	default UpsertRenderContext getUpsertRenderContext() {
		return StandardSqlUpsertRenderContext.INSTANCE;
	}

}
