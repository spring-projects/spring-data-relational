/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.jdbc.repository.support;

import java.lang.reflect.Method;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;
import org.springframework.data.repository.core.NamedQueries;

/**
 * {@link QueryMethod} implementation that implements a method by executing the
 * query from a {@link Query} annotation on that method. Binds method arguments
 * to named parameters in the SQL statement.
 *
 * @author Jens Schauder
 * @author Kazuki Shimizu
 * @author Moises Cisneros
 * @deprecated Visibility of this class will be reduced to package private.
 */
@Deprecated
public class JdbcQueryMethod extends QueryMethod {

	private final Method method;
	private final NamedQueries namedQueries;

	public JdbcQueryMethod(Method method, RepositoryMetadata metadata, ProjectionFactory factory,
			NamedQueries namedQueries) {

		super(method, metadata, factory);
		this.namedQueries = namedQueries;
		this.method = method;
	}

	/**
	 * Returns the annotated query if it exists.
	 *
	 * @return May be {@code null}.
	 */

	@Nullable
	public String getAnnotatedQuery() {
		String annotatedValue = getQueryValue();
		return StringUtils.hasText(annotatedValue) ? annotatedValue : getNamedQuery();
	}

	/**
	 * Returns the annotated query with key value if it exists.
	 *
	 * @return May be {@code null}.
	 */
	@Nullable
	 String getQueryValue() {
		return getMergedAnnotationAttribute("value");
	}


	/**
	 * Returns the annotated query name.
	 *
	 * @return May be {@code null}.
	 */
	@Nullable
	 String getQueryName() {
		return getMergedAnnotationAttribute("name");
	}
	/**
	 * Returns the annotated query with key name if it exists.
	 *
	 * @return May be {@code null}.
	 */
	@Nullable
	 String getNamedQuery() {
		String annotatedName = getMergedAnnotationAttribute("name");
		return (StringUtils.hasText(annotatedName) && this.namedQueries.hasQuery(annotatedName))
				? this.namedQueries.getQuery(annotatedName)
				: this.namedQueries.getQuery(super.getName());
	}

	/*
	 * Returns the class to be used as {@link
	 * org.springframework.jdbc.core.RowMapper}
	 *
	 * @return May be {@code null}.
	 */
	@Nullable
	Class<? extends RowMapper> getRowMapperClass() {
		return getMergedAnnotationAttribute("rowMapperClass");
	}

	/**
	 * Returns the class to be used as
	 * {@link org.springframework.jdbc.core.ResultSetExtractor}
	 *
	 * @return May be {@code null}.
	 */
	@Nullable
	Class<? extends ResultSetExtractor> getResultSetExtractorClass() {
		return getMergedAnnotationAttribute("resultSetExtractorClass");
	}

	/**
	 * Returns whether the query method is a modifying one.
	 *
	 * @return if it's a modifying query, return {@code true}.
	 */
	@Override
	public boolean isModifyingQuery() {
		return AnnotationUtils.findAnnotation(method, Modifying.class) != null;
	}

	@SuppressWarnings("unchecked")
	@Nullable
	private <T> T getMergedAnnotationAttribute(String attribute) {

		Query queryAnnotation = AnnotatedElementUtils.findMergedAnnotation(method, Query.class);
		return (T) AnnotationUtils.getValue(queryAnnotation, attribute);
	}
}
