/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.jdbc.repository.support;

import java.lang.reflect.Method;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.lang.Nullable;

/**
 * {@link QueryMethod} implementation that implements a method by executing the query from a {@link Query} annotation on
 * that method. Binds method arguments to named parameters in the SQL statement.
 *
 * @author Jens Schauder
 * @author Kazuki Shimizu
 */
public class JdbcQueryMethod extends QueryMethod {

	private final Method method;

	public JdbcQueryMethod(Method method, RepositoryMetadata metadata, ProjectionFactory factory) {

		super(method, metadata, factory);

		this.method = method;
	}

	/**
	 * Returns the annotated query if it exists.
	 *
	 * @return May be {@code null}.
	 */
	@Nullable
	public String getAnnotatedQuery() {
		return getMergedAnnotationAttribute("value");
	}

	/**
	 * Returns the class to be used as {@link org.springframework.jdbc.core.RowMapper}
	 *
	 * @return May be {@code null}.
	 */
	@Nullable
	public Class<?> getRowMapperClass() {
		return getMergedAnnotationAttribute("rowMapperClass");
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
