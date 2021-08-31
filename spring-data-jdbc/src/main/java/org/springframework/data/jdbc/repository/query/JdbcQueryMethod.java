/*
 * Copyright 2020-2021 the original author or authors.
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
package org.springframework.data.jdbc.repository.query;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.relational.repository.query.RelationalParameters;
import org.springframework.data.relational.repository.query.SimpleRelationalEntityMetadata;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.ConcurrentReferenceHashMap;
import org.springframework.util.StringUtils;

/**
 * {@link QueryMethod} implementation that implements a method by executing the query from a {@link Query} annotation on
 * that method. Binds method arguments to named parameters in the SQL statement.
 *
 * @author Jens Schauder
 * @author Kazuki Shimizu
 * @author Moises Cisneros
 * @author Hebert Coelho
 */
public class JdbcQueryMethod extends QueryMethod {

	private final Method method;
	private final MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext;
	private final Map<Class<? extends Annotation>, Optional<Annotation>> annotationCache;
	private final NamedQueries namedQueries;
	private @Nullable RelationalEntityMetadata<?> metadata;

	// TODO: Remove NamedQueries and put it into JdbcQueryLookupStrategy
	public JdbcQueryMethod(Method method, RepositoryMetadata metadata, ProjectionFactory factory,
			NamedQueries namedQueries,
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext) {

		super(method, metadata, factory);
		this.namedQueries = namedQueries;
		this.method = method;
		this.mappingContext = mappingContext;
		this.annotationCache = new ConcurrentReferenceHashMap<>();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryMethod#createParameters(java.lang.reflect.Method)
	 */
	@Override
	protected RelationalParameters createParameters(Method method) {
		return new RelationalParameters(method);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryMethod#getEntityInformation()
	 */
	@Override
	@SuppressWarnings("unchecked")
	public RelationalEntityMetadata<?> getEntityInformation() {

		if (metadata == null) {

			Class<?> returnedObjectType = getReturnedObjectType();
			Class<?> domainClass = getDomainClass();

			if (ClassUtils.isPrimitiveOrWrapper(returnedObjectType)) {

				this.metadata = new SimpleRelationalEntityMetadata<>((Class<Object>) domainClass,
						mappingContext.getRequiredPersistentEntity(domainClass));

			} else {

				RelationalPersistentEntity<?> returnedEntity = mappingContext.getPersistentEntity(returnedObjectType);
				RelationalPersistentEntity<?> managedEntity = mappingContext.getRequiredPersistentEntity(domainClass);
				returnedEntity = returnedEntity == null || returnedEntity.getType().isInterface() ? managedEntity
						: returnedEntity;
				RelationalPersistentEntity<?> tableEntity = domainClass.isAssignableFrom(returnedObjectType) ? returnedEntity
						: managedEntity;

				this.metadata = new SimpleRelationalEntityMetadata<>((Class<Object>) returnedEntity.getType(), tableEntity);
			}
		}

		return this.metadata;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryMethod#getParameters()
	 */
	@Override
	public RelationalParameters getParameters() {
		return (RelationalParameters) super.getParameters();
	}

	/**
	 * Returns the annotated query if it exists.
	 *
	 * @return May be {@code null}.
	 */
	@Nullable
	String getDeclaredQuery() {

		String annotatedValue = getQueryValue();
		return StringUtils.hasText(annotatedValue) ? annotatedValue : getNamedQuery();
	}

	/**
	 * Returns the annotated query if it exists.
	 *
	 * @return May be {@code null}.
	 */
	@Nullable
	private String getQueryValue() {
		return getMergedAnnotationAttribute("value");
	}

	/**
	 * Returns the named query for this method if it exists.
	 *
	 * @return May be {@code null}.
	 */
	@Nullable
	private String getNamedQuery() {

		String name = getNamedQueryName();
		return this.namedQueries.hasQuery(name) ? this.namedQueries.getQuery(name) : null;
	}

	@Override
	public String getNamedQueryName() {

		String annotatedName = getMergedAnnotationAttribute("name");

		return StringUtils.hasText(annotatedName) ? annotatedName : super.getNamedQueryName();
	}

	/**
	 * Returns the class to be used as {@link org.springframework.jdbc.core.RowMapper}
	 *
	 * @return May be {@code null}.
	 */
	@Nullable
	Class<? extends RowMapper> getRowMapperClass() {
		return getMergedAnnotationAttribute("rowMapperClass");
	}

	/**
	 * Returns the name of the bean to be used as {@link org.springframework.jdbc.core.RowMapper}
	 *
	 * @return May be {@code null}.
	 */
	@Nullable
	String getRowMapperRef() {
		return getMergedAnnotationAttribute("rowMapperRef");
	}

	/**
	 * Returns the class to be used as {@link org.springframework.jdbc.core.ResultSetExtractor}
	 *
	 * @return May be {@code null}.
	 */
	@Nullable
	Class<? extends ResultSetExtractor> getResultSetExtractorClass() {
		return getMergedAnnotationAttribute("resultSetExtractorClass");
	}

	/**
	 * Returns the bean name to be used as {@link org.springframework.jdbc.core.ResultSetExtractor}
	 *
	 * @return May be {@code null}.
	 */
	@Nullable
	String getResultSetExtractorRef() {
		return getMergedAnnotationAttribute("resultSetExtractorRef");
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

	/**
	 * Returns whether the method has an annotated query.
	 */
	public boolean hasAnnotatedQuery() {
		return findAnnotatedQuery().isPresent();
	}

	private Optional<String> findAnnotatedQuery() {

		return lookupQueryAnnotation() //
				.map(Query::value) //
				.filter(StringUtils::hasText);
	}

	Optional<Query> lookupQueryAnnotation() {
		return doFindAnnotation(Query.class);
	}

	@SuppressWarnings("unchecked")
	private <A extends Annotation> Optional<A> doFindAnnotation(Class<A> annotationType) {

		return (Optional<A>) this.annotationCache.computeIfAbsent(annotationType,
				it -> Optional.ofNullable(AnnotatedElementUtils.findMergedAnnotation(method, it)));
	}

}
