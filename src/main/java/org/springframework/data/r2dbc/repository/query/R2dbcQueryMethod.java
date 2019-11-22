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
package org.springframework.data.r2dbc.repository.query;

import static org.springframework.data.repository.util.ClassUtils.*;

import java.lang.reflect.Method;
import java.util.Optional;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.relational.repository.query.RelationalParameters;
import org.springframework.data.relational.repository.query.SimpleRelationalEntityMetadata;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.util.ReactiveWrapperConverters;
import org.springframework.data.repository.util.ReactiveWrappers;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Reactive specific implementation of {@link QueryMethod}.
 *
 * @author Mark Paluch
 */
public class R2dbcQueryMethod extends QueryMethod {

	@SuppressWarnings("rawtypes") //
	private static final ClassTypeInformation<Page> PAGE_TYPE = ClassTypeInformation.from(Page.class);

	@SuppressWarnings("rawtypes") //
	private static final ClassTypeInformation<Slice> SLICE_TYPE = ClassTypeInformation.from(Slice.class);

	private final Method method;
	private final MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext;
	private final Optional<Query> query;
	private final boolean modifying;

	private @Nullable RelationalEntityMetadata<?> metadata;

	/**
	 * Creates a new {@link R2dbcQueryMethod} from the given {@link Method}.
	 *
	 * @param method must not be {@literal null}.
	 * @param metadata must not be {@literal null}.
	 * @param projectionFactory must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 */
	public R2dbcQueryMethod(Method method, RepositoryMetadata metadata, ProjectionFactory projectionFactory,
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext) {

		super(method, metadata, projectionFactory);

		Assert.notNull(mappingContext, "MappingContext must not be null!");

		this.mappingContext = mappingContext;

		if (hasParameterOfType(method, Pageable.class)) {

			TypeInformation<?> returnType = ClassTypeInformation.fromReturnTypeOf(method);

			boolean multiWrapper = ReactiveWrappers.isMultiValueType(returnType.getType());
			boolean singleWrapperWithWrappedPageableResult = ReactiveWrappers.isSingleValueType(returnType.getType())
					&& (PAGE_TYPE.isAssignableFrom(returnType.getRequiredComponentType())
							|| SLICE_TYPE.isAssignableFrom(returnType.getRequiredComponentType()));

			if (singleWrapperWithWrappedPageableResult) {
				throw new InvalidDataAccessApiUsageException(
						String.format("'%s.%s' must not use sliced or paged execution. Please use Flux.buffer(size, skip).",
								ClassUtils.getShortName(method.getDeclaringClass()), method.getName()));
			}

			if (!multiWrapper) {
				throw new IllegalStateException(String.format(
						"Method has to use a either multi-item reactive wrapper return type or a wrapped Page/Slice type. Offending method: %s",
						method.toString()));
			}

			if (hasParameterOfType(method, Sort.class)) {
				throw new IllegalStateException(String.format("Method must not have Pageable *and* Sort parameter. "
						+ "Use sorting capabilities on Pageble instead! Offending method: %s", method.toString()));
			}
		}

		this.method = method;
		this.query = Optional.ofNullable(AnnotatedElementUtils.findMergedAnnotation(method, Query.class));
		this.modifying = AnnotatedElementUtils.hasAnnotation(method, Modifying.class);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryMethod#createParameters(java.lang.reflect.Method)
	 */
	@Override
	protected RelationalParameters createParameters(Method method) {
		return new RelationalParameters(method);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryMethod#isCollectionQuery()
	 */
	@Override
	public boolean isCollectionQuery() {
		return !(isPageQuery() || isSliceQuery()) && ReactiveWrappers.isMultiValueType(method.getReturnType());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryMethod#isModifyingQuery()
	 */
	@Override
	public boolean isModifyingQuery() {
		return modifying;
	}

	/*
	 * All reactive query methods are streaming queries.
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryMethod#isStreamQuery()
	 */
	@Override
	public boolean isStreamQuery() {
		return true;
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
	 * Check if the given {@link org.springframework.data.repository.query.QueryMethod} receives a reactive parameter
	 * wrapper as one of its parameters.
	 *
	 * @return {@literal true} if the given {@link org.springframework.data.repository.query.QueryMethod} receives a
	 *         reactive parameter wrapper as one of its parameters.
	 */
	public boolean hasReactiveWrapperParameter() {

		for (Parameter parameter : getParameters()) {
			if (ReactiveWrapperConverters.supports(parameter.getType())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns the required query string declared in a {@link Query} annotation or throws {@link IllegalStateException} if
	 * neither the annotation found nor the attribute was specified.
	 *
	 * @return the query string.
	 * @throws IllegalStateException in case query method has no annotated query.
	 */
	public String getRequiredAnnotatedQuery() {
		return this.query.map(Query::value)
				.orElseThrow(() -> new IllegalStateException("Query method " + this + " has no annotated query"));
	}

	/**
	 * Returns the {@link Query} annotation that is applied to the method or {@literal null} if none available.
	 *
	 * @return the optional query annotation.
	 */
	Optional<Query> getQueryAnnotation() {
		return this.query;
	}

	/**
	 * @return {@literal true} if the {@link Method} is annotated with {@link Query}.
	 */
	public boolean hasAnnotatedQuery() {
		return getQueryAnnotation().isPresent();
	}
}
