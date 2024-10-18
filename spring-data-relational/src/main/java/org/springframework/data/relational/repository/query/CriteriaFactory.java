/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.relational.repository.query;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Simple factory to contain logic to create {@link Criteria}s from {@link Part}s.
 *
 * @author Roman Chigvintsev
 * @author Mark Paluch
 */
class CriteriaFactory {

	private final ParameterMetadataProvider parameterMetadataProvider;

	/**
	 * Creates new instance of this class with the given {@link ParameterMetadataProvider}.
	 *
	 * @param parameterMetadataProvider parameter metadata provider (must not be {@literal null})
	 */
	public CriteriaFactory(ParameterMetadataProvider parameterMetadataProvider) {
		Assert.notNull(parameterMetadataProvider, "Parameter metadata provider must not be null");
		this.parameterMetadataProvider = parameterMetadataProvider;
	}

	/**
	 * Creates {@link Criteria} for the given {@link Part}.
	 *
	 * @param part method name part (must not be {@literal null})
	 * @return {@link Criteria} instance
	 * @throws IllegalArgumentException if part type is not supported
	 */
	public Criteria createCriteria(Part part) {
		Part.Type type = part.getType();

		String propertyName = part.getProperty().toDotPath();
		Class<?> propertyType = part.getProperty().getType();

		Criteria.CriteriaStep criteriaStep = Criteria.where(propertyName);

		if (type == Part.Type.IS_NULL || type == Part.Type.IS_NOT_NULL) {
			return part.getType() == Part.Type.IS_NULL ? criteriaStep.isNull() : criteriaStep.isNotNull();
		}

		if (type == Part.Type.TRUE || type == Part.Type.FALSE) {
			return part.getType() == Part.Type.TRUE ? criteriaStep.isTrue() : criteriaStep.isFalse();
		}

		switch (type) {
			case BETWEEN: {
				ParameterMetadata geParamMetadata = parameterMetadataProvider.next(part);
				ParameterMetadata leParamMetadata = parameterMetadataProvider.next(part);
				return criteriaStep.between(geParamMetadata.getValue(), leParamMetadata.getValue());
			}
			case AFTER:
			case GREATER_THAN: {
				ParameterMetadata paramMetadata = parameterMetadataProvider.next(part);
				return criteriaStep.greaterThan(paramMetadata.getValue());
			}
			case GREATER_THAN_EQUAL: {
				ParameterMetadata paramMetadata = parameterMetadataProvider.next(part);
				return criteriaStep.greaterThanOrEquals(paramMetadata.getValue());
			}
			case BEFORE:
			case LESS_THAN: {
				ParameterMetadata paramMetadata = parameterMetadataProvider.next(part);
				return criteriaStep.lessThan(paramMetadata.getValue());
			}
			case LESS_THAN_EQUAL: {
				ParameterMetadata paramMetadata = parameterMetadataProvider.next(part);
				return criteriaStep.lessThanOrEquals(paramMetadata.getValue());
			}
			case IN:
			case NOT_IN: {
				ParameterMetadata paramMetadata = parameterMetadataProvider.next(part);
				Criteria criteria = part.getType() == Part.Type.IN ? criteriaStep.in(asCollection(paramMetadata.getValue()))
						: criteriaStep.notIn(asCollection(paramMetadata.getValue()));
				return criteria.ignoreCase(shouldIgnoreCase(part) && checkCanUpperCase(part, part.getProperty().getType()));
			}
			case STARTING_WITH:
			case ENDING_WITH:
			case CONTAINING:
			case NOT_CONTAINING:
			case LIKE:
			case NOT_LIKE: {
				ParameterMetadata paramMetadata = parameterMetadataProvider.next(part);
				Criteria criteria = part.getType() == Part.Type.NOT_LIKE || part.getType() == Part.Type.NOT_CONTAINING
						? criteriaStep.notLike(paramMetadata.getValue())
						: criteriaStep.like(paramMetadata.getValue());
				return criteria
						.ignoreCase(shouldIgnoreCase(part) && checkCanUpperCase(part, propertyType, paramMetadata.getType()));
			}
			case SIMPLE_PROPERTY: {
				ParameterMetadata paramMetadata = parameterMetadataProvider.next(part);
				if (paramMetadata.getValue() == null) {
					return criteriaStep.isNull();
				}
				return criteriaStep.is(paramMetadata.getValue())
						.ignoreCase(shouldIgnoreCase(part) && checkCanUpperCase(part, propertyType, paramMetadata.getType()));
			}
			case NEGATING_SIMPLE_PROPERTY: {
				ParameterMetadata paramMetadata = parameterMetadataProvider.next(part);
				return criteriaStep.not(paramMetadata.getValue())
						.ignoreCase(shouldIgnoreCase(part) && checkCanUpperCase(part, propertyType, paramMetadata.getType()));
			}
			default:
				throw new IllegalArgumentException("Unsupported keyword " + type);
		}
	}

	/**
	 * Checks whether comparison should be done in case-insensitive way.
	 *
	 * @param part method name part (must not be {@literal null})
	 * @return {@literal true} if comparison should be done in case-insensitive way
	 */
	private boolean shouldIgnoreCase(Part part) {
		return part.shouldIgnoreCase() == Part.IgnoreCaseType.ALWAYS
				|| part.shouldIgnoreCase() == Part.IgnoreCaseType.WHEN_POSSIBLE;
	}

	/**
	 * Checks whether "upper-case" conversion can be applied to the given {@link Expression}s in case the underlying
	 * {@link Part} requires ignoring case.
	 *
	 * @param part method name part (must not be {@literal null})
	 * @param expressionTypes types of the given expressions (must not be {@literal null} or empty)
	 * @throws IllegalStateException if {@link Part} requires ignoring case but "upper-case" conversion cannot be applied
	 *           to at least one of the given {@link Expression}s
	 */
	private boolean checkCanUpperCase(Part part, Class<?>... expressionTypes) {
		Assert.notEmpty(expressionTypes, "Expression types must not be null or empty");
		boolean strict = part.shouldIgnoreCase() == Part.IgnoreCaseType.ALWAYS;
		for (Class<?> expressionType : expressionTypes) {
			if (!canUpperCase(expressionType)) {
				if (strict) {
					throw new IllegalStateException("Unable to ignore case of " + expressionType.getName()
							+ " type, the property '" + part.getProperty().getSegment() + "' must reference a string");
				}
				return false;
			}
		}
		return true;
	}

	private boolean canUpperCase(Class<?> expressionType) {
		return expressionType == String.class;
	}

	@SuppressWarnings("unchecked")
	private static Collection<Object> asCollection(Object value) {

		if (value instanceof Collection) {
			return (Collection<Object>) value;
		}

		if (value.getClass().isArray()) {
			return Arrays.asList(ObjectUtils.toObjectArray(value));
		}

		return Collections.singletonList(value);
	}
}
