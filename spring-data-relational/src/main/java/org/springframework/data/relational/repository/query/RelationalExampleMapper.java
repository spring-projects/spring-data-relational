/*
 * Copyright 2021-2025 the original author or authors.
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

import static org.springframework.data.domain.ExampleMatcher.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.support.ExampleMatcherAccessor;
import org.springframework.util.Assert;

/**
 * Transform an {@link Example} into a {@link Query}.
 *
 * @since 2.2
 * @author Greg Turnquist
 * @author Jens Schauder
 * @author Mikhail Polivakha
 */
public class RelationalExampleMapper {

	private final MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext;

	public RelationalExampleMapper(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext) {
		this.mappingContext = mappingContext;
	}

	/**
	 * Use the {@link Example} to extract a {@link Query}.
	 *
	 * @param example
	 * @return query
	 */
	public <T> Query getMappedExample(Example<T> example) {
		return getMappedExample(example, mappingContext.getRequiredPersistentEntity(example.getProbeType()));
	}

	/**
	 * Transform each property of the {@link Example}'s probe into a {@link Criteria} and assemble them into a
	 * {@link Query}.
	 *
	 * @param example
	 * @param persistentEntity
	 * @return query
	 */
	private <T> Query getMappedExample(Example<T> example, RelationalPersistentEntity<?> persistentEntity) {

		Assert.notNull(example, "Example must not be null");
		Assert.notNull(persistentEntity, "RelationalPersistentEntity must not be null");

		PersistentPropertyAccessor<T> probePropertyAccessor = persistentEntity.getPropertyAccessor(example.getProbe());
		ExampleMatcherAccessor matcherAccessor = new ExampleMatcherAccessor(example.getMatcher());

		final List<Criteria> criteriaBasedOnProperties = buildCriteria( //
				persistentEntity, //
				matcherAccessor, //
				probePropertyAccessor //
		);

		// Criteria, assemble!
		Criteria criteria = Criteria.empty();

		for (Criteria propertyCriteria : criteriaBasedOnProperties) {

			if (example.getMatcher().isAllMatching()) {
				criteria = criteria.and(propertyCriteria);
			} else {
				criteria = criteria.or(propertyCriteria);
			}
		}

		return Query.query(criteria);
	}

	private <T> List<Criteria> buildCriteria( //
			RelationalPersistentEntity<?> persistentEntity, //
			ExampleMatcherAccessor matcherAccessor, //
			PersistentPropertyAccessor<T> probePropertyAccessor //
	) {
		final List<Criteria> criteriaBasedOnProperties = new ArrayList<>();

		persistentEntity.doWithProperties((PropertyHandler<RelationalPersistentProperty>) property -> {
			potentiallyEnrichCriteria(
					null,
					matcherAccessor,  //
					probePropertyAccessor, //
					property, //
					criteriaBasedOnProperties //
			);
		});
		return criteriaBasedOnProperties;
	}

	/**
	 * Analyzes the incoming {@code property} and potentially enriches the {@code criteriaBasedOnProperties} with the new
	 * {@link Criteria} for this property.
	 * <p>
	 * This algorithm is recursive in order to take the embedded properties into account. The caller can expect that the result
	 * of this method call is fully processed subtree of an aggreagte where the passed {@code property} serves as the root.
	 *
	 * @param propertyPath the {@link PropertyPath} of the passed {@code property}.
	 * @param matcherAccessor the accessor for the original {@link ExampleMatcher}.
	 * @param entityPropertiesAccessor the accessor for the properties of the current entity that holds the given {@code property}
	 * @param property the property under analysis
	 * @param criteriaBasedOnProperties the {@link List} of criteria objects that potentially gets enriched as a
	 *                                  result of the incoming {@code property} processing
	 */
	private <T> void potentiallyEnrichCriteria(
			@Nullable PropertyPath propertyPath,
			ExampleMatcherAccessor matcherAccessor, //
			PersistentPropertyAccessor<T> entityPropertiesAccessor, //
			RelationalPersistentProperty property, //
			List<Criteria> criteriaBasedOnProperties //
	) {

		// QBE do not support queries on Child aggregates yet
		if (property.isCollectionLike() || property.isMap()) {
			return;
		}

		PropertyPath currentPropertyPath = resolveCurrentPropertyPath(propertyPath, property);
		String currentPropertyDotPath = currentPropertyPath.toDotPath();

		if (matcherAccessor.isIgnoredPath(currentPropertyDotPath)) {
			return;
		}

		Object actualPropertyValue = entityPropertiesAccessor.getProperty(property);

		if (property.isEmbedded() && actualPropertyValue != null) {
			processEmbeddedRecursively( //
					matcherAccessor, //
					actualPropertyValue,
					property, //
					criteriaBasedOnProperties, //
					currentPropertyPath //
			);
		} else {
			Optional<?> optionalConvertedPropValue = matcherAccessor //
					.getValueTransformerForPath(currentPropertyDotPath) //
					.apply(Optional.ofNullable(actualPropertyValue));

			// If the value is empty, don't try to match against it
			if (optionalConvertedPropValue.isEmpty()) {
				return;
			}

			Object convPropValue = optionalConvertedPropValue.get();
			boolean ignoreCase = matcherAccessor.isIgnoreCaseForPath(currentPropertyDotPath);

			String column = property.getName();

			switch (matcherAccessor.getStringMatcherForPath(currentPropertyDotPath)) {
				case DEFAULT:
				case EXACT:
					criteriaBasedOnProperties.add(includeNulls(matcherAccessor) //
							? Criteria.where(column).isNull().or(column).is(convPropValue).ignoreCase(ignoreCase)
							: Criteria.where(column).is(convPropValue).ignoreCase(ignoreCase));
					break;
				case ENDING:
					criteriaBasedOnProperties.add(includeNulls(matcherAccessor) //
							? Criteria.where(column).isNull().or(column).like("%" + convPropValue).ignoreCase(ignoreCase)
							: Criteria.where(column).like("%" + convPropValue).ignoreCase(ignoreCase));
					break;
				case STARTING:
					criteriaBasedOnProperties.add(includeNulls(matcherAccessor) //
							? Criteria.where(column).isNull().or(column).like(convPropValue + "%").ignoreCase(ignoreCase)
							: Criteria.where(column).like(convPropValue + "%").ignoreCase(ignoreCase));
					break;
				case CONTAINING:
					criteriaBasedOnProperties.add(includeNulls(matcherAccessor) //
							? Criteria.where(column).isNull().or(column).like("%" + convPropValue + "%").ignoreCase(ignoreCase)
							: Criteria.where(column).like("%" + convPropValue + "%").ignoreCase(ignoreCase));
					break;
				default:
					throw new IllegalStateException(matcherAccessor.getDefaultStringMatcher() + " is not supported");
			}
		}

	}

	/**
	 * Processes an embedded entity's properties recursively.
	 *
	 * @param matcherAccessor the input matcher on the {@link Example#getProbe() original probe}.
	 * @param value the actual embedded object.
	 * @param property the embedded property.
	 * @param criteriaBasedOnProperties collection of {@link Criteria} objects to potentially enrich.
	 * @param currentPropertyPath the dot-separated path of the passed {@code property}.
	 */
	private void processEmbeddedRecursively(
			ExampleMatcherAccessor matcherAccessor,
			Object value,
			RelationalPersistentProperty property,
			List<Criteria> criteriaBasedOnProperties,
			PropertyPath currentPropertyPath
	) {
		RelationalPersistentEntity<?> embeddedPersistentEntity = mappingContext.getPersistentEntity(property.getTypeInformation());

		PersistentPropertyAccessor<?> embeddedEntityPropertyAccessor = embeddedPersistentEntity.getPropertyAccessor(value);

		embeddedPersistentEntity.doWithProperties((PropertyHandler<RelationalPersistentProperty>) embeddedProperty ->
						potentiallyEnrichCriteria(
								currentPropertyPath,
								matcherAccessor,
								embeddedEntityPropertyAccessor,
								embeddedProperty,
								criteriaBasedOnProperties
						)
		);
	}

	private static PropertyPath resolveCurrentPropertyPath(@Nullable PropertyPath propertyPath, RelationalPersistentProperty property) {
		PropertyPath currentPropertyPath;

		if (propertyPath == null) {
			currentPropertyPath = PropertyPath.from(property.getName(), property.getOwner().getTypeInformation());
		} else {
			currentPropertyPath = propertyPath.nested(property.getName());
		}
		return currentPropertyPath;
	}

	/**
	 * Does this {@link ExampleMatcherAccessor} need to include {@literal NULL} values in its {@link Criteria}?
	 *
	 * @return whether to include nulls.
	 */
	private static <T> boolean includeNulls(ExampleMatcherAccessor exampleMatcher) {
		return exampleMatcher.getNullHandler() == NullHandler.INCLUDE;
	}
}
