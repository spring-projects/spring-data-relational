/*
 * Copyright 2021-2024 the original author or authors.
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

import org.springframework.data.domain.Example;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PropertyHandler;
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
	 * @param entity
	 * @return query
	 */
	private <T> Query getMappedExample(Example<T> example, RelationalPersistentEntity<?> entity) {

		Assert.notNull(example, "Example must not be null");
		Assert.notNull(entity, "RelationalPersistentEntity must not be null");

		PersistentPropertyAccessor<T> propertyAccessor = entity.getPropertyAccessor(example.getProbe());
		ExampleMatcherAccessor matcherAccessor = new ExampleMatcherAccessor(example.getMatcher());

		final List<Criteria> criteriaBasedOnProperties = new ArrayList<>();

		entity.doWithProperties((PropertyHandler<RelationalPersistentProperty>) property -> {

			if (matcherAccessor.isIgnoredPath(property.getName())) {
				return;
			}

			Optional<?> optionalConvertedPropValue = matcherAccessor //
					.getValueTransformerForPath(property.getName()) //
					.apply(Optional.ofNullable(propertyAccessor.getProperty(property)));

			// If the value is empty, don't try to match against it
			if (!optionalConvertedPropValue.isPresent()) {
				return;
			}

			Object convPropValue = optionalConvertedPropValue.get();
			boolean ignoreCase = matcherAccessor.isIgnoreCaseForPath(property.getName());

			String column = property.getName();

			switch (matcherAccessor.getStringMatcherForPath(property.getName())) {
				case DEFAULT:
				case EXACT:
					criteriaBasedOnProperties.add(includeNulls(example) //
							? Criteria.where(column).isNull().or(column).is(convPropValue).ignoreCase(ignoreCase)
							: Criteria.where(column).is(convPropValue).ignoreCase(ignoreCase));
					break;
				case ENDING:
					criteriaBasedOnProperties.add(includeNulls(example) //
							? Criteria.where(column).isNull().or(column).like("%" + convPropValue).ignoreCase(ignoreCase)
							: Criteria.where(column).like("%" + convPropValue).ignoreCase(ignoreCase));
					break;
				case STARTING:
					criteriaBasedOnProperties.add(includeNulls(example) //
							? Criteria.where(column).isNull().or(column).like(convPropValue + "%").ignoreCase(ignoreCase)
							: Criteria.where(column).like(convPropValue + "%").ignoreCase(ignoreCase));
					break;
				case CONTAINING:
					criteriaBasedOnProperties.add(includeNulls(example) //
							? Criteria.where(column).isNull().or(column).like("%" + convPropValue + "%").ignoreCase(ignoreCase)
							: Criteria.where(column).like("%" + convPropValue + "%").ignoreCase(ignoreCase));
					break;
				default:
					throw new IllegalStateException(example.getMatcher().getDefaultStringMatcher() + " is not supported");
			}
		});

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

	/**
	 * Does this {@link Example} need to include {@literal NULL} values in its {@link Criteria}?
	 *
	 * @param example
	 * @return whether or not to include nulls.
	 */
	private static <T> boolean includeNulls(Example<T> example) {
		return example.getMatcher().getNullHandler() == NullHandler.INCLUDE;
	}
}
