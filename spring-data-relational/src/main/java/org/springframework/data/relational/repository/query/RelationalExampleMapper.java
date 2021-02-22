/*
 * Copyright 2021 the original author or authors.
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

import java.beans.PropertyDescriptor;

import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.beans.NotReadablePropertyException;
import org.springframework.data.domain.Example;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.util.Assert;

/**
 * Transform an {@link Example} into a {@link Query}.
 *
 * @author Greg Turnquist
 */
public class RelationalExampleMapper {

	private final RelationalMappingContext mappingContext;

	public RelationalExampleMapper(RelationalMappingContext mappingContext) {
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

		Assert.notNull(example, "Example must not be null!");
		Assert.notNull(entity, "RelationalPersistentEntity must not be null!");

		Criteria criteria = Criteria.empty();

		BeanWrapper beanWrapper = new BeanWrapperImpl(example.getProbe());

		for (PropertyDescriptor propertyDescriptor : beanWrapper.getPropertyDescriptors()) {

			// "class" isn't grounds for a query criteria
			if (propertyDescriptor.getName().equals("class")) {
				continue;
			}

			// if this property descriptor is part of the ignoredPaths set, skip over it.
			if (example.getMatcher().getIgnoredPaths().contains(propertyDescriptor.getName())) {
				continue;
			}

			Object propertyValue = null;
			try {
				propertyValue = beanWrapper.getPropertyValue(propertyDescriptor.getName());
			} catch (NotReadablePropertyException e) {}

			if (propertyValue != null) {

				String columnName = entity.getPersistentProperty(propertyDescriptor.getName()).getColumnName().getReference();

				Criteria propertyCriteria;

				// First, check the overall matcher for settings
				StringMatcher stringMatcher = example.getMatcher().getDefaultStringMatcher();
				boolean ignoreCase = example.getMatcher().isIgnoreCaseEnabled();

				// Then, apply any property-specific overrides
				if (example.getMatcher().getPropertySpecifiers().hasSpecifierForPath(propertyDescriptor.getName())) {

					PropertySpecifier propertySpecifier = example.getMatcher().getPropertySpecifiers()
							.getForPath(propertyDescriptor.getName());

					if (propertySpecifier.getStringMatcher() != null) {
						stringMatcher = propertySpecifier.getStringMatcher();
					}

					if (propertySpecifier.getIgnoreCase() != null) {
						ignoreCase = propertySpecifier.getIgnoreCase();
					}
				}

				// Assemble the property's criteria
				switch (stringMatcher) {
					case DEFAULT:
					case EXACT:
						propertyCriteria = includeNulls((Example<T>) example) //
								? Criteria.where(columnName).isNull().or(columnName).is(propertyValue).ignoreCase(ignoreCase)
								: Criteria.where(columnName).is(propertyValue).ignoreCase(ignoreCase);
						break;
					case ENDING:
						propertyCriteria = includeNulls(example) //
								? Criteria.where(columnName).isNull().or(columnName).like("%" + propertyValue).ignoreCase(ignoreCase)
								: Criteria.where(columnName).like("%" + propertyValue).ignoreCase(ignoreCase);
						break;
					case STARTING:
						propertyCriteria = includeNulls(example) //
								? Criteria.where(columnName).isNull().or(columnName).like(propertyValue + "%").ignoreCase(ignoreCase)
								: Criteria.where(columnName).like(propertyValue + "%").ignoreCase(ignoreCase);
						break;
					case CONTAINING:
						propertyCriteria = includeNulls(example) //
								? Criteria.where(columnName).isNull().or(columnName).like("%" + propertyValue + "%")
										.ignoreCase(ignoreCase)
								: Criteria.where(columnName).like("%" + propertyValue + "%").ignoreCase(ignoreCase);
						break;
					default:
						throw new IllegalStateException(example.getMatcher().getDefaultStringMatcher() + " is not supported!");
				}

				// Add this criteria based on any/all
				if (example.getMatcher().isAllMatching()) {
					criteria = criteria.and(propertyCriteria);
				} else {
					criteria = criteria.or(propertyCriteria);
				}
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
