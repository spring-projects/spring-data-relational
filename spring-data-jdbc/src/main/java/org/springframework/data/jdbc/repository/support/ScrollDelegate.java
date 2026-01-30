/*
 * Copyright 2023-2025 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.springframework.data.domain.KeysetScrollPosition;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.OffsetScrollPosition;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Window;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Query;
import org.springframework.util.Assert;

/**
 * Delegate to run {@link ScrollPosition scroll queries} and create result {@link Window}.
 *
 * @author Mark Paluch
 * @author Artemij Degtyarev
 * @since 3.1.4
 */
public class ScrollDelegate {
	/**
	 * Run the {@link String} and return a scroll {@link Window}
	 *
	 * @param resultCollection must not be {@literal null}
	 * @param scrollPosition must not be {@literal null}
	 * @return the scroll {@link Window}
	 */
	public static <T> Window<T> scroll(Collection<T> resultCollection, ScrollPosition scrollPosition, Limit limit, Sort sort, RelationalPersistentEntity<?> entity) {
		Assert.notNull(scrollPosition, "ScrollPosition must not be null");
		Assert.notNull(resultCollection, "ResultCollection must not be null");

		List<T> resultList = resultCollection instanceof List<T> ? (List<T>) resultCollection : new ArrayList<>(resultCollection);
		if (scrollPosition instanceof OffsetScrollPosition offset) {
			return createWindow(resultList, limit, offset.positionFunction());
		}

		if (scrollPosition instanceof KeysetScrollPosition keyset) {
			List<Sort.Order> orderList = sort.get().toList();
			List<String> sortProperties = orderList.stream()
				.map(Sort.Order::getProperty)
				.toList();

			Map<String, Object> keys = keyset.getKeys();
			if (!keys.isEmpty()) {
				Set<String> keySet = keys.keySet();
				Set<String> properties = new HashSet<>(sortProperties);

				Set<String> missingKeys = keySet.stream()
					.filter(key -> !properties.contains(key))
					.collect(Collectors.toSet());

				if (!missingKeys.isEmpty())
					throw new IllegalStateException(
						"Keyset keys are not present in sort properties: " + missingKeys +
							". Available sort properties: " + properties
					);
			}

			Set<RelationalPersistentProperty> properties = new HashSet<>();
			for (String propertyName : keys.keySet()) {
				RelationalPersistentProperty prop = entity.getPersistentProperty(propertyName);
				if (prop == null)
					throw new IllegalArgumentException(
						"Unknown property: " + propertyName + ". Available properties: " + String.join(", ", sortProperties)
					);

				properties.add(prop);
			}

			if (properties.isEmpty()) {
				for (String sortProperty : sortProperties) {
					RelationalPersistentProperty prop = entity.getPersistentProperty(sortProperty);
					if (prop == null)
						throw new IllegalArgumentException("Unknown property: " + sortProperty + ".");

					properties.add(prop);
				}
			}

			Map<String, Object> resultKeys = extractKeys(resultList, properties, limit, entity);
			ScrollPosition.Direction direction = keyset.getDirection();

			IntFunction<? extends ScrollPosition> positionFunction = ignoredI -> ScrollPosition.of(resultKeys, direction);

			return createWindow(resultList, limit, positionFunction);
		}

		throw new UnsupportedOperationException("ScrollPosition " + scrollPosition + " not supported");
	}

	/**
	 * Run the {@link Query} and return a scroll {@link Window}.
	 *
	 * @param query must not be {@literal null}.
	 * @param scrollPosition must not be {@literal null}.
	 * @return the scroll {@link Window}.
	 */
	public static <T> Window<T> scroll(Query query, Function<Query, List<T>> queryFunction,
			ScrollPosition scrollPosition) {

		Assert.notNull(scrollPosition, "ScrollPosition must not be null");

		int limit = query.getLimit();
		if (limit > 0 && limit != Integer.MAX_VALUE) {
			query = query.limit(limit + 1);
		}

		List<T> result = queryFunction.apply(query);

		if (scrollPosition instanceof OffsetScrollPosition offset) {
			return createWindow(result, limit, offset.positionFunction());
		}

		throw new UnsupportedOperationException("ScrollPosition " + scrollPosition + " not supported");
	}

	private static <T> Map<String, Object> extractKeys(List<T> resultList, Set<RelationalPersistentProperty> properties, Limit limit, RelationalPersistentEntity<?> entity) {
		if (resultList.isEmpty())
			return Map.of();

		int pageSize = limit.isUnlimited() ? 0 : limit.max();
		List<T> limitedList = getFirst(pageSize, resultList);

		if (limitedList.isEmpty())
			return Map.of();

		T last = limitedList.get(limitedList.size() - 1);
		PersistentPropertyAccessor<T> accessor = entity.getPropertyAccessor(last);

		Map<String, Object> result = new HashMap<>();
		for (RelationalPersistentProperty property : properties) {
			String propertyName = property.getName();
			Object propertyValue = accessor.getProperty(property);

			if (propertyValue == null)
				throw new IllegalStateException("Unexpected null value for keyset property '" + propertyName + "'. Keyset pagination requires all sort properties to be non-nullable");

			result.put(propertyName, propertyValue);
		}

		return result;
	}

	private static <T> Window<T> createWindow(List<T> result, Limit limit, IntFunction<? extends ScrollPosition> positionFunction) {
		int limitSize = limit.isLimited() ? limit.max() : 0;

		return createWindow(result, limitSize, positionFunction);
	}

	private static <T> Window<T> createWindow(List<T> result, int limit,
			IntFunction<? extends ScrollPosition> positionFunction) {
		return Window.from(getFirst(limit, result), positionFunction, hasMoreElements(result, limit));
	}

	private static boolean hasMoreElements(List<?> result, int limit) {
		return !result.isEmpty() && result.size() > limit;
	}

	/**
	 * Return the first {@code count} items from the list.
	 *
	 * @param count the number of first elements to be included in the returned list.
	 * @param list must not be {@literal null}
	 * @return the returned sublist if the {@code list} is greater {@code count}.
	 * @param <T> the element type of the lists.
	 */
	public static <T> List<T> getFirst(int count, List<T> list) {

		if (count > 0 && list.size() > count) {
			return list.subList(0, count);
		}

		return list;
	}

}
