/*
 * Copyright 2017-2024 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.ConditionalConverter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * A converter for creating a {@link Map} from an {@link Iterable<Map.Entry>}.
 *
 * @author Jens Schauder
 */
class IterableOfEntryToMapConverter implements ConditionalConverter, Converter<Iterable<?>, Map<?, ?>> {

	@SuppressWarnings("unchecked")
	@Nullable
	@Override
	public Map<?, ?> convert(Iterable<?> source) {

		Map result = new HashMap();

		source.forEach(element -> {

			if (element instanceof Entry entry) {
				result.put(entry.getKey(), entry.getValue());
				return;
			}

			throw new IllegalArgumentException(String.format("Cannot convert %s to Map.Entry", element.getClass()));
		});

		return result;
	}

	/**
	 * Tries to determine if the {@literal sourceType} can be converted to a {@link Map}. If this can not be determined,
	 * because the sourceType does not contain information about the element type it returns {@literal true}.
	 *
	 * @param sourceType {@link TypeDescriptor} to convert from.
	 * @param targetType {@link TypeDescriptor} to convert to.
	 * @return if the sourceType can be converted to a Map.
	 */
	@Override
	public boolean matches(TypeDescriptor sourceType, TypeDescriptor targetType) {

		Assert.notNull(sourceType, "Source type must not be null");
		Assert.notNull(targetType, "Target type must not be null");

		if (!sourceType.isAssignableTo(TypeDescriptor.valueOf(Iterable.class)))
			return false;

		TypeDescriptor elementDescriptor = sourceType.getElementTypeDescriptor();
		return elementDescriptor == null || elementDescriptor.isAssignableTo(TypeDescriptor.valueOf(Entry.class));
	}
}
