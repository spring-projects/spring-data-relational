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
package org.springframework.data.jdbc.core.convert;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.springframework.core.ResolvableType;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.lang.Nullable;

/**
 * Converters for aggregate references. They need a {@link ConversionService} in order to delegate the conversion of the
 * content of the {@link AggregateReference}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 2.3
 */
class AggregateReferenceConverters {

	/**
	 * Returns the converters to be registered.
	 *
	 * @return a collection of converters. Guaranteed to be not {@literal null}.
	 */
	public static Collection<GenericConverter> getConvertersToRegister(ConversionService conversionService) {

		return Arrays.asList(new AggregateReferenceToSimpleTypeConverter(conversionService),
				new SimpleTypeToAggregateReferenceConverter(conversionService));
	}

	/**
	 * Converts from an AggregateReference to its id, leaving the conversion of the id to the ultimate target type to the
	 * delegate {@link ConversionService}.
	 */
	@WritingConverter
	private static class AggregateReferenceToSimpleTypeConverter implements GenericConverter {

		private static final Set<ConvertiblePair> CONVERTIBLE_TYPES = Collections
				.singleton(new ConvertiblePair(AggregateReference.class, Object.class));

		private final ConversionService delegate;

		AggregateReferenceToSimpleTypeConverter(ConversionService delegate) {
			this.delegate = delegate;
		}

		@Override
		public Set<ConvertiblePair> getConvertibleTypes() {
			return CONVERTIBLE_TYPES;
		}

		@Override
		public Object convert(@Nullable Object source, TypeDescriptor sourceDescriptor, TypeDescriptor targetDescriptor) {

			if (source == null) {
				return null;
			}

			// if the target type is an AggregateReference we are going to assume it is of the correct type,
			// because it was already converted.
			Class<?> objectType = targetDescriptor.getObjectType();
			if (objectType.isAssignableFrom(AggregateReference.class)) {
				return source;
			}

			Object id = ((AggregateReference<?, ?>) source).getId();

			if (id == null) {
				throw new IllegalStateException(
						String.format("Aggregate references id must not be null when converting to %s from %s to %s", source,
								sourceDescriptor, targetDescriptor));
			}

			return delegate.convert(id, TypeDescriptor.valueOf(id.getClass()), targetDescriptor);
		}
	}

	/**
	 * Convert any simple type to an {@link AggregateReference}. If the {@literal targetDescriptor} contains information
	 * about the generic type id will properly get converted to the desired type by the delegate
	 * {@link ConversionService}.
	 */
	@ReadingConverter
	private static class SimpleTypeToAggregateReferenceConverter implements GenericConverter {

		private static final Set<ConvertiblePair> CONVERTIBLE_TYPES = Collections
				.singleton(new ConvertiblePair(Object.class, AggregateReference.class));

		private final ConversionService delegate;

		SimpleTypeToAggregateReferenceConverter(ConversionService delegate) {
			this.delegate = delegate;
		}

		@Override
		public Set<ConvertiblePair> getConvertibleTypes() {
			return CONVERTIBLE_TYPES;
		}

		@Override
		public Object convert(@Nullable Object source, TypeDescriptor sourceDescriptor, TypeDescriptor targetDescriptor) {

			if (source == null) {
				return null;
			}

			ResolvableType componentType = targetDescriptor.getResolvableType().getGenerics()[1];
			TypeDescriptor targetType = TypeDescriptor.valueOf(componentType.resolve());
			Object convertedId = delegate.convert(source, TypeDescriptor.valueOf(source.getClass()), targetType);

			return AggregateReference.to(convertedId);
		}
	}
}
