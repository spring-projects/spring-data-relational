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

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.core.ResolvableType;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.support.ConfigurableConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.jdbc.core.mapping.AggregateReference;

/**
 * Tests for converters from an to {@link org.springframework.data.jdbc.core.mapping.AggregateReference}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
class AggregateReferenceConvertersUnitTests {

	ConfigurableConversionService conversionService;

	@BeforeEach
	void setUp() {
		conversionService = new DefaultConversionService();
		AggregateReferenceConverters.getConvertersToRegister(DefaultConversionService.getSharedInstance())
				.forEach(it -> conversionService.addConverter(it));
	}

	@Test // GH-992
	void convertsFromSimpleValue() {

		ResolvableType aggregateReferenceWithIdTypeInteger = ResolvableType.forClassWithGenerics(AggregateReference.class,
				String.class, Integer.class);
		Object converted = conversionService.convert(23, TypeDescriptor.forObject(23),
				new TypeDescriptor(aggregateReferenceWithIdTypeInteger, null, null));

		assertThat(converted).isEqualTo(AggregateReference.to(23));
	}

	@Test // GH-992
	void convertsFromSimpleValueThatNeedsSeparateConversion() {

		ResolvableType aggregateReferenceWithIdTypeInteger = ResolvableType.forClassWithGenerics(AggregateReference.class,
				String.class, Long.class);
		Object converted = conversionService.convert(23, TypeDescriptor.forObject(23),
				new TypeDescriptor(aggregateReferenceWithIdTypeInteger, null, null));

		assertThat(converted).isEqualTo(AggregateReference.to(23L));
	}

	@Test // GH-992
	void convertsFromSimpleValueWithMissingTypeInformation() {

		Object converted = conversionService.convert(23, TypeDescriptor.forObject(23),
				TypeDescriptor.valueOf(AggregateReference.class));

		assertThat(converted).isEqualTo(AggregateReference.to(23));
	}

	@Test // GH-992
	void convertsToSimpleValue() {

		AggregateReference<Object, Integer> source = AggregateReference.to(23);

		Object converted = conversionService.convert(source, TypeDescriptor.forObject(source),
				TypeDescriptor.valueOf(Integer.class));

		assertThat(converted).isEqualTo(23);
	}

	@Test // GH-992
	void convertsToSimpleValueThatNeedsSeparateConversion() {

		AggregateReference<Object, Integer> source = AggregateReference.to(23);

		Object converted = conversionService.convert(source, TypeDescriptor.forObject(source),
				TypeDescriptor.valueOf(Long.class));

		assertThat(converted).isEqualTo(23L);
	}

}
