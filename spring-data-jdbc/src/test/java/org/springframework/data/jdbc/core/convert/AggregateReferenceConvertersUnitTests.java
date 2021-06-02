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
package org.springframework.data.jdbc.core.convert;

import org.junit.jupiter.api.Test;
import org.springframework.core.ResolvableType;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.jdbc.core.mapping.AggregateReference;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for converters from an to {@link org.springframework.data.jdbc.core.mapping.AggregateReference}.
 *
 * @author Jens Schauder
 */
class AggregateReferenceConvertersUnitTests {

	AggregateReferenceConverters.SimpleTypeToAggregateReferenceConverter simpleToAggregate = new AggregateReferenceConverters.SimpleTypeToAggregateReferenceConverter(DefaultConversionService.getSharedInstance());
	AggregateReferenceConverters.AggregateReferenceToSimpleTypeConverter aggregateToSimple = new AggregateReferenceConverters.AggregateReferenceToSimpleTypeConverter(DefaultConversionService.getSharedInstance());

	@Test // #992
	void convertsFromSimpleValue() {

		ResolvableType aggregateReferenceWithIdTypeInteger = ResolvableType.forClassWithGenerics(AggregateReference.class, String.class, Integer.class);
		final Object converted = simpleToAggregate.convert(23, TypeDescriptor.forObject(23), new TypeDescriptor(aggregateReferenceWithIdTypeInteger, null, null));

		assertThat(converted).isEqualTo(AggregateReference.to(23));
	}

	@Test // #992
	void convertsFromSimpleValueThatNeedsSeparateConversion() {

		ResolvableType aggregateReferenceWithIdTypeInteger = ResolvableType.forClassWithGenerics(AggregateReference.class, String.class, Long.class);
		final Object converted = simpleToAggregate.convert(23, TypeDescriptor.forObject(23), new TypeDescriptor(aggregateReferenceWithIdTypeInteger, null, null));

		assertThat(converted).isEqualTo(AggregateReference.to(23L));
	}

	@Test // #992
	void convertsFromSimpleValueWithMissingTypeInformation() {

		final Object converted = simpleToAggregate.convert(23, TypeDescriptor.forObject(23), TypeDescriptor.valueOf(AggregateReference.class));

		assertThat(converted).isEqualTo(AggregateReference.to(23));
	}

	@Test // #992
	void convertsToSimpleValue() {

		final AggregateReference<Object, Integer> source = AggregateReference.to(23);

		final Object converted = aggregateToSimple.convert(source, TypeDescriptor.forObject(source), TypeDescriptor.valueOf(Integer.class));

		assertThat(converted).isEqualTo(23);
	}

	@Test // #992
	void convertsToSimpleValueThatNeedsSeparateConversion() {

		final AggregateReference<Object, Integer> source = AggregateReference.to(23);

		final Object converted = aggregateToSimple.convert(source, TypeDescriptor.forObject(source), TypeDescriptor.valueOf(Long.class));

		assertThat(converted).isEqualTo(23L);
	}


}