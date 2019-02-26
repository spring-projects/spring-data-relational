/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.core.convert;

import static org.assertj.core.api.Assertions.*;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.Test;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.ClassTypeInformation;

/**
 * Unit tests for the handling of {@link AggregateReference}s in the
 * {@link org.springframework.data.relational.core.conversion.BasicRelationalConverter}.
 *
 * @author Jens Schauder
 */
public class BasicRelationalConverterAggregateReferenceUnitTests {

	SoftAssertions softly = new SoftAssertions();

	ConversionService conversionService = new DefaultConversionService();

	JdbcMappingContext context = new JdbcMappingContext();
	RelationalConverter converter = new BasicJdbcConverter(context, JdbcTypeFactory.DUMMY_JDBC_TYPE_FACTORY);

	RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);

	@Test // DATAJDBC-221
	public void convertsToAggregateReference() {

		final RelationalPersistentProperty property = entity.getRequiredPersistentProperty("reference");

		Object readValue = converter.readValue(23, property.getTypeInformation());

		Assertions.assertThat(readValue).isInstanceOf(AggregateReference.class);
		assertThat(((AggregateReference<DummyEntity, Long>) readValue).getId()).isEqualTo(23L);
	}

	@Test // DATAJDBC-221
	public void convertsFromAggregateReference() {

		final RelationalPersistentProperty property = entity.getRequiredPersistentProperty("reference");

		AggregateReference<Object, Integer> reference = AggregateReference.to(23);

		Object writeValue = converter.writeValue(reference, ClassTypeInformation.from(property.getColumnType()));

		Assertions.assertThat(writeValue).isEqualTo(23L);
	}

	private static class DummyEntity {

		@Id Long simple;
		AggregateReference<DummyEntity, Long> reference;
	}
}
