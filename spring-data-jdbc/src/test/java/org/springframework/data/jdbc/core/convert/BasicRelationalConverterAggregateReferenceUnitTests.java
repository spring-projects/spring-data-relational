/*
 * Copyright 2018-2024 the original author or authors.
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
import static org.mockito.Mockito.*;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.TypeInformation;

/**
 * Unit tests for the handling of {@link AggregateReference}s in the {@link MappingJdbcConverter}.
 *
 * @author Jens Schauder
 */
public class BasicRelationalConverterAggregateReferenceUnitTests {

	JdbcMappingContext context = new JdbcMappingContext();
	JdbcConverter converter = new MappingJdbcConverter(context, mock(RelationResolver.class));

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

		Object writeValue = converter.writeValue(reference, TypeInformation.of(converter.getColumnType(property)));

		Assertions.assertThat(writeValue).isEqualTo(23L);
	}

	private static class DummyEntity {

		@Id Long simple;
		AggregateReference<DummyEntity, Long> reference;
	}
}
