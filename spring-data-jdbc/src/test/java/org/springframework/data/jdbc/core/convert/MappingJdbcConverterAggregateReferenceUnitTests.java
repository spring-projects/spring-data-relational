/*
 * Copyright 2018-present the original author or authors.
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

import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.core.ResolvableType;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.jdbc.core.dialect.JdbcPostgresDialect;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.repository.CrudRepository;

/**
 * Unit tests for the handling of {@link AggregateReference}s in the {@link MappingJdbcConverter}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
class MappingJdbcConverterAggregateReferenceUnitTests {

	JdbcCustomConversions conversions = JdbcCustomConversions.of(JdbcPostgresDialect.INSTANCE,
			List.of(AggregateIdToLong.INSTANCE, NumberToAggregateId.INSTANCE));

	JdbcMappingContext context = new JdbcMappingContext();
	JdbcConverter converter = new MappingJdbcConverter(context, mock(RelationResolver.class), conversions,
			JdbcTypeFactory.unsupported());

	MappingJdbcConverterAggregateReferenceUnitTests() {
		context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
	}

	RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);

	@Test // DATAJDBC-221
	void convertsToAggregateReference() {

		final RelationalPersistentProperty property = entity.getRequiredPersistentProperty("reference");

		Object readValue = converter.readValue(23, property.getTypeInformation());

		assertThat(readValue).isInstanceOf(AggregateReference.class);
		assertThat(((AggregateReference<DummyEntity, Long>) readValue).getId()).isEqualTo(23L);
	}

	@Test // DATAJDBC-221
	void convertsFromAggregateReference() {

		RelationalPersistentProperty property = entity.getRequiredPersistentProperty("reference");

		AggregateReference<Object, Integer> reference = AggregateReference.to(23);

		Object writeValue = converter.writeValue(reference, TypeInformation.of(converter.getColumnType(property)));

		assertThat(writeValue).isEqualTo(23L);
	}

	@Test // GH-1828
	void considersReadConverter() {

		TypeInformation<?> targetType = TypeInformation
				.of(ResolvableType.forClassWithGenerics(AggregateReference.class, Object.class, AggregateId.class));

		AggregateReference<Object, Integer> reference = AggregateReference.to(23);
		AggregateReference<Object, AggregateId> converted = (AggregateReference) converter.readValue(reference, targetType);

		assertThat(converted.getId()).isEqualTo(new AggregateId(23L));
	}

	@Test // GH-1828
	void considersWriteConverter() {

		AggregateReference<Object, AggregateId> reference = AggregateReference.to(new AggregateId(23L));
		Object converted = converter.writeValue(reference, TypeInformation.of(Long.class));

		assertThat(converted).isEqualTo(23L);
	}

	private static class DummyEntity {

		@Id Long simple;
		AggregateReference<DummyEntity, Long> reference;
	}

	static class AggregateOne {

		@Id Long id;
		String name;
		AggregateReference<AggregateTwo, Long> two;
	}

	static class AggregateTwo {

		@Id Long id;
		String name;
	}

	interface ReferencingAggregateRepository extends CrudRepository<ReferencingAggregate, Long> {

	}

	record AggregateWithConvertableId(@Id AggregateId id, String name) {

	}

	record AggregateId(Long value) {

	}

	record ReferencingAggregate(@Id Long id, String name,
			AggregateReference<AggregateWithConvertableId, AggregateId> ref) {
	}

	@WritingConverter
	private enum AggregateIdToLong implements Converter<AggregateId, Long> {

		INSTANCE;

		@Override
		public Long convert(AggregateId source) {
			return source.value;
		}
	}

	@ReadingConverter
	private enum NumberToAggregateId implements Converter<Number, AggregateId> {

		INSTANCE;

		@Override
		public AggregateId convert(Number source) {
			return new AggregateId(source.longValue());
		}
	}
}
