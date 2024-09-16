/*
 * Copyright 2023-2024 the original author or authors.
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
package org.springframework.data.relational.core.conversion;

import static org.assertj.core.api.Assertions.*;

import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.convert.ConverterBuilder;
import org.springframework.data.convert.ConverterBuilder.ConverterAware;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.CustomConversions.StoreConversions;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.domain.RowDocument;
import org.springframework.data.util.TypeInformation;

/**
 * Unit tests for {@link MappingRelationalConverter}.
 *
 * @author Mark Paluch
 * @author Lukáš Křečan
 * @author Jens Schauder
 */
class MappingRelationalConverterUnitTests {

	MappingRelationalConverter converter = new MappingRelationalConverter(new RelationalMappingContext());

	@Test // GH-1586
	void shouldReadSimpleType() {

		RowDocument document = new RowDocument().append("id", "1").append("name", "John").append("age", 30);

		SimpleType result = converter.read(SimpleType.class, document);

		assertThat(result.id).isEqualTo("1");
		assertThat(result.name).isEqualTo("John");
		assertThat(result.age).isEqualTo(30);
	}

	@Test // GH-1586
	void shouldReadSimpleRecord() {

		RowDocument document = new RowDocument().append("id", "1").append("name", "John").append("age", 30);

		SimpleRecord result = converter.read(SimpleRecord.class, document);

		assertThat(result.id).isEqualTo("1");
		assertThat(result.name).isEqualTo("John");
		assertThat(result.age).isEqualTo(30);
	}

	@Test // GH-1586
	void shouldEvaluateExpression() {

		RowDocument document = new RowDocument().append("myprop", "bar");

		WithExpression result = converter.read(WithExpression.class, document);

		assertThat(result.name).isEqualTo("bar");
	}

	@Test
	// GH-1689
	void shouldApplySimpleTypeConverterSimpleType() {

		converter = new MappingRelationalConverter(converter.getMappingContext(),
				new CustomConversions(StoreConversions.NONE, List.of(MyEnumConverter.INSTANCE)));

		RowDocument document = new RowDocument().append("my_enum", "one");

		WithMyEnum result = converter.read(WithMyEnum.class, document);

		assertThat(result.myEnum).isEqualTo(MyEnum.ONE);
	}

	@Test // GH-1586
	void shouldReadNonstaticInner() {

		RowDocument document = new RowDocument().append("name", "John").append("child",
				new RowDocument().append("name", "Johnny"));

		Parent result = converter.read(Parent.class, document);

		assertThat(result.name).isEqualTo("John");
		assertThat(result.child.name).isEqualTo("Johnny");
	}

	@Test // GH-1586
	void shouldReadEmbedded() {

		RowDocument document = new RowDocument().append("simple_id", "1").append("simple_name", "John").append("simple_age",
				30);

		WithEmbedded result = converter.read(WithEmbedded.class, document);

		assertThat(result.simple).isNotNull();
		assertThat(result.simple.id).isEqualTo("1");
		assertThat(result.simple.name).isEqualTo("John");
		assertThat(result.simple.age).isEqualTo(30);
	}

	@Test // GH-1586
	void shouldReadWithLists() {

		RowDocument nested = new RowDocument().append("id", "1").append("name", "John").append("age", 30);

		RowDocument document = new RowDocument().append("strings", List.of("one", "two"))
				.append("states", List.of("NEW", "USED")).append("entities", List.of(nested));

		WithCollection result = converter.read(WithCollection.class, document);

		assertThat(result.strings).containsExactly("one", "two");
		assertThat(result.states).containsExactly(State.NEW, State.USED);
		assertThat(result.entities).hasSize(1);

		assertThat(result.entities.get(0).id).isEqualTo("1");
		assertThat(result.entities.get(0).name).isEqualTo("John");
	}

	@Test // GH-1586
	void shouldReadWithMaps() {

		RowDocument nested = new RowDocument().append("id", "1").append("name", "John").append("age", 30);

		RowDocument document = new RowDocument().append("strings", Map.of(1, "one", 2, "two")).append("entities",
				Map.of(1, nested));

		WithMap result = converter.read(WithMap.class, document);

		assertThat(result.strings).hasSize(2).containsEntry(1, "one").containsEntry(2, "two");

		assertThat(result.entities).hasSize(1);
		assertThat(result.entities.get(1).id).isEqualTo("1");
		assertThat(result.entities.get(1).name).isEqualTo("John");
	}

	@Test // GH-1586
	void shouldApplyConverters() {

		ConverterAware converterAware = ConverterBuilder.reading(String.class, Money.class, s -> {
			String[] s1 = s.split(" ");
			return new Money(Integer.parseInt(s1[0]), s1[1]);
		}).andWriting(money -> money.amount + " " + money.currency);

		CustomConversions conversions = new CustomConversions(StoreConversions.of(SimpleTypeHolder.DEFAULT),
				List.of(converterAware));
		RelationalMappingContext mappingContext = new RelationalMappingContext();
		mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		mappingContext.afterPropertiesSet();

		MappingRelationalConverter converter = new MappingRelationalConverter(mappingContext, conversions);

		RowDocument document = new RowDocument().append("money", "1 USD");

		WithMoney result = converter.read(WithMoney.class, document);

		assertThat(result.money.amount).isEqualTo(1);
		assertThat(result.money.currency).isEqualTo("USD");
	}

	@Test // GH-1554
	void projectShouldReadNestedProjection() {

		RowDocument source = RowDocument.of("addresses", Collections.singletonList(RowDocument.of("s", "hwy")));

		EntityProjection<WithNestedProjection, Person> projection = converter
				.introspectProjection(WithNestedProjection.class, Person.class);
		WithNestedProjection person = converter.project(projection, source);

		assertThat(person.getAddresses()).extracting(AddressProjection::getStreet).hasSize(1).containsOnly("hwy");
	}

	@Test // GH-1554
	void projectShouldReadProjectionWithNestedEntity() {

		RowDocument source = RowDocument.of("addresses", Collections.singletonList(RowDocument.of("s", "hwy")));

		EntityProjection<ProjectionWithNestedEntity, Person> projection = converter
				.introspectProjection(ProjectionWithNestedEntity.class, Person.class);
		ProjectionWithNestedEntity person = converter.project(projection, source);

		assertThat(person.getAddresses()).extracting(Address::getStreet).hasSize(1).containsOnly("hwy");
	}

	@Test // GH-1842
	void shouldApplyGenericTypeConverter() {

		converter = new MappingRelationalConverter(converter.getMappingContext(),
				new CustomConversions(StoreConversions.NONE, List.of(GenericTypeConverter.INSTANCE)));

		UUID uuid = UUID.randomUUID();
		GenericClass<UUID> wrappedUuid = new GenericClass<>(uuid);
		GenericClass<String> wrappedString = new GenericClass<>("test");

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(converter.writeValue(uuid, TypeInformation.of(GenericClass.class))).isEqualTo(wrappedUuid);
			softly.assertThat(converter.writeValue(wrappedUuid, TypeInformation.of(UUID.class))).isEqualTo(uuid);

			softly.assertThat(converter.writeValue("test", TypeInformation.of(GenericClass.class))).isEqualTo(wrappedString);
			softly.assertThat(converter.writeValue(wrappedString, TypeInformation.of(String.class))).isEqualTo("test");
		});
	}

	static class SimpleType {

		@Id String id;
		String name;
		int age;

	}

	record SimpleRecord(@Id String id, String name, int age) {
	}

	static class Parent {
		String name;

		Child child;

		class Child {

			String name;
		}
	}

	static class WithCollection {

		List<String> strings;
		EnumSet<State> states;
		List<SimpleType> entities;
	}

	static class WithMap {

		Map<Integer, String> strings;
		Map<Integer, SimpleType> entities;
	}

	enum State {
		NEW, USED, UNKNOWN
	}

	static class WithMoney {

		Money money;

	}

	static class Money {
		final int amount;
		final String currency;

		public Money(int amount, String currency) {
			this.amount = amount;
			this.currency = currency;
		}
	}

	static class WithExpression {

		private final String name;

		public WithExpression(@Value("#root.myprop") String foo) {
			this.name = foo;
		}
	}

	static class WithEmbedded {

		@Embedded.Nullable(prefix = "simple_") SimpleType simple;
	}

	static class Person {

		@Id String id;

		Date birthDate;

		@Column("foo") String firstname;
		String lastname;

		Set<Address> addresses;

		Person() {

		}

		@PersistenceCreator
		public Person(Set<Address> addresses) {
			this.addresses = addresses;
		}
	}

	static class Address {

		@Column("s") String street;
		String city;

		public String getStreet() {
			return street;
		}

		public String getCity() {
			return city;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Address address = (Address) o;
			return Objects.equals(street, address.street) && Objects.equals(city, address.city);
		}

		@Override
		public int hashCode() {
			return Objects.hash(street, city);
		}
	}

	interface WithNestedProjection {

		Set<AddressProjection> getAddresses();
	}

	interface ProjectionWithNestedEntity {

		Set<Address> getAddresses();
	}

	interface AddressProjection {

		String getStreet();
	}

	record WithMyEnum(MyEnum myEnum) {
	}

	enum MyEnum {
		ONE, TWO,
	}

	@ReadingConverter
	enum MyEnumConverter implements Converter<String, MyEnum> {

		INSTANCE;

		@Override
		public MyEnum convert(String source) {
			return MyEnum.valueOf(source.toUpperCase());
		}

	}

	@WritingConverter
	enum GenericTypeConverter implements GenericConverter {

		INSTANCE;

		@Override
		public Set<ConvertiblePair> getConvertibleTypes() {
			return Set.of(new ConvertiblePair(String.class, GenericClass.class),
					new ConvertiblePair(UUID.class, GenericClass.class), new ConvertiblePair(GenericClass.class, String.class),
					new ConvertiblePair(GenericClass.class, UUID.class));
		}

		@Override
		public Object convert(Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {
			if (targetType.getType() == GenericClass.class)
				return new GenericClass<>(source);

			return ((GenericClass<?>) source).value();
		}

	}

	public record GenericClass<T>(T value) {
	}

}
