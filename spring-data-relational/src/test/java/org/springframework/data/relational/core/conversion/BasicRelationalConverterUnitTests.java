/*
 * Copyright 2018-2021 the original author or authors.
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

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.data.convert.ConverterBuilder;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;

import lombok.Data;
import lombok.Value;

/**
 * Unit tests for {@link BasicRelationalConverter}.
 *
 * @author Mark Paluch
 */
public class BasicRelationalConverterUnitTests {

	RelationalMappingContext context = new RelationalMappingContext();
	RelationalConverter converter;

	@BeforeEach
	public void before() throws Exception {

		Set<GenericConverter> converters = ConverterBuilder.writing(MyValue.class, String.class, MyValue::getFoo)
				.andReading(MyValue::new).getConverters();

		CustomConversions conversions = new CustomConversions(CustomConversions.StoreConversions.NONE, converters);
		context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());

		converter = new BasicRelationalConverter(context, conversions);
	}

	@Test // DATAJDBC-235
	@SuppressWarnings("unchecked")
	public void shouldUseConvertingPropertyAccessor() {

		RelationalPersistentEntity<MyEntity> entity = (RelationalPersistentEntity) context
				.getRequiredPersistentEntity(MyEntity.class);

		MyEntity instance = new MyEntity();

		PersistentPropertyAccessor<MyEntity> accessor = converter.getPropertyAccessor(entity, instance);
		RelationalPersistentProperty property = entity.getRequiredPersistentProperty("flag");
		accessor.setProperty(property, "1");

		assertThat(instance.isFlag()).isTrue();
	}

	@Test // DATAJDBC-235
	public void shouldConvertEnumToString() {

		Object result = converter.writeValue(MyEnum.ON, ClassTypeInformation.from(String.class));

		assertThat(result).isEqualTo("ON");
	}

	@Test // DATAJDBC-235
	public void shouldConvertStringToEnum() {

		Object result = converter.readValue("OFF", ClassTypeInformation.from(MyEnum.class));

		assertThat(result).isEqualTo(MyEnum.OFF);
	}

	@Test
	void shouldConvertArrayElementsToTargetElementType() throws NoSuchMethodException {
		TypeInformation<Object> typeInformation = ClassTypeInformation.fromReturnTypeOf(EntityWithArray.class.getMethod("getFloats"));
		Double[] value = {1.2d, 1.3d, 1.4d};
		Object result = converter.readValue(value, typeInformation);
		assertThat(result).isEqualTo(Arrays.asList(1.2f, 1.3f, 1.4f));
	}

	@Test // DATAJDBC-235
	@SuppressWarnings("unchecked")
	public void shouldCreateInstance() {

		RelationalPersistentEntity<WithConstructorCreation> entity = (RelationalPersistentEntity) context
				.getRequiredPersistentEntity(WithConstructorCreation.class);

		WithConstructorCreation result = converter.createInstance(entity, it -> "bar");

		assertThat(result.getFoo()).isEqualTo("bar");
	}

	@Test // DATAJDBC-516
	public void shouldConsiderWriteConverter() {

		Object result = converter.writeValue(new MyValue("hello-world"), ClassTypeInformation.from(MyValue.class));

		assertThat(result).isEqualTo("hello-world");
	}

	@Test // DATAJDBC-516
	public void shouldConsiderReadConverter() {

		Object result = converter.readValue("hello-world", ClassTypeInformation.from(MyValue.class));

		assertThat(result).isEqualTo(new MyValue("hello-world"));
	}

	@Data
	static class EntityWithArray {
		List<Float> floats;
	}

	@Data
	static class MyEntity {
		boolean flag;
	}

	@Value
	static class WithConstructorCreation {
		String foo;
	}

	@Value
	static class MyValue {
		String foo;
	}

	@Value
	static class MyEntityWithConvertibleProperty {

		MyValue myValue;
	}

	enum MyEnum {
		ON, OFF;
	}
}
