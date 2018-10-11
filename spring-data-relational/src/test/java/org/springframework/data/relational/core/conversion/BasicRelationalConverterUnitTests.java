/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.relational.core.conversion;

import static org.assertj.core.api.Assertions.*;

import lombok.Data;
import lombok.Value;

import org.junit.Test;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.util.ClassTypeInformation;

/**
 * Unit tests for {@link BasicRelationalConverter}.
 *
 * @author Mark Paluch
 */
public class BasicRelationalConverterUnitTests {

	RelationalMappingContext context = new RelationalMappingContext();
	RelationalConverter converter = new BasicRelationalConverter(context);

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

	@Test // DATAJDBC-235
	@SuppressWarnings("unchecked")
	public void shouldCreateInstance() {

		RelationalPersistentEntity<MyValue> entity = (RelationalPersistentEntity) context
				.getRequiredPersistentEntity(MyValue.class);

		MyValue result = converter.createInstance(entity, it -> "bar");

		assertThat(result.getFoo()).isEqualTo("bar");
	}

	@Data
	static class MyEntity {
		boolean flag;
	}

	@Value
	static class MyValue {
		final String foo;
	}

	enum MyEnum {
		ON, OFF;
	}
}
