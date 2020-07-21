/*
 * Copyright 2019-2020 the original author or authors.
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
package org.springframework.data.r2dbc.core;

import static org.assertj.core.api.Assertions.*;

import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.mapping.SettableValue;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.r2dbc.core.Parameter;

/**
 * {@link PostgresDialect} specific tests for {@link ReactiveDataAccessStrategy}.
 *
 * @author Mark Paluch
 */
public class PostgresReactiveDataAccessStrategyTests extends ReactiveDataAccessStrategyTestSupport {

	private final ReactiveDataAccessStrategy strategy = new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE);

	@Override
	protected ReactiveDataAccessStrategy getStrategy() {
		return strategy;
	}

	@Test // gh-161
	public void shouldConvertPrimitiveMultidimensionArrayToWrapper() {

		OutboundRow row = strategy.getOutboundRow(new WithMultidimensionalArray(new int[][] { { 1, 2, 3 }, { 4, 5 } }));

		assertThat(row.get(SqlIdentifier.unquoted("myarray")).hasValue()).isTrue();
		assertThat(row.get(SqlIdentifier.unquoted("myarray")).getValue()).isInstanceOf(Integer[][].class);
	}

	@Test // gh-161
	public void shouldConvertNullArrayToDriverArrayType() {

		OutboundRow row = strategy.getOutboundRow(new WithMultidimensionalArray(null));

		assertThat(row.get(SqlIdentifier.unquoted("myarray")).hasValue()).isFalse();
		assertThat(row.get(SqlIdentifier.unquoted("myarray")).getType()).isEqualTo(Integer[].class);
	}

	@Test // gh-161
	public void shouldConvertCollectionToArray() {

		OutboundRow row = strategy.getOutboundRow(new WithIntegerCollection(Arrays.asList(1, 2, 3)));

		assertThat(row.get(SqlIdentifier.unquoted("myarray")).hasValue()).isTrue();
		assertThat(row.get(SqlIdentifier.unquoted("myarray")).getValue()).isInstanceOf(Integer[].class);
		assertThat((Integer[]) row.get(SqlIdentifier.unquoted("myarray")).getValue()).contains(1, 2, 3);
	}

	@Test // gh-139
	public void shouldConvertToArray() {

		DefaultReactiveDataAccessStrategy strategy = new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE);

		WithArray withArray = new WithArray();
		withArray.stringArray = new String[] { "hello", "world" };
		withArray.stringList = Arrays.asList("hello", "world");

		OutboundRow outboundRow = strategy.getOutboundRow(withArray);

		assertThat(outboundRow) //
				.containsEntry(SqlIdentifier.unquoted("string_array"), Parameter.from(new String[] { "hello", "world" }))
				.containsEntry(SqlIdentifier.unquoted("string_list"), Parameter.from(new String[] { "hello", "world" }));
	}

	@Test // gh-139
	public void shouldApplyCustomConversion() {

		DefaultReactiveDataAccessStrategy strategy = new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE,
				Collections.singletonList(MyObjectsToStringConverter.INSTANCE));

		WithConversion withConversion = new WithConversion();
		withConversion.myObjects = Arrays.asList(new MyObject("one"), new MyObject("two"));

		OutboundRow outboundRow = strategy.getOutboundRow(withConversion);

		assertThat(outboundRow) //
				.containsEntry(SqlIdentifier.unquoted("my_objects"), Parameter.from("[one, two]"));
	}

	@Test // gh-139
	public void shouldApplyCustomConversionForNull() {

		DefaultReactiveDataAccessStrategy strategy = new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE,
				Collections.singletonList(MyObjectsToStringConverter.INSTANCE));

		WithConversion withConversion = new WithConversion();
		withConversion.myObjects = null;

		OutboundRow outboundRow = strategy.getOutboundRow(withConversion);

		assertThat(outboundRow) //
				.containsKey(SqlIdentifier.unquoted("my_objects"));

		Parameter value = outboundRow.get("my_objects");
		assertThat(value.isEmpty()).isTrue();
		assertThat(value.getType()).isEqualTo(String.class);
	}

	@Test // gh-252
	public void shouldConvertSetOfEnumToString() {

		DefaultReactiveDataAccessStrategy strategy = new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE,
				Collections.singletonList(MyObjectsToStringConverter.INSTANCE));

		WithEnumCollections withEnums = new WithEnumCollections();
		withEnums.enumSet = EnumSet.of(MyEnum.ONE, MyEnum.TWO);

		OutboundRow outboundRow = strategy.getOutboundRow(withEnums);

		assertThat(outboundRow).containsKey(SqlIdentifier.unquoted("enum_set"));

		Parameter value = outboundRow.get(SqlIdentifier.unquoted("enum_set"));
		assertThat(value.getValue()).isEqualTo(new String[] { "ONE", "TWO" });
	}

	@Test // gh-252
	public void shouldConvertArrayOfEnumToString() {

		DefaultReactiveDataAccessStrategy strategy = new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE,
				Collections.singletonList(MyObjectsToStringConverter.INSTANCE));

		WithEnumCollections withEnums = new WithEnumCollections();
		withEnums.enumArray = new MyEnum[] { MyEnum.ONE, MyEnum.TWO };

		OutboundRow outboundRow = strategy.getOutboundRow(withEnums);

		assertThat(outboundRow).containsKey(SqlIdentifier.unquoted("enum_array"));

		Parameter value = outboundRow.get(SqlIdentifier.unquoted("enum_array"));
		assertThat(value.getValue()).isEqualTo(new String[] { "ONE", "TWO" });
	}

	@RequiredArgsConstructor
	static class WithMultidimensionalArray {

		final int[][] myarray;
	}

	@RequiredArgsConstructor
	static class WithIntegerCollection {

		final List<Integer> myarray;
	}

	static class WithArray {

		String[] stringArray;
		List<String> stringList;
	}

	static class WithEnumCollections {

		MyEnum[] enumArray;
		Set<MyEnum> enumSet;
	}

	static class WithConversion {

		List<MyObject> myObjects;
	}

	static class MyObject {
		String foo;

		public MyObject(String foo) {
			this.foo = foo;
		}

		@Override
		public String toString() {
			return foo;
		}
	}

	enum MyEnum {
		ONE, TWO, THREE;
	}

	@WritingConverter
	enum MyObjectsToStringConverter implements Converter<List<MyObject>, String> {

		INSTANCE;

		@Override
		public String convert(List<MyObject> myObjects) {
			return myObjects.toString();
		}
	}
}
