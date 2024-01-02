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
package org.springframework.data.r2dbc.convert;

import static org.assertj.core.api.Assertions.*;

import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.test.MockColumnMetadata;
import io.r2dbc.spi.test.MockRow;
import io.r2dbc.spi.test.MockRowMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.r2dbc.dialect.MySqlDialect;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.r2dbc.testing.OutboundRowAssert;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;

/**
 * MySQL-specific unit tests for {@link MappingR2dbcConverter}.
 *
 * @author Mark Paluch
 */
class MySqlMappingR2dbcConverterUnitTests {

	private RelationalMappingContext mappingContext = new R2dbcMappingContext();
	private MappingR2dbcConverter converter = new MappingR2dbcConverter(mappingContext);

	@BeforeEach
	void before() {

		List<Object> converters = new ArrayList<>(MySqlDialect.INSTANCE.getConverters());
		converters.addAll(R2dbcCustomConversions.STORE_CONVERTERS);
		CustomConversions.StoreConversions storeConversions = CustomConversions.StoreConversions
				.of(MySqlDialect.INSTANCE.getSimpleTypeHolder(), converters);

		R2dbcCustomConversions customConversions = new R2dbcCustomConversions(storeConversions, Collections.emptyList());

		mappingContext.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());

		converter = new MappingR2dbcConverter(mappingContext, customConversions);
	}

	@Test // gh-589
	void shouldWriteBooleanToByte() {

		BooleanMapping object = new BooleanMapping(0, true, false);
		OutboundRow row = new OutboundRow();

		converter.write(object, row);

		OutboundRowAssert.assertThat(row).containsColumnWithValue("flag1", (byte) 1).containsColumnWithValue("flag2",
				(byte) 0);
	}

	@Test // gh-589
	void shouldReadByteToBoolean() {

		MockRow row = MockRow.builder().identified("flag1", Object.class, (byte) 1)
				.identified("flag2", Object.class, (byte) 0).build();

		MockRowMetadata metadata = MockRowMetadata.builder()
				.columnMetadata(MockColumnMetadata.builder().name("flag1").type(R2dbcType.SMALLINT).build())
				.columnMetadata(MockColumnMetadata.builder().name("flag2").type(R2dbcType.SMALLINT).build()).build();

		BooleanMapping mapped = converter.read(BooleanMapping.class, row, metadata);

		assertThat(mapped.flag1).isTrue();
		assertThat(mapped.flag2).isFalse();
	}

	@Test // gh-589
	void shouldPreserveByteValue() {

		WithByte object = new WithByte(0, (byte) 3);
		OutboundRow row = new OutboundRow();

		converter.write(object, row);

		OutboundRowAssert.assertThat(row).containsColumnWithValue("state", (byte) 3);
	}

	record BooleanMapping(

			Integer id, boolean flag1, boolean flag2) {
	}

	record WithByte (

		Integer id,
		byte state){
	}

}
