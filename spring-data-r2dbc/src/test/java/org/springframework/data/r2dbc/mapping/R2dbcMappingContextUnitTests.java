/*
 * Copyright 2019-2024 the original author or authors.
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
package org.springframework.data.r2dbc.mapping;

import static org.assertj.core.api.Assertions.*;

import io.r2dbc.spi.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.r2dbc.convert.R2dbcCustomConversions;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;

/**
 * Unit tests for {@link R2dbcMappingContext}.
 *
 * @author Mark Paluch
 */
public class R2dbcMappingContextUnitTests {

	@Test
	public void shouldCreateMetadataForConvertedTypes() {

		R2dbcCustomConversions conversions = new R2dbcCustomConversions(CustomConversions.StoreConversions.NONE,
				Arrays.asList(ConvertedEntityToRow.INSTANCE, RowToConvertedEntity.INSTANCE));
		R2dbcMappingContext context = new R2dbcMappingContext();
		context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		context.afterPropertiesSet();

		assertThat(context.getPersistentEntity(ConvertedEntity.class)).isNotNull();
	}

	@Test // gh-284
	public void shouldNotCreateEntityForUUID() {

		R2dbcDialect dialect = PostgresDialect.INSTANCE;

		List<Object> converters = new ArrayList<>(dialect.getConverters());
		converters.addAll(R2dbcCustomConversions.STORE_CONVERTERS);

		CustomConversions.StoreConversions storeConversions = CustomConversions.StoreConversions
				.of(dialect.getSimpleTypeHolder(), converters);

		R2dbcCustomConversions conversions = new R2dbcCustomConversions(storeConversions, Collections.emptyList());

		R2dbcMappingContext context = new R2dbcMappingContext();
		context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		context.afterPropertiesSet();

		assertThat(context.getPersistentEntity(MyEntity.class)).isNotNull();
		assertThat(context.getPersistentEntity(UUID.class)).isNull();
	}

	static class ConvertedEntity {

	}

	@WritingConverter
	enum ConvertedEntityToRow implements Converter<ConvertedEntity, OutboundRow> {

		INSTANCE;

		@Override
		public OutboundRow convert(ConvertedEntity convertedEntity) {

			return new OutboundRow();
		}
	}

	@ReadingConverter
	enum RowToConvertedEntity implements Converter<Row, ConvertedEntity> {

		INSTANCE;

		@Override
		public ConvertedEntity convert(Row source) {
			return new ConvertedEntity();
		}
	}

	static class MyEntity {

		UUID id;
	}
}
