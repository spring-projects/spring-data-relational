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
package org.springframework.data.r2dbc.convert;

import static org.assertj.core.api.Assertions.*;

import io.r2dbc.postgresql.codec.Json;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.r2dbc.core.Parameter;

/**
 * Postgres-specific unit tests for {@link MappingR2dbcConverter}.
 *
 * @author Mark Paluch
 */
public class PostgresMappingR2dbcConverterUnitTests {

	RelationalMappingContext mappingContext = new R2dbcMappingContext();
	MappingR2dbcConverter converter = new MappingR2dbcConverter(mappingContext);

	@Before
	public void before() {

		List<Object> converters = new ArrayList<>(PostgresDialect.INSTANCE.getConverters());
		converters.addAll(R2dbcCustomConversions.STORE_CONVERTERS);
		CustomConversions.StoreConversions storeConversions = CustomConversions.StoreConversions
				.of(PostgresDialect.INSTANCE.getSimpleTypeHolder(), converters);

		R2dbcCustomConversions customConversions = new R2dbcCustomConversions(storeConversions, Collections.emptyList());

		mappingContext.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());

		converter = new MappingR2dbcConverter(mappingContext, customConversions);
	}

	@Test // gh-318
	public void shouldPassThruJson() {

		JsonPerson person = new JsonPerson(null, Json.of("{\"hello\":\"world\"}"));

		OutboundRow row = new OutboundRow();
		converter.write(person, row);

		assertThat(row).containsEntry(SqlIdentifier.unquoted("json_value"), Parameter.from(person.jsonValue));
	}

	@AllArgsConstructor
	static class JsonPerson {

		@Id Long id;

		Json jsonValue;
	}
}
