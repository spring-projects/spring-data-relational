/*
 * Copyright 2020 the original author or authors.
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
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.UUID;

import org.junit.Test;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.r2dbc.dialect.BindTarget;
import org.springframework.data.r2dbc.dialect.MySqlDialect;
import org.springframework.data.r2dbc.mapping.SettableValue;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Update;
import org.springframework.r2dbc.core.PreparedOperation;

/**
 * Unit tests for {@link ReactiveDataAccessStrategy}.
 *
 * @author Mark Paluch
 */
public class ReactiveDataAccessStrategyTests {

	BindTarget bindTarget = mock(BindTarget.class);

	ReactiveDataAccessStrategy strategy = new DefaultReactiveDataAccessStrategy(MySqlDialect.INSTANCE,
			Arrays.asList(UuidToStringConverter.INSTANCE, StringToUuidConverter.INSTANCE));

	@Test // gh-305
	public void shouldConvertSettableValue() {

		UUID value = UUID.randomUUID();

		assertThat(strategy.getBindValue(SettableValue.from(value))).isEqualTo(SettableValue.from(value.toString()));
		assertThat(strategy.getBindValue(SettableValue.from(Condition.New))).isEqualTo(SettableValue.from("New"));
	}

	@Test // gh-305
	public void shouldConvertEmptySettableValue() {

		assertThat(strategy.getBindValue(SettableValue.empty(UUID.class))).isEqualTo(SettableValue.empty(String.class));
		assertThat(strategy.getBindValue(SettableValue.empty(Condition.class)))
				.isEqualTo(SettableValue.empty(String.class));
	}

	@Test // gh-305
	public void shouldConvertCriteria() {

		UUID value = UUID.randomUUID();

		StatementMapper mapper = strategy.getStatementMapper();
		StatementMapper.SelectSpec spec = mapper.createSelect("foo").withProjection("*")
				.withCriteria(Criteria.where("id").is(value));

		PreparedOperation<?> mappedObject = mapper.getMappedObject(spec);
		mappedObject.bindTo(bindTarget);

		verify(bindTarget).bind(0, value.toString());
	}

	@Test // gh-305
	public void shouldConvertAssignment() {

		UUID value = UUID.randomUUID();

		StatementMapper mapper = strategy.getStatementMapper();
		StatementMapper.UpdateSpec update = mapper.createUpdate("foo", Update.update("id", value));

		PreparedOperation<?> mappedObject = mapper.getMappedObject(update);
		mappedObject.bindTo(bindTarget);

		verify(bindTarget).bind(0, value.toString());
	}

	@WritingConverter
	enum UuidToStringConverter implements Converter<UUID, String> {
		INSTANCE;

		@Override
		public String convert(UUID uuid) {
			return uuid.toString();
		}
	}

	@ReadingConverter
	enum StringToUuidConverter implements Converter<String, UUID> {
		INSTANCE;

		@Override
		public UUID convert(String value) {
			return UUID.fromString(value);
		}
	}

	enum Condition {
		New, Used
	}
}
