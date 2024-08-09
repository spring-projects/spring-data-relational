/*
 * Copyright 2020-2024 the original author or authors.
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.springframework.data.r2dbc.testing.Assertions.assertThat;

import io.r2dbc.spi.Parameters;
import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.r2dbc.dialect.MySqlDialect;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Update;
import org.springframework.r2dbc.core.PreparedOperation;
import org.springframework.r2dbc.core.binding.BindTarget;

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
	public void shouldConvertParameter() {

		UUID value = UUID.randomUUID();

		assertThat(strategy.getBindValue(Parameters.in(value))).isEqualTo(Parameters.in(value.toString()));
		assertThat(strategy.getBindValue(Parameters.in(Condition.New))).isEqualTo(Parameters.in("New"));
	}

	@Test // gh-305
	public void shouldConvertEmptyParameter() {

		assertThat(strategy.getBindValue(Parameters.in(UUID.class))).isEqualTo(Parameters.in(String.class));
		assertThat(strategy.getBindValue(Parameters.in(Condition.class))).isEqualTo(Parameters.in(String.class));
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
