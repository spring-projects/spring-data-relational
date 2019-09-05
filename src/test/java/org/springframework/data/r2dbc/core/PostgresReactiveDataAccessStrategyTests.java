/*
 * Copyright 2019 the original author or authors.
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
import java.util.List;

import org.junit.Test;

import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.mapping.OutboundRow;

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

		assertThat(row.get("myarray").hasValue()).isTrue();
		assertThat(row.get("myarray").getValue()).isInstanceOf(Integer[][].class);
	}

	@Test // gh-161
	public void shouldConvertNullArrayToDriverArrayType() {

		OutboundRow row = strategy.getOutboundRow(new WithMultidimensionalArray(null));

		assertThat(row.get("myarray").hasValue()).isFalse();
		assertThat(row.get("myarray").getType()).isEqualTo(Integer[].class);
	}

	@Test // gh-161
	public void shouldConvertCollectionToArray() {

		OutboundRow row = strategy.getOutboundRow(new WithIntegerCollection(Arrays.asList(1, 2, 3)));

		assertThat(row.get("myarray").hasValue()).isTrue();
		assertThat(row.get("myarray").getValue()).isInstanceOf(Integer[].class);
		assertThat((Integer[]) row.get("myarray").getValue()).contains(1, 2, 3);
	}

	@RequiredArgsConstructor
	static class WithMultidimensionalArray {

		final int[][] myarray;
	}

	@RequiredArgsConstructor
	static class WithIntegerCollection {

		final List<Integer> myarray;
	}
}
