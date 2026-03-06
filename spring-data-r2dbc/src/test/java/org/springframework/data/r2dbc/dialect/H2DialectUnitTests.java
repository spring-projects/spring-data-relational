/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.r2dbc.dialect;

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;
import java.util.LinkedHashSet;

import org.h2.api.Interval;
import org.junit.jupiter.api.Test;

import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.core.convert.support.GenericConversionService;

/**
 * Unit tests for {@link H2Dialect}.
 *
 * @author wonderfulrosemari
 */
class H2DialectUnitTests {

	@Test // gh-502
	void shouldConsiderIntervalSimpleType() {
		assertThat(H2Dialect.INSTANCE.getSimpleTypeHolder().isSimpleType(Interval.class)).isTrue();
	}

	@Test // gh-502
	void shouldConvertIntervalToDuration() {

		Interval interval = Interval.ofSeconds(42, 123_000_000);
		Duration converted = conversionService().convert(interval, Duration.class);

		assertThat(converted).isEqualTo(Duration.ofSeconds(42, 123_000_000));
	}

	@Test // gh-502
	void shouldConvertNegativeDurationToInterval() {

		Duration duration = Duration.ofMillis(-500);
		Interval interval = conversionService().convert(duration, Interval.class);

		assertThat(interval).isEqualTo(Interval.ofSeconds(0, -500_000_000));
		assertThat(conversionService().convert(interval, Duration.class)).isEqualTo(duration);
	}

	private static GenericConversionService conversionService() {

		GenericConversionService conversionService = new GenericConversionService();
		ConversionServiceFactory.registerConverters(new LinkedHashSet<>(H2Dialect.INSTANCE.getConverters()), conversionService);
		return conversionService;
	}
}
