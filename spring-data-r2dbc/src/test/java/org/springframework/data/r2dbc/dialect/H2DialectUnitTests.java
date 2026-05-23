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

import org.h2.api.Interval;

import java.time.Duration;
import java.util.LinkedHashSet;

import org.junit.jupiter.api.Test;

import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.mapping.model.SimpleTypeHolder;

/**
 * Unit tests for {@link H2Dialect}.
 *
 * @author YeongJae Min
 */
class H2DialectUnitTests {

	@Test // GH-502
	void shouldConsiderIntervalSimpleType() {

		SimpleTypeHolder holder = H2Dialect.INSTANCE.getSimpleTypeHolder();

		assertThat(holder.isSimpleType(Interval.class)).isTrue();
	}

	@Test // GH-502
	void shouldConvertDayTimeIntervalToDuration() {

		Interval interval = Interval.ofDaysHoursMinutesNanos(1, 2, 3, 4_500_000_000L);
		Duration converted = conversionService().convert(interval, Duration.class);

		assertThat(converted).isEqualTo(Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4)
				.plusNanos(500_000_000));
	}

	@Test // GH-502
	void shouldRejectYearMonthIntervalToDurationConversion() {

		assertThatThrownBy(() -> conversionService().convert(Interval.ofYearsMonths(1, 2), Duration.class))
				.isInstanceOf(ConversionFailedException.class)
				.hasRootCauseInstanceOf(IllegalArgumentException.class);
	}

	@Test // GH-502
	void shouldConvertNegativeDurationToInterval() {

		Duration duration = Duration.ofMillis(-500);
		Interval interval = conversionService().convert(duration, Interval.class);

		assertThat(interval).isEqualTo(Interval.ofSeconds(0, -500_000_000));
		assertThat(conversionService().convert(interval, Duration.class)).isEqualTo(duration);
	}

	private static GenericConversionService conversionService() {

		GenericConversionService conversionService = new GenericConversionService();
		ConversionServiceFactory.registerConverters(new LinkedHashSet<>(H2Dialect.INSTANCE.getConverters()),
				conversionService);

		return conversionService;
	}
}
