/*
 * Copyright 2019-present the original author or authors.
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

import org.h2.api.Interval;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.relational.core.dialect.ArrayColumns;
import org.springframework.data.relational.core.dialect.ObjectArrayColumns;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.util.Lazy;
import org.springframework.r2dbc.core.binding.BindMarkersFactory;
import org.springframework.util.ClassUtils;

/**
 * R2DBC dialect for H2.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Diego Krupitza
 * @author Kurt Niemi
 * @author YeongJae Min
 */
public class H2Dialect extends org.springframework.data.relational.core.dialect.H2Dialect implements R2dbcDialect {

	private static final Set<Class<?>> SIMPLE_TYPES;

	private static final boolean H2_INTERVAL_PRESENT = ClassUtils.isPresent("org.h2.api.Interval",
			H2Dialect.class.getClassLoader());

	static {

		Set<Class<?>> simpleTypes = new HashSet<>(
				org.springframework.data.relational.core.dialect.H2Dialect.INSTANCE.simpleTypes());

		// conditional H2 Interval support.
		ifClassPresent("org.h2.api.Interval", simpleTypes::add);

		SIMPLE_TYPES = simpleTypes;
	}

	/**
	 * Singleton instance.
	 */
	public static final H2Dialect INSTANCE = new H2Dialect();

	private static final BindMarkersFactory INDEXED = BindMarkersFactory.indexed("$", 1);

	private static final List<Object> CONVERTERS = List.copyOf(createConverters());

	private final Lazy<ArrayColumns> arrayColumns = Lazy
			.of(() -> new SimpleTypeArrayColumns(ObjectArrayColumns.INSTANCE, getSimpleTypeHolder()));

	private static List<Object> createConverters() {

		List<Object> converters = new ArrayList<>();

		if (H2_INTERVAL_PRESENT) {
			converters.add(H2IntervalToDurationConverter.INSTANCE);
			converters.add(DurationToH2IntervalConverter.INSTANCE);
		}

		return converters;
	}

	/**
	 * If the class is present on the class path, invoke the specified consumer {@code action} with the class object,
	 * otherwise do nothing.
	 *
	 * @param action block to be executed if a value is present.
	 */
	private static void ifClassPresent(String className, Consumer<Class<?>> action) {

		if (ClassUtils.isPresent(className, H2Dialect.class.getClassLoader())) {
			action.accept(ClassUtils.resolveClassName(className, H2Dialect.class.getClassLoader()));
		}
	}

	@Override
	public ArrayColumns getArraySupport() {
		return this.arrayColumns.get();
	}

	@Override
	public BindMarkersFactory getBindMarkersFactory() {
		return INDEXED;
	}

	@Override
	public String renderForGeneratedValues(SqlIdentifier identifier) {
		return identifier.getReference();
	}

	@Override
	public Collection<Object> getConverters() {
		return CONVERTERS;
	}

	@Override
	public Set<Class<?>> simpleTypes() {
		return SIMPLE_TYPES;
	}

	@ReadingConverter
	private enum H2IntervalToDurationConverter implements Converter<Interval, Duration> {

		INSTANCE;

		@Override
		public Duration convert(Interval source) {

			if (source.getQualifier().isYearMonth()) {
				throw new IllegalArgumentException("Year-month intervals cannot be represented as Duration");
			}

			return Duration.ZERO //
					.plusDays(source.getDays()) //
					.plusHours(source.getHours()) //
					.plusMinutes(source.getMinutes()) //
					.plusSeconds(source.getSeconds()) //
					.plusNanos(source.getNanosOfSecond());
		}
	}

	@WritingConverter
	private enum DurationToH2IntervalConverter implements Converter<Duration, Interval> {

		INSTANCE;

		private static final int NANOS_PER_SECOND = 1_000_000_000;

		@Override
		public Interval convert(Duration source) {

			long seconds = source.getSeconds();
			int nanos = source.getNano();

			if (seconds < 0 && nanos > 0) {
				seconds++;
				nanos -= NANOS_PER_SECOND;
			}

			return Interval.ofSeconds(seconds, nanos);
		}
	}

}
