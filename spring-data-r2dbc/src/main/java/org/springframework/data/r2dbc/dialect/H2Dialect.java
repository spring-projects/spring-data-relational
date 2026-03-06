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

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jspecify.annotations.Nullable;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.GenericConverter;
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
 * @author wonderfulrosemari
 */
public class H2Dialect extends org.springframework.data.relational.core.dialect.H2Dialect implements R2dbcDialect {

	/**
	 * Singleton instance.
	 */
	public static final H2Dialect INSTANCE = new H2Dialect();

	private static final BindMarkersFactory INDEXED = BindMarkersFactory.indexed("$", 1);
	private static final String H2_INTERVAL_CLASS_NAME = "org.h2.api.Interval";
	private static final @Nullable Class<?> H2_INTERVAL_CLASS = resolveIntervalClass();
	private static final Set<Class<?>> SIMPLE_TYPES;
	private static final Collection<Object> CONVERTERS;

	static {

		Set<Class<?>> simpleTypes = new HashSet<>(org.springframework.data.relational.core.dialect.H2Dialect.INSTANCE.simpleTypes());
		List<Object> converters = new ArrayList<>();

		if (H2_INTERVAL_CLASS != null) {
			simpleTypes.add(H2_INTERVAL_CLASS);
			converters.add(new IntervalToDurationConverter(H2_INTERVAL_CLASS));
			converters.add(new DurationToIntervalConverter(H2_INTERVAL_CLASS));
		}

		SIMPLE_TYPES = Collections.unmodifiableSet(simpleTypes);
		CONVERTERS = Collections.unmodifiableList(converters);
	}

	private final Lazy<ArrayColumns> arrayColumns = Lazy
			.of(() -> new SimpleTypeArrayColumns(ObjectArrayColumns.INSTANCE, getSimpleTypeHolder()));

	private static @Nullable Class<?> resolveIntervalClass() {

		if (!ClassUtils.isPresent(H2_INTERVAL_CLASS_NAME, H2Dialect.class.getClassLoader())) {
			return null;
		}

		return ClassUtils.resolveClassName(H2_INTERVAL_CLASS_NAME, H2Dialect.class.getClassLoader());
	}

	@Override
	public BindMarkersFactory getBindMarkersFactory() {
		return INDEXED;
	}

	@Override
	public Collection<? extends Class<?>> getSimpleTypes() {
		return SIMPLE_TYPES;
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
	public ArrayColumns getArraySupport() {
		return this.arrayColumns.get();
	}

	@ReadingConverter
	private static final class IntervalToDurationConverter implements GenericConverter {

		private final Class<?> intervalClass;
		private final Method getSecondsMethod;
		private final Method getNanosOfSecondMethod;
		private final Set<ConvertiblePair> convertibleTypes;

		private IntervalToDurationConverter(Class<?> intervalClass) {

			this.intervalClass = intervalClass;
			this.getSecondsMethod = ClassUtils.getMethod(intervalClass, "getSeconds");
			this.getNanosOfSecondMethod = ClassUtils.getMethod(intervalClass, "getNanosOfSecond");
			this.convertibleTypes = Collections.singleton(new ConvertiblePair(intervalClass, Duration.class));
		}

		@Override
		public Set<ConvertiblePair> getConvertibleTypes() {
			return this.convertibleTypes;
		}

		@Override
		public @Nullable Object convert(@Nullable Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {

			if (source == null) {
				return null;
			}

			if (!this.intervalClass.isInstance(source)) {
				throw new IllegalArgumentException("Expected source type %s but got %s"
						.formatted(this.intervalClass.getName(), source.getClass().getName()));
			}

			try {
				long seconds = ((Number) this.getSecondsMethod.invoke(source)).longValue();
				long nanos = ((Number) this.getNanosOfSecondMethod.invoke(source)).longValue();
				return Duration.ofSeconds(seconds, nanos);
			} catch (ReflectiveOperationException ex) {
				throw new IllegalStateException("Cannot convert H2 Interval to Duration", ex);
			}
		}
	}

	@WritingConverter
	private static final class DurationToIntervalConverter implements GenericConverter {

		private static final int NANOS_PER_SECOND = 1_000_000_000;

		private final Class<?> intervalClass;
		private final Method ofSecondsMethod;
		private final Set<ConvertiblePair> convertibleTypes;

		private DurationToIntervalConverter(Class<?> intervalClass) {
			this.intervalClass = intervalClass;
			this.ofSecondsMethod = ClassUtils.getMethod(intervalClass, "ofSeconds", long.class, int.class);
			this.convertibleTypes = Collections.singleton(new ConvertiblePair(Duration.class, intervalClass));
		}

		@Override
		public Set<ConvertiblePair> getConvertibleTypes() {
			return this.convertibleTypes;
		}

		@Override
		public @Nullable Object convert(@Nullable Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {

			if (source == null) {
				return null;
			}

			Duration duration = (Duration) source;

			long seconds = duration.getSeconds();
			int nanos = duration.getNano();

			// H2 Interval.ofSeconds requires both parts to have the same sign.
			if (seconds < 0 && nanos > 0) {
				seconds = seconds + 1;
				nanos = nanos - NANOS_PER_SECOND;
			}

			try {
				return this.ofSecondsMethod.invoke(null, seconds, nanos);
			} catch (ReflectiveOperationException ex) {
				throw new IllegalStateException("Cannot convert Duration to H2 Interval", ex);
			}
		}
	}
}
