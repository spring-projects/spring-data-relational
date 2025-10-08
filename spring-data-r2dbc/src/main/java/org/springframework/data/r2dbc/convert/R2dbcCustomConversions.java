/*
 * Copyright 2018-2025 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.springframework.data.convert.CustomConversions;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.r2dbc.mapping.R2dbcSimpleTypeHolder;

/**
 * Value object to capture custom conversion. {@link R2dbcCustomConversions} also act as factory for
 * {@link org.springframework.data.mapping.model.SimpleTypeHolder}
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @see CustomConversions
 * @see org.springframework.data.mapping.model.SimpleTypeHolder
 */
public class R2dbcCustomConversions extends CustomConversions {

	public static final List<Object> STORE_CONVERTERS;

	public static final StoreConversions STORE_CONVERSIONS;

	static {

		List<Object> converters = new ArrayList<>(R2dbcConverters.getConvertersToRegister());

		STORE_CONVERTERS = Collections.unmodifiableList(converters);
		STORE_CONVERSIONS = StoreConversions.of(R2dbcSimpleTypeHolder.HOLDER, STORE_CONVERTERS);
	}

	/**
	 * Create a new {@link R2dbcCustomConversions} instance registering the given converters.
	 *
	 * @param storeConversions must not be {@literal null}.
	 * @param converters must not be {@literal null}.
	 */
	public R2dbcCustomConversions(StoreConversions storeConversions, Collection<?> converters) {
		super(new R2dbcCustomConversionsConfiguration(storeConversions,
				converters instanceof List ? (List<?>) converters : new ArrayList<>(converters)));
	}

	protected R2dbcCustomConversions(ConverterConfiguration converterConfiguration) {
		super(converterConfiguration);
	}

	/**
	 * Create a new {@link R2dbcCustomConversions} from the given {@link R2dbcDialect} and {@code converters}.
	 *
	 * @param dialect must not be {@literal null}.
	 * @param converters must not be {@literal null}.
	 * @return the custom conversions object.
	 * @since 1.2
	 */
	public static R2dbcCustomConversions of(R2dbcDialect dialect, Object... converters) {
		return of(dialect, Arrays.asList(converters));
	}

	/**
	 * Create a new {@link R2dbcCustomConversions} from the given {@link R2dbcDialect} and {@code converters}.
	 *
	 * @param dialect must not be {@literal null}.
	 * @param converters must not be {@literal null}.
	 * @return the custom conversions object.
	 * @since 1.2
	 */
	public static R2dbcCustomConversions of(R2dbcDialect dialect, Collection<?> converters) {

		List<Object> storeConverters = new ArrayList<>(dialect.getConverters());
		storeConverters.addAll(R2dbcCustomConversions.STORE_CONVERTERS);

		return new R2dbcCustomConversions(StoreConversions.of(dialect.getSimpleTypeHolder(), storeConverters), converters);
	}

	static class R2dbcCustomConversionsConfiguration extends ConverterConfiguration {

		public R2dbcCustomConversionsConfiguration(StoreConversions storeConversions, List<?> userConverters) {
			super(storeConversions, userConverters, convertiblePair -> {

				// Avoid JSR-310 temporal types conversion into java.util.Date
				if (convertiblePair.getSourceType().getName().startsWith("java.time.")
						&& convertiblePair.getTargetType().equals(Date.class)) {
					return false;
				}

				return true;
			});
		}
	}
}
