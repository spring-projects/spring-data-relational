/*
 * Copyright 2018-present the original author or authors.
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
import java.util.function.Consumer;

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.data.convert.ConverterBuilder;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.r2dbc.mapping.R2dbcSimpleTypeHolder;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.lang.Contract;
import org.springframework.util.Assert;

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
		return create(dialect, configurer -> configurer.registerConverters(converters));
	}

	/**
	 * Create a new {@link R2dbcCustomConversions} instance using the given {@link R2dbcDialect} and
	 * {@link R2dbcConverterConfigurer} callback to configure converters.
	 *
	 * @param dialect the {@link Dialect} to use, must not be {@literal null}.
	 * @param configurer the configurer callback to configure converters, must not be {@literal null}.
	 * @return a new {@link R2dbcCustomConversions} instance configured from the given dialect and configured converters.
	 * @since 4.0
	 */
	public static R2dbcCustomConversions create(R2dbcDialect dialect, Consumer<R2dbcConverterConfigurer> configurer) {

		Assert.notNull(dialect, "Dialect must not be null");
		Assert.notNull(configurer, "R2dbcConverterConfigurer Consumer must not be null");

		R2dbcConverterConfigurer converterConfigurer = R2dbcConverterConfigurer.from(dialect);
		configurer.accept(converterConfigurer);

		return new R2dbcCustomConversions(converterConfigurer.createConfiguration());
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

	/**
	 * {@link R2dbcConverterConfigurer} encapsulates creation of {@link R2dbcCustomConversionsConfiguration} with R2DBC
	 * specifics.
	 *
	 * @author Mark Paluch
	 * @since 4.0
	 */
	public static class R2dbcConverterConfigurer {

		private final StoreConversions storeConversions;
		private final List<Object> customConverters = new ArrayList<>();

		private R2dbcConverterConfigurer(StoreConversions storeConversions) {
			this.storeConversions = storeConversions;
		}

		/**
		 * Create a {@link R2dbcConverterConfigurer} using the provided {@code dialect} and our own codecs for JSR-310
		 * types.
		 *
		 * @param dialect must not be {@literal null}.
		 * @return
		 */
		static R2dbcConverterConfigurer from(R2dbcDialect dialect) {

			List<Object> converters = new ArrayList<>();
			converters.addAll(dialect.getConverters());
			converters.addAll(R2dbcCustomConversions.STORE_CONVERTERS);

			StoreConversions storeConversions = StoreConversions.of(dialect.getSimpleTypeHolder(), converters);

			return new R2dbcConverterConfigurer(storeConversions);
		}

		/**
		 * Create a {@link R2dbcConverterConfigurer} using the provided {@code storeConversions}.
		 *
		 * @param storeConversions must not be {@literal null}.
		 * @return
		 */
		static R2dbcConverterConfigurer from(StoreConversions storeConversions) {
			return new R2dbcConverterConfigurer(storeConversions);
		}

		/**
		 * Add a custom {@link Converter} implementation.
		 *
		 * @param converter must not be {@literal null}.
		 * @return this.
		 */
		@Contract("_ -> this")
		public R2dbcConverterConfigurer registerConverter(Converter<?, ?> converter) {

			Assert.notNull(converter, "Converter must not be null");
			customConverters.add(converter);
			return this;
		}

		/**
		 * Add {@link Converter converters}, {@link ConverterFactory factories}, {@link ConverterBuilder.ConverterAware
		 * converter-aware objects}, and {@link GenericConverter generic converters}.
		 *
		 * @param converters must not be {@literal null} nor contain {@literal null} values.
		 * @return this.
		 */
		@Contract("_ -> this")
		public R2dbcConverterConfigurer registerConverters(Object... converters) {
			return registerConverters(Arrays.asList(converters));
		}

		/**
		 * Add {@link Converter converters}, {@link ConverterFactory factories}, {@link ConverterBuilder.ConverterAware
		 * converter-aware objects}, and {@link GenericConverter generic converters}.
		 *
		 * @param converters must not be {@literal null} nor contain {@literal null} values.
		 * @return this.
		 */
		@Contract("_ -> this")
		public R2dbcConverterConfigurer registerConverters(Collection<?> converters) {

			Assert.notNull(converters, "Converters must not be null");
			Assert.noNullElements(converters, "Converters must not be null nor contain null values");

			customConverters.addAll(converters);
			return this;
		}

		/**
		 * Add a custom {@link ConverterFactory} implementation.
		 *
		 * @param converterFactory must not be {@literal null}.
		 * @return this.
		 */
		@Contract("_ -> this")
		public R2dbcConverterConfigurer registerConverterFactory(ConverterFactory<?, ?> converterFactory) {

			Assert.notNull(converterFactory, "ConverterFactory must not be null");
			customConverters.add(converterFactory);
			return this;
		}

		ConverterConfiguration createConfiguration() {
			return new R2dbcCustomConversionsConfiguration(storeConversions, this.customConverters);
		}

	}

}
