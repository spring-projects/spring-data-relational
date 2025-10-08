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
package org.springframework.data.jdbc.core.convert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.core.convert.converter.GenericConverter.ConvertiblePair;
import org.springframework.data.convert.ConverterBuilder;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.jdbc.core.mapping.JdbcSimpleTypes;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.lang.Contract;
import org.springframework.util.Assert;

/**
 * Value object to capture custom conversion. {@link JdbcCustomConversions} also act as factory for
 * {@link org.springframework.data.mapping.model.SimpleTypeHolder}
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Christoph Strobl
 * @see CustomConversions
 * @see org.springframework.data.mapping.model.SimpleTypeHolder
 * @see JdbcSimpleTypes
 */
public class JdbcCustomConversions extends CustomConversions {

	private static final Collection<Object> STORE_CONVERTERS = Collections
			.unmodifiableCollection(Jsr310TimestampBasedConverters.getConvertersToRegister());

	/**
	 * Creates an empty {@link JdbcCustomConversions} object.
	 */
	public JdbcCustomConversions() {
		this(Collections.emptyList());
	}

	/**
	 * Create a new {@link JdbcCustomConversions} instance registering the given converters and the default store
	 * converters.
	 *
	 * @param userConverters must not be {@literal null}.
	 */
	public JdbcCustomConversions(List<?> userConverters) {
		this(StoreConversions.of(JdbcSimpleTypes.HOLDER, STORE_CONVERTERS), userConverters);
	}

	/**
	 * Create a new {@link JdbcCustomConversions} instance registering the given converters and the default store
	 * converters.
	 *
	 * @since 2.3
	 */
	public JdbcCustomConversions(StoreConversions storeConversions, List<?> userConverters) {
		super(JdbcConverterConfigurer.from(storeConversions).registerConverters(userConverters).createConfiguration());
	}

	/**
	 * Create a new {@link JdbcCustomConversions} instance given
	 * {@link org.springframework.data.convert.CustomConversions.ConverterConfiguration}.
	 *
	 * @param converterConfiguration must not be {@literal null}.
	 * @since 2.2
	 */
	public JdbcCustomConversions(ConverterConfiguration converterConfiguration) {
		super(converterConfiguration);
	}

	/**
	 * Create a new {@link JdbcCustomConversions} from the given {@link Dialect} and {@code converters}.
	 *
	 * @param dialect must not be {@literal null}.
	 * @param converters must not be {@literal null}.
	 * @return a new {@link JdbcCustomConversions} instance configured from the given dialect and configured converters.
	 * @since 4.0
	 */
	public static JdbcCustomConversions of(Dialect dialect, Collection<?> converters) {

		Assert.notNull(dialect, "Dialect must not be null");
		Assert.notNull(converters, "Converters must not be null");

		return create(dialect, configurer -> configurer.registerConverters(converters));
	}

	/**
	 * Create a new {@link JdbcCustomConversions} instance using the given {@link Dialect} and
	 * {@link JdbcConverterConfigurer} callback to configure converters.
	 *
	 * @param dialect the {@link Dialect} to use, must not be {@literal null}.
	 * @param configurer the configurer callback to configure converters, must not be {@literal null}.
	 * @return a new {@link JdbcCustomConversions} instance configured from the given dialect and configured converters.
	 */
	public static JdbcCustomConversions create(Dialect dialect, Consumer<JdbcConverterConfigurer> configurer) {

		Assert.notNull(dialect, "Dialect must not be null");
		Assert.notNull(configurer, "JdbcConverterConfigurer Consumer must not be null");

		JdbcConverterConfigurer converterConfigurer = JdbcConverterConfigurer.from(dialect);
		configurer.accept(converterConfigurer);

		return new JdbcCustomConversions(converterConfigurer.createConfiguration());
	}

	/**
	 * Obtain a read only copy of default store converters.
	 *
	 * @return never {@literal null}.
	 * @since 2.3
	 */
	public static Collection<Object> storeConverters() {
		return STORE_CONVERTERS;
	}

	private static boolean isDateTimeApiConversion(ConvertiblePair cp) {

		if (cp.getSourceType().equals(java.util.Date.class)) {
			return cp.getTargetType().getTypeName().startsWith("java.time.");
		}

		if (cp.getTargetType().equals(java.util.Date.class)) {
			return cp.getSourceType().getTypeName().startsWith("java.time.");
		}

		return false;
	}

	private static boolean excludeConversionsBetweenDateAndJsr310Types(ConvertiblePair cp) {
		return !isDateTimeApiConversion(cp);
	}

	/**
	 * {@link JdbcConverterConfigurer} encapsulates creation of
	 * {@link org.springframework.data.convert.CustomConversions.ConverterConfiguration} with JDBC specifics.
	 *
	 * @author Mark Paluch
	 * @since 4.0
	 */
	public static class JdbcConverterConfigurer {

		private final StoreConversions storeConversions;
		private final List<Object> customConverters = new ArrayList<>();

		private JdbcConverterConfigurer(StoreConversions storeConversions) {
			this.storeConversions = storeConversions;
		}

		/**
		 * Create a {@link JdbcConverterConfigurer} using the provided {@code dialect} and our own codecs for JSR-310 types.
		 *
		 * @param dialect must not be {@literal null}.
		 * @return
		 */
		static JdbcConverterConfigurer from(Dialect dialect) {

			List<Object> converters = new ArrayList<>();
			converters.addAll(dialect.getConverters());
			converters.addAll(JdbcCustomConversions.storeConverters());

			SimpleTypeHolder simpleTypeHolder = new SimpleTypeHolder(dialect.simpleTypes(), JdbcSimpleTypes.HOLDER);
			return new JdbcConverterConfigurer(StoreConversions.of(simpleTypeHolder, converters));
		}

		/**
		 * Create a {@link JdbcConverterConfigurer} using the provided {@code storeConversions}.
		 *
		 * @param storeConversions must not be {@literal null}.
		 * @return
		 */
		static JdbcConverterConfigurer from(StoreConversions storeConversions) {
			return new JdbcConverterConfigurer(storeConversions);
		}

		/**
		 * Add a custom {@link Converter} implementation.
		 *
		 * @param converter must not be {@literal null}.
		 * @return this.
		 */
		@Contract("_ -> this")
		public JdbcConverterConfigurer registerConverter(Converter<?, ?> converter) {

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
		public JdbcConverterConfigurer registerConverters(Object... converters) {
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
		public JdbcConverterConfigurer registerConverters(Collection<?> converters) {

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
		public JdbcConverterConfigurer registerConverterFactory(ConverterFactory<?, ?> converterFactory) {

			Assert.notNull(converterFactory, "ConverterFactory must not be null");
			customConverters.add(converterFactory);
			return this;
		}

		ConverterConfiguration createConfiguration() {
			return new ConverterConfiguration(storeConversions, this.customConverters,
					JdbcCustomConversions::excludeConversionsBetweenDateAndJsr310Types);
		}

	}

}
