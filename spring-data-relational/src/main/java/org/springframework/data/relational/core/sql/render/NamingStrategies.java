/*
 * Copyright 2019-2024 the original author or authors.
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
package org.springframework.data.relational.core.sql.render;

import java.util.Locale;
import java.util.function.Function;

import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.TableLike;
import org.springframework.util.Assert;

/**
 * Factory for {@link RenderNamingStrategy} objects.
 *
 * @author Mark Paluch
 * @since 1.1
 */
public abstract class NamingStrategies {

	private NamingStrategies() {}

	/**
	 * Creates a as-is {@link RenderNamingStrategy} that preserves {@link Column} and {@link Table} names as they were
	 * expressed during their declaration.
	 *
	 * @return as-is {@link RenderNamingStrategy}.
	 */
	public static RenderNamingStrategy asIs() {
		return AsIs.INSTANCE;
	}

	/**
	 * Creates a mapping {@link RenderNamingStrategy} that applies a {@link Function mapping function} to {@link Column}
	 * and {@link Table} names.
	 *
	 * @param mappingFunction the mapping {@link Function}, must not be {@literal null}.
	 * @return the mapping {@link RenderNamingStrategy}.
	 */
	public static RenderNamingStrategy mapWith(Function<String, String> mappingFunction) {
		return AsIs.INSTANCE.map(mappingFunction);
	}

	/**
	 * Creates a mapping {@link RenderNamingStrategy} that converts {@link Column} and {@link Table} names to upper case
	 * using the default {@link Locale}.
	 *
	 * @return upper-casing {@link RenderNamingStrategy}.
	 * @see String#toUpperCase()
	 * @see Locale
	 */
	public static RenderNamingStrategy toUpper() {
		return toUpper(Locale.getDefault());
	}

	/**
	 * Creates a mapping {@link RenderNamingStrategy} that converts {@link Column} and {@link Table} names to upper case
	 * using the given {@link Locale}.
	 *
	 * @param locale the locale to use.
	 * @return upper-casing {@link RenderNamingStrategy}.
	 * @see String#toUpperCase(Locale)
	 */
	public static RenderNamingStrategy toUpper(Locale locale) {

		Assert.notNull(locale, "Locale must not be null");

		return AsIs.INSTANCE.map(it -> it.toUpperCase(locale));
	}

	/**
	 * Creates a mapping {@link RenderNamingStrategy} that converts {@link Column} and {@link Table} names to lower case
	 * using the default {@link Locale}.
	 *
	 * @return lower-casing {@link RenderNamingStrategy}.
	 * @see String#toLowerCase()
	 * @see Locale
	 */
	public static RenderNamingStrategy toLower() {
		return toLower(Locale.getDefault());
	}

	/**
	 * Creates a mapping {@link RenderNamingStrategy} that converts {@link Column} and {@link Table} names to lower case
	 * using the given {@link Locale}.
	 *
	 * @param locale the locale to use.
	 * @return lower-casing {@link RenderNamingStrategy}.
	 * @see String#toLowerCase(Locale)
	 * @see Locale
	 */
	public static RenderNamingStrategy toLower(Locale locale) {

		Assert.notNull(locale, "Locale must not be null");

		return AsIs.INSTANCE.map(it -> it.toLowerCase(locale));
	}

	enum AsIs implements RenderNamingStrategy {
		INSTANCE
	}

	static class DelegatingRenderNamingStrategy implements RenderNamingStrategy {

		private final RenderNamingStrategy delegate;
		private final Function<String, String> mappingFunction;

		DelegatingRenderNamingStrategy(RenderNamingStrategy delegate, Function<String, String> mappingFunction) {

			this.delegate = delegate;
			this.mappingFunction = mappingFunction;
		}

		@Override
		public SqlIdentifier getName(Column column) {
			return delegate.getName(column).transform(mappingFunction::apply);
		}

		@Override
		public SqlIdentifier getReferenceName(Column column) {
			return delegate.getReferenceName(column).transform(mappingFunction::apply);
		}

		@Override
		public SqlIdentifier getName(TableLike table) {
			return delegate.getName(table).transform(mappingFunction::apply);
		}

		@Override
		public SqlIdentifier getReferenceName(TableLike table) {
			return delegate.getReferenceName(table).transform(mappingFunction::apply);
		}
	}
}
