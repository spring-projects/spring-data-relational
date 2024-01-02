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
package org.springframework.data.r2dbc.dialect;

import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.r2dbc.core.binding.BindMarkersFactory;

/**
 * An SQL dialect for MySQL.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Kurt Niemi
 */
public class MySqlDialect extends org.springframework.data.relational.core.dialect.MySqlDialect
		implements R2dbcDialect {

	private static final Set<Class<?>> SIMPLE_TYPES = new HashSet<>(
			Arrays.asList(UUID.class, URL.class, URI.class, InetAddress.class));

	/**
	 * Singleton instance.
	 */
	public static final MySqlDialect INSTANCE = new MySqlDialect();

	private static final BindMarkersFactory ANONYMOUS = BindMarkersFactory.anonymous("?");

	/**
	 * MySQL specific converters.
	 */
	private static final List<Object> CONVERTERS = Arrays.asList(ByteToBooleanConverter.INSTANCE,
			BooleanToByteConverter.INSTANCE);

	@Override
	public BindMarkersFactory getBindMarkersFactory() {
		return ANONYMOUS;
	}

	@Override
	public Collection<? extends Class<?>> getSimpleTypes() {
		return SIMPLE_TYPES;
	}

	@Override
	public Collection<Object> getConverters() {
		return CONVERTERS;
	}

	/**
	 * Simple singleton to convert {@link Byte}s to their {@link Boolean} representation. MySQL does not have a built-in
	 * boolean type by default, so relies on using a byte instead. Non-zero values represent {@literal true}.
	 *
	 * @author Michael Berry
	 */
	@ReadingConverter
	public enum ByteToBooleanConverter implements Converter<Byte, Boolean> {

		INSTANCE;

		@Override
		public Boolean convert(Byte s) {

			if (s == null) {
				return null;
			}

			return s != 0;
		}
	}

	@Override
	public String renderForGeneratedValues(SqlIdentifier identifier) {
		return identifier.getReference();
	}

	/**
	 * Simple singleton to convert {@link Boolean}s to their {@link Byte} representation. MySQL does not have a built-in
	 * boolean type by default, so relies on using a byte instead. {@literal true} maps to {@code 1}.
	 *
	 * @author Mark Paluch
	 */
	@WritingConverter
	public enum BooleanToByteConverter implements Converter<Boolean, Byte> {

		INSTANCE;

		@Override
		public Byte convert(Boolean s) {
			return (byte) (s.booleanValue() ? 1 : 0);
		}
	}
}
