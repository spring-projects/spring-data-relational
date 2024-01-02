/*
 * Copyright 2020-2024 the original author or authors.
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

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;

/**
 * Support class to natively write {@link Enum} values to the database.
 * <p>
 * By default, Spring Data converts enum values by to {@link Enum#name() String} for maximum portability. Registering a
 * {@link WritingConverter} allows retaining the enum type so that actual enum values get passed thru to the driver.
 * <p>
 * Enum types that should be written using their actual enum value to the database should require a converter for type
 * pinning. Extend this class as the {@link org.springframework.data.convert.CustomConversions} support inspects
 * {@link Converter} generics to identify conversion rules.
 * <p>
 * For example:
 *
 * <pre class="code">
 * enum Color {
 * 	Grey, Blue
 * }
 *
 * class ColorConverter extends EnumWriteSupport&lt;Color&gt; {
 *
 * }
 * </pre>
 *
 * @author Mark Paluch
 * @param <E> the enum type that should be written using the actual value.
 * @since 1.2
 */
@WritingConverter
public abstract class EnumWriteSupport<E extends Enum<E>> implements Converter<E, E> {

	@Override
	public E convert(E enumInstance) {
		return enumInstance;
	}

}
