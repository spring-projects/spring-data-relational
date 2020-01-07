/*
 * Copyright 2018-2020 the original author or authors.
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

import java.util.Collections;
import java.util.List;

import org.springframework.data.jdbc.core.mapping.JdbcSimpleTypes;

/**
 * Value object to capture custom conversion. {@link JdbcCustomConversions} also act as factory for
 * {@link org.springframework.data.mapping.model.SimpleTypeHolder}
 *
 * @author Mark Paluch
 * @see org.springframework.data.convert.CustomConversions
 * @see org.springframework.data.mapping.model.SimpleTypeHolder
 * @see JdbcSimpleTypes
 */
public class JdbcCustomConversions extends org.springframework.data.convert.CustomConversions {

	private static final StoreConversions STORE_CONVERSIONS;
	private static final List<Object> STORE_CONVERTERS;

	static {

		STORE_CONVERTERS = Collections.emptyList();
		STORE_CONVERSIONS = StoreConversions.of(JdbcSimpleTypes.HOLDER, STORE_CONVERTERS);
	}

	/**
	 * Creates an empty {@link JdbcCustomConversions} object.
	 */
	public JdbcCustomConversions() {
		this(Collections.emptyList());
	}

	/**
	 * Create a new {@link JdbcCustomConversions} instance registering the given converters.
	 *
	 * @param converters must not be {@literal null}.
	 */
	public JdbcCustomConversions(List<?> converters) {
		super(STORE_CONVERSIONS, converters);
	}

}
