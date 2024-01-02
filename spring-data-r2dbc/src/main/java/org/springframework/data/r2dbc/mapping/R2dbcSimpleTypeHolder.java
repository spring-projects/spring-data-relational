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
package org.springframework.data.r2dbc.mapping;

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Row;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.springframework.data.mapping.model.SimpleTypeHolder;

/**
 * Simple constant holder for a {@link SimpleTypeHolder} enriched with R2DBC specific simple types.
 *
 * @author Mark Paluch
 */
public class R2dbcSimpleTypeHolder extends SimpleTypeHolder {

	/**
	 * Set of R2DBC simple types.
	 */
	public static final Set<Class<?>> R2DBC_SIMPLE_TYPES = Collections
			.unmodifiableSet(new HashSet<>(Arrays.asList(OutboundRow.class, Row.class, BigInteger.class, BigDecimal.class,
					UUID.class, Blob.class, Clob.class, ByteBuffer.class, Parameter.class)));

	public static final SimpleTypeHolder HOLDER = new R2dbcSimpleTypeHolder();

	/**
	 * Create a new {@link R2dbcSimpleTypeHolder} instance.
	 */
	private R2dbcSimpleTypeHolder() {
		super(R2DBC_SIMPLE_TYPES, true);
	}

}
