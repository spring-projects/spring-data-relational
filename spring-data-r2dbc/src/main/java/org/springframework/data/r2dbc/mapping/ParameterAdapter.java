/*
 * Copyright 2026-present the original author or authors.
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

import io.r2dbc.spi.Type;

import java.util.Objects;

import org.jspecify.annotations.Nullable;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Adapter for a {@link Parameter} to an {@link io.r2dbc.spi.Parameter}.
 *
 * @author Christoph Strobl
 * @since 4.1
 */
@SuppressWarnings("deprecation")
public class ParameterAdapter implements io.r2dbc.spi.Parameter {

	private final org.springframework.r2dbc.core.@Nullable Parameter delegate;
	private final Type inferredType;

	public ParameterAdapter(@Nullable Parameter delegate) {
		this.delegate = delegate;
		this.inferredType = new Type.InferredType() {

			@Override
			public Class<?> getJavaType() {
				return delegate != null ? delegate.getType() : Object.class;
			}

			@Override
			public String getName() {
				return "(inferred)";
			}
		};
	}

	/**
	 * Wraps a {@link Parameter} into an {@link io.r2dbc.spi.Parameter}.
	 *
	 * @param parameter
	 * @return new instance of {@link ParameterAdapter}.
	 */
	public static io.r2dbc.spi.Parameter wrap(Parameter parameter) {

		Assert.notNull(parameter, "Parameter must not be null");
		return new ParameterAdapter(parameter);
	}

	@Override
	public Type getType() {
		return inferredType;
	}

	@Override
	public @Nullable Object getValue() {
		return delegate != null ? delegate.getValue() : null;
	}

	@Override
	public boolean equals(Object o) {

		if (o == this) {
			return true;
		}
		if (o == null) {
			return false;
		}
		if (o instanceof Parameter p) {
			return equals(p);
		}
		if (!(o instanceof ParameterAdapter that)) {
			return false;
		}
		return Objects.equals(delegate, that.delegate);
	}

	private boolean equals(Parameter that) {
		return ObjectUtils.nullSafeEquals(delegate, that);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(delegate);
	}

	@Override
	public String toString() {
		return "ParameterAdapter[value=" + this.getValue() + ",type=" + this.getType().getName() + "]";
	}
}
