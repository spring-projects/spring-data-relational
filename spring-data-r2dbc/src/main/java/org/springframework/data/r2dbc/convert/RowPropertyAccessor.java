/*
 * Copyright 2013-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.r2dbc.convert;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.PropertyAccessor;
import org.springframework.expression.TypedValue;
import org.springframework.lang.Nullable;

/**
 * {@link PropertyAccessor} to read values from a {@link Row}.
 *
 * @author Mark Paluch
 * @since 1.2
 */
class RowPropertyAccessor implements PropertyAccessor {

	private final @Nullable RowMetadata rowMetadata;

	RowPropertyAccessor(@Nullable RowMetadata rowMetadata) {
		this.rowMetadata = rowMetadata;
	}

	@Override
	public Class<?>[] getSpecificTargetClasses() {
		return new Class<?>[] { Row.class };
	}

	@Override
	public boolean canRead(EvaluationContext context, @Nullable Object target, String name) {
		return rowMetadata != null && target != null && RowMetadataUtils.containsColumn(rowMetadata, name);
	}

	@Override
	public TypedValue read(EvaluationContext context, @Nullable Object target, String name) {

		if (target == null) {
			return TypedValue.NULL;
		}

		Object value = ((Row) target).get(name);

		if (value == null) {
			return TypedValue.NULL;
		}

		return new TypedValue(value);
	}

	@Override
	public boolean canWrite(EvaluationContext context, @Nullable Object target, String name) {
		return false;
	}

	@Override
	public void write(EvaluationContext context, @Nullable Object target, String name, @Nullable Object newValue) {
		throw new UnsupportedOperationException();
	}
}
