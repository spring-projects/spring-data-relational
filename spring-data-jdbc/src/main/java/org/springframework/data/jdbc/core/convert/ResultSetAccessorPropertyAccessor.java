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
package org.springframework.data.jdbc.core.convert;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.PropertyAccessor;
import org.springframework.expression.TypedValue;
import org.springframework.lang.Nullable;

/**
 * {@link PropertyAccessor} to access a column from a {@link ResultSetAccessor}.
 *
 * @author Mark Paluch
 * @since 2.1
 */
class ResultSetAccessorPropertyAccessor implements PropertyAccessor {

	static final PropertyAccessor INSTANCE = new ResultSetAccessorPropertyAccessor();

	@Override
	public Class<?>[] getSpecificTargetClasses() {
		return new Class<?>[] { ResultSetAccessor.class };
	}

	@Override
	public boolean canRead(EvaluationContext context, @Nullable Object target, String name) {
		return target instanceof ResultSetAccessor resultSetAccessor && resultSetAccessor.hasValue(name);
	}

	@Override
	public TypedValue read(EvaluationContext context, @Nullable Object target, String name) {

		if (target == null) {
			return TypedValue.NULL;
		}

		Object value = ((ResultSetAccessor) target).getObject(name);

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
