/*
 * Copyright 2022-2024 the original author or authors.
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

import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.core.dialect.ArrayColumns;
import org.springframework.util.ClassUtils;

/**
 * {@link ArrayColumns} support based on {@link SimpleTypeHolder store-native simple types}.
 *
 * @author Mark Paluch
 * @since 3.0
 */
record SimpleTypeArrayColumns(ArrayColumns delegate, SimpleTypeHolder simpleTypeHolder) implements ArrayColumns {

	@Override
	public boolean isSupported() {
		return this.delegate.isSupported();
	}

	@Override
	public Class<?> getArrayType(Class<?> userType) {

		Class<?> typeToUse = ArrayColumns.unwrapComponentType(userType);

		if (!this.simpleTypeHolder.isSimpleType(typeToUse)) {
			throw new IllegalArgumentException(
					"Unsupported array type: %s".formatted(ClassUtils.getQualifiedName(typeToUse)));
		}

		return this.delegate.getArrayType(typeToUse);
	}
}
