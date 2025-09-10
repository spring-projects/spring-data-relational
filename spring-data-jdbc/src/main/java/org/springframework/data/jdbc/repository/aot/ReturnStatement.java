/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.jdbc.repository.aot;

import org.springframework.data.util.ReflectionUtils;
import org.springframework.javapoet.CodeBlock;
import org.springframework.util.ClassUtils;

/**
 * @author Mark Paluch
 */
class ReturnStatement {

	public static ReturnStatementBuilder returning(Class<?> returnType) {
		return new ReturnStatementBuilder(returnType);
	}

	static class ReturnStatementBuilder {

		private final Class<?> returnType;
		private final CodeBlock.Builder builder = CodeBlock.builder();
		private boolean hasReturn = false;

		private ReturnStatementBuilder(Class<?> returnType) {

			this.returnType = returnType;
			whenBoxed(Void.class, "null");
			hasReturn = ReflectionUtils.isVoid(returnType);
		}

		public ReturnStatementBuilder whenBoolean(String format, Object... args) {
			return when(returnType == boolean.class || returnType == Boolean.class, format, args);
		}

		public ReturnStatementBuilder whenBoxedLong(String format, Object... args) {
			return whenBoxed(long.class, format, args);
		}

		public ReturnStatementBuilder whenLong(String format, Object... args) {
			return when(returnType == long.class, format, args);
		}

		public ReturnStatementBuilder whenBoxedInteger(String format, Object... args) {
			return whenBoxed(int.class, format, args);
		}

		public ReturnStatementBuilder whenInt(String format, Object... args) {
			return when(returnType == int.class, format, args);
		}

		public ReturnStatementBuilder whenBoxed(Class<?> primitiveOrWrapper, String format, Object... args) {

			Class<?> primitiveWrapper = ClassUtils.resolvePrimitiveIfNecessary(primitiveOrWrapper);
			return when(returnType == primitiveWrapper, format, args);
		}

		public ReturnStatementBuilder when(Class<?> returnType, String format, Object... args) {
			return when(this.returnType.isAssignableFrom(returnType), format, args);
		}

		public ReturnStatementBuilder when(boolean condition, String format, Object... args) {

			if (hasReturn) {
				return this;
			}

			if (condition) {
				builder.addStatement("return " + format, args);
				hasReturn = true;
			}

			return this;
		}

		public ReturnStatementBuilder otherwise(String format, Object... args) {
			return when(!hasReturn, format, args);
		}

		public ReturnStatementBuilder number(String resultVariableName) {

			return whenBoxedLong("$1L != null ? $1L.longValue() : null", resultVariableName) //
					.whenLong("$1L != null ? $1L.longValue() : 0L", resultVariableName) //
					.whenBoxedInteger("$1L != null ? $1L.intValue() : null", resultVariableName) //
					.whenInt("$1L != null ? $1L.intValue() : 0", resultVariableName);
		}

		public CodeBlock build() {
			return builder.build();
		}

	}
}
