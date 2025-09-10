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
 * Utility for building return statements for code generation purposes.
 * <p>
 * Provides a fluent API to construct return statements based on the given return type. Typically used in AOT scenarios
 * to generate code fragments for repository methods.
 *
 * @author Mark Paluch
 */
class ReturnStatement {

	/**
	 * Create a builder for a return statement targeting the given return type.
	 *
	 * @param returnType the method return type
	 * @return a new {@code ReturnStatementBuilder}
	 */
	public static ReturnStatementBuilder forType(Class<?> returnType) {
		return new ReturnStatementBuilder(returnType);
	}

	/**
	 * Builder for constructing return statements based on the target return type.
	 */
	static class ReturnStatementBuilder {

		private final Class<?> returnType;
		private final CodeBlock.Builder builder = CodeBlock.builder();
		private boolean hasReturn = false;

		/**
		 * Create a new builder for the given return type.
		 *
		 * @param returnType the method return type
		 */
		private ReturnStatementBuilder(Class<?> returnType) {
			this.returnType = returnType;

			// consider early return cases for Void and void.
			whenBoxed(Void.class, "null");
			this.hasReturn = ReflectionUtils.isVoid(returnType);
		}

		/**
		 * Add return statements for numeric types if the given {@code resultToReturn} points to a {@link Number}. Considers
		 * primitive and boxed {@code int} and {@code long} type return paths and that {@code resultToReturn} can be
		 * {@literal null}.
		 *
		 * @param resultToReturn the argument or variable name holding the result.
		 * @return {@code this} builder.
		 */
		public ReturnStatementBuilder number(String resultToReturn) {
			return whenBoxedLong("$1L != null ? $1L.longValue() : null", resultToReturn)
					.whenLong("$1L != null ? $1L.longValue() : 0L", resultToReturn)
					.whenBoxedInteger("$1L != null ? $1L.intValue() : null", resultToReturn)
					.whenInt("$1L != null ? $1L.intValue() : 0", resultToReturn);
		}

		/**
		 * Add a return statement if the return type is boolean (primitive or box type).
		 *
		 * @param format the code format string.
		 * @param args the format arguments.
		 * @return {@code this} builder.
		 */
		public ReturnStatementBuilder whenBoolean(String format, Object... args) {
			return when(returnType == boolean.class || returnType == Boolean.class, format, args);
		}

		/**
		 * Add a return statement if the return type is {@link Long} (boxed {@code long} type).
		 *
		 * @param format the code format string.
		 * @param args the format arguments.
		 * @return {@code this} builder.
		 */
		public ReturnStatementBuilder whenBoxedLong(String format, Object... args) {
			return whenBoxed(long.class, format, args);
		}

		/**
		 * Add a return statement if the return type is a primitive {@code long} type.
		 *
		 * @param format the code format string.
		 * @param args the format arguments.
		 * @return {@code this} builder.
		 */
		public ReturnStatementBuilder whenLong(String format, Object... args) {
			return when(returnType == long.class, format, args);
		}

		/**
		 * Add a return statement if the return type is {@link Integer} (boxed {@code int} type).
		 *
		 * @param format the code format string.
		 * @param args the format arguments.
		 * @return {@code this} builder.
		 */
		public ReturnStatementBuilder whenBoxedInteger(String format, Object... args) {
			return whenBoxed(int.class, format, args);
		}

		/**
		 * Add a return statement if the return type is a primitive {@code int} type.
		 *
		 * @param format the code format string.
		 * @param args the format arguments.
		 * @return {@code this} builder.
		 */
		public ReturnStatementBuilder whenInt(String format, Object... args) {
			return when(returnType == int.class, format, args);
		}

		/**
		 * Add a return statement if the return type matches the given boxed wrapper type.
		 *
		 * @param primitiveOrWrapper the primitive or wrapper type.
		 * @param format the code format string.
		 * @param args the format arguments.
		 * @return {@code this} builder.
		 */
		public ReturnStatementBuilder whenBoxed(Class<?> primitiveOrWrapper, String format, Object... args) {
			Class<?> primitiveWrapper = ClassUtils.resolvePrimitiveIfNecessary(primitiveOrWrapper);
			return when(returnType == primitiveWrapper, format, args);
		}

		/**
		 * Add a return statement if the declared return type is assignable from the given {@code returnType}.
		 *
		 * @param returnType the candidate return type.
		 * @param format the code format string.
		 * @param args the format arguments
		 * @return {@code this} builder.
		 */
		public ReturnStatementBuilder when(Class<?> returnType, String format, Object... args) {
			return when(this.returnType.isAssignableFrom(returnType), format, args);
		}

		/**
		 * Add a return statement if the given condition is {@code true}.
		 *
		 * @param condition the condition to evaluate.
		 * @param format the code format string.
		 * @param args the format arguments.
		 * @return {@code this} builder.
		 */
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

		/**
		 * Add a fallback return statement if no previous return statement was added.
		 *
		 * @param format the code format string.
		 * @param args the format arguments.
		 * @return {@code this} builder.
		 */
		public ReturnStatementBuilder otherwise(String format, Object... args) {
			return when(!hasReturn, format, args);
		}

		/**
		 * Build the code block representing the return statement.
		 *
		 * @return the resulting {@code CodeBlock}
		 */
		public CodeBlock build() {
			return builder.build();
		}

	}
}
