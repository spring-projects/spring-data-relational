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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.springframework.javapoet.CodeBlock;

/**
 * @author Mark Paluch
 */
class BetterCodeBlock {

	public static BetterBuilder of(CodeBlock.Builder builder) {
		return new BetterBuilder(builder);
	}

	public static class BetterBuilder {

		private final CodeBlock.Builder builder;

		BetterBuilder(CodeBlock.Builder builder) {
			this.builder = builder;
		}

		public boolean isEmpty() {
			return builder.isEmpty();
		}

		public BetterBuilder indent() {
			builder.indent();
			return this;
		}

		public BetterBuilder endControlFlow() {
			builder.endControlFlow();
			return this;
		}

		public BetterBuilder nextControlFlow(String controlFlow, Object... args) {
			builder.nextControlFlow(controlFlow, args);
			return this;
		}

		public BetterBuilder add(String format, Object... args) {
			builder.add(format, args);
			return this;
		}

		public BetterBuilder addStatement(CodeBlock codeBlock) {
			builder.addStatement(codeBlock);
			return this;
		}

		public BetterBuilder addStatement(Consumer<StepBuilder> consumer) {

			StepBuilder statementBuilder = new StepBuilder();
			consumer.accept(statementBuilder);

			if (!statementBuilder.isEmpty()) {

				this.add("$[");

				for (int i = 0; i < statementBuilder.formats.size(); i++) {
					builder.add(statementBuilder.formats.get(i), statementBuilder.args.get(i));
				}

				this.add(";\n$]");

			}
			return this;
		}

		public BetterBuilder beginControlFlow(String controlFlow, Object... args) {
			builder.beginControlFlow(controlFlow, args);
			return this;
		}

		public BetterBuilder add(CodeBlock codeBlock) {
			builder.add(codeBlock);
			return this;
		}

		public BetterBuilder addStatement(String format, Object... args) {
			builder.addStatement(format, args);
			return this;
		}

		public CodeBlock build() {
			return builder.build();
		}

		public BetterBuilder clear() {
			builder.clear();
			return this;
		}

		public BetterBuilder addNamed(String format, Map<String, ?> arguments) {
			builder.addNamed(format, arguments);
			return this;
		}

		public BetterBuilder endControlFlow(String controlFlow, Object... args) {
			builder.endControlFlow(controlFlow, args);
			return this;
		}

		public BetterBuilder unindent() {
			builder.unindent();
			return this;
		}
	}

	public static class StepBuilder {

		List<String> formats = new ArrayList<>();
		List<Object[]> args = new ArrayList<>();

		public boolean isEmpty() {
			return formats.isEmpty();
		}

		public ConditionalStatementStep when(boolean state) {
			return whenNot(!state);
		}

		public StepBuilder add(String format, Object... args) {
			formats.add(format);
			this.args.add(args);
			return this;
		}

		public ConditionalStatementStep whenNot(boolean state) {

			return (format, args) -> {

				if (state) {
					formats.add(format);
					this.args.add(args);
				}
				return this;
			};
		}

		public interface ConditionalStatementStep {

			StepBuilder then(String format, Object... args);
		}
	}
}
