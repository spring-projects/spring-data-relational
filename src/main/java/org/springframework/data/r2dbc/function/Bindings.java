/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.r2dbc.function;

import io.r2dbc.spi.Statement;
import lombok.RequiredArgsConstructor;

import java.util.List;

import org.springframework.data.r2dbc.domain.SettableValue;

/**
 * @author Jens Schauder
 */
public class Bindings {

	private final List<SingleBinding> bindings;

	public Bindings(List<SingleBinding> bindings) {
		this.bindings = bindings;
	}

	public void apply(Statement statement) {
		bindings.forEach(sb -> sb.bindTo(statement));
	}

	@RequiredArgsConstructor
	public static abstract class SingleBinding<T> {

		final T identifier;
		final SettableValue value;

		public abstract void bindTo(Statement statement);

		public abstract boolean isIndexed();

		public final boolean isNamed() {
			return !isIndexed();
		}
	}


	public static class IndexedSingleBinding extends SingleBinding<Integer> {

		public IndexedSingleBinding(Integer identifier, SettableValue value) {
			super(identifier, value);
		}

		@Override
		public void bindTo(Statement statement) {

			if (value.isEmpty()) {
				statement.bindNull((int) identifier, value.getType());
			} else {
				statement.bind((int) identifier, value.getValue());
			}
		}

		@Override
		public boolean isIndexed() {
			return true;
		}
	}

	public static class NamedExpandedSingleBinding extends SingleBinding<String> {

		private final BindableOperation operation;

		public NamedExpandedSingleBinding(String identifier, SettableValue value, BindableOperation operation) {

			super(identifier, value);

			this.operation = operation;
		}

		@Override
		public void bindTo(Statement statement) {

			if (value != null) {
				operation.bind(statement, identifier, value);
			} else {
				operation.bindNull(statement, identifier, value.getType());
			}
		}

		@Override
		public boolean isIndexed() {
			return false;
		}
	}
}
