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
package org.springframework.data.relational.core.sql;

/**
 * Bind marker/parameter placeholder used to construct prepared statements with parameter substitution.
 *
 * @author Mark Paluch
 * @since 1.1
 */
public class BindMarker extends AbstractSegment implements Expression {

	@Override
	public String toString() {
		return "?";
	}

	static class NamedBindMarker extends BindMarker implements Named {

		private final String name;

		NamedBindMarker(String name) {
			this.name = name;
		}

		@Override
		public SqlIdentifier getName() {
			return SqlIdentifier.unquoted(name);
		}

		@Override
		public String toString() {
			return "?[" + name + "]";
		}
	}
}
