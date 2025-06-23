/*
 * Copyright 2019-2025 the original author or authors.
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

import org.springframework.util.ObjectUtils;

/**
 * Bind marker/parameter placeholder used to construct prepared statements with parameter substitution.
 *
 * @author Mark Paluch
 * @since 1.1
 */
public class BindMarker extends AbstractSegment implements Expression {

	public static BindMarker named(String name) {
		return new NamedBindMarker(name);
	}

	public static BindMarker indexed(int index) {
		return new IndexedBindMarker("" + index, index);
	}

	public static BindMarker indexed(String name, int index) {
		return new IndexedBindMarker(name, index);
	}

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
		public boolean equals(Object o) {
			if (!(o instanceof NamedBindMarker that)) {
				return false;
			}
			if (!super.equals(o)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(name, that.name);
		}

		@Override
		public int hashCode() {
			return super.hashCode() * 31 + ObjectUtils.nullSafeHash(name);
		}

		@Override
		public String toString() {
			return "?[" + name + "]";
		}
	}

	static class IndexedBindMarker extends BindMarker implements Named {

		private final int index;
		private final String name;

		IndexedBindMarker(String name, int index) {
			this.name = name;
			this.index = index;
		}

		@Override
		public SqlIdentifier getName() {
			return SqlIdentifier.unquoted(name);
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof IndexedBindMarker that)) {
				return false;
			}
			if (!super.equals(o)) {
				return false;
			}
			if (index != that.index) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(name, that.name);
		}

		@Override
		public int hashCode() {
			return super.hashCode() * 31 + ObjectUtils.nullSafeHash(index, name);
		}

		@Override
		public String toString() {
			return "?" + index;
		}
	}
}
