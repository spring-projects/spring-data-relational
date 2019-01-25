/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.relational.core.sql;

import org.springframework.lang.Nullable;

/**
 * Bind marker/parameter placeholder used to construct prepared statements with parameter substitution.
 *
 * @author Mark Paluch
 */
public class BindMarker extends AbstractSegment implements Expression {

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "?";
	}

	static class NamedBindMarker extends BindMarker implements Named {

		private final String name;

		NamedBindMarker(String name) {
			this.name = name;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.Named#getName()
		 */
		@Nullable
		@Override
		public String getName() {
			return name;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.relational.core.sql.BindMarker#toString()
		 */
		@Override
		public String toString() {
			return "?[" + name + "]";
		}
	}
}
